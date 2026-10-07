"""Notification channels in the catalog: CRUD, test messages and delivery for workflow blocks."""

import uuid
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.connectors import notify
from app.connectors.notify import Message, NotifyError
from app.core.config import get_settings
from app.core.crypto import SecretDecryptionError, decrypt_secret, encrypt_secret
from app.core.exceptions import BadRequestError, ConflictError, NotFoundError
from app.db.base import utcnow
from app.models.notification_channel import ChannelKind, ChannelStatus, NotificationChannel
from app.models.user import User
from app.schemas.channel import ChannelCreate, ChannelUpdate
from app.services import audit_service

HTTP_KINDS = (ChannelKind.SLACK, ChannelKind.TEAMS, ChannelKind.WEBHOOK)


@dataclass(frozen=True)
class Delivery:
    ok: bool
    detail: str  # "Sent to #ops" / the error


# ---------------------------------------------------------------------- helpers


def target(channel: NotificationChannel) -> str:
    """What the channel points at, without secrets."""
    if channel.kind == ChannelKind.EMAIL:
        s = channel.settings or {}
        return f"{s.get('smtp_host', '')}:{s.get('smtp_port') or ''}".rstrip(":")
    try:
        url = decrypt_secret(channel.encrypted_secret) if channel.encrypted_secret else ""
    except SecretDecryptionError:
        return "(link cannot be decrypted)"
    return notify.masked_url(url)


def _secret(channel: NotificationChannel) -> str | None:
    try:
        return decrypt_secret(channel.encrypted_secret) if channel.encrypted_secret else None
    except SecretDecryptionError as exc:
        raise NotifyError(
            "The stored password/link cannot be decrypted (was ENCRYPTION_KEY changed?). "
            "Re-enter it on the channel."
        ) from exc


def _check_config(kind: ChannelKind, email: dict | None, secret: str | None) -> None:
    settings = get_settings()
    if kind == ChannelKind.EMAIL:
        if not email:
            raise BadRequestError("Email channels need the SMTP settings")
        return
    if not secret:
        raise BadRequestError("Paste the webhook link for this channel")
    try:
        notify.check_webhook_url(
            secret, kind=kind.value, allowed_hosts=settings.AUTOMATION_WEBHOOK_ALLOWED_HOSTS
        )
    except NotifyError as exc:
        raise BadRequestError(str(exc)) from exc


# ---------------------------------------------------------------------- CRUD


def list_channels(db: Session) -> list[NotificationChannel]:
    return list(db.scalars(select(NotificationChannel).order_by(NotificationChannel.name)).all())


def get_channel(db: Session, channel_id: uuid.UUID) -> NotificationChannel:
    channel = db.get(NotificationChannel, channel_id)
    if channel is None:
        raise NotFoundError("Notification channel not found")
    return channel


def _ensure_unique_name(db: Session, name: str, exclude_id: uuid.UUID | None = None) -> None:
    query = select(NotificationChannel.id).where(
        func.lower(NotificationChannel.name) == name.lower()
    )
    if exclude_id:
        query = query.where(NotificationChannel.id != exclude_id)
    if db.scalar(query):
        raise ConflictError(f"A channel named {name!r} already exists")


def create_channel(
    db: Session, data: ChannelCreate, *, actor: User, ip_address: str | None
) -> NotificationChannel:
    email = data.email.model_dump(mode="json") if data.email else None
    _check_config(data.kind, email, data.secret)
    _ensure_unique_name(db, data.name)
    channel = NotificationChannel(
        id=uuid.uuid4(),
        name=data.name,
        kind=data.kind,
        settings=email or {},
        encrypted_secret=encrypt_secret(data.secret) if data.secret else None,
        is_active=data.is_active,
        created_by=actor.id,
    )
    db.add(channel)
    db.flush()
    audit_service.record(
        db,
        action="notification_channel.create",
        entity_type="notification_channel",
        entity_id=channel.id,
        actor=actor,
        details={"name": channel.name, "kind": channel.kind, "target": target(channel)},
        ip_address=ip_address,
    )
    db.commit()
    return channel


def update_channel(
    db: Session,
    channel_id: uuid.UUID,
    data: ChannelUpdate,
    *,
    actor: User,
    ip_address: str | None,
) -> NotificationChannel:
    channel = get_channel(db, channel_id)
    changes = data.model_dump(exclude_unset=True, mode="json")
    changed: list[str] = []
    if changes.get("name") and changes["name"] != channel.name:
        _ensure_unique_name(db, changes["name"], exclude_id=channel.id)
        channel.name = changes["name"]
        changed.append("name")
    if changes.get("is_active") is not None and changes["is_active"] != channel.is_active:
        channel.is_active = changes["is_active"]
        changed.append("is_active")
    if changes.get("email") and channel.kind == ChannelKind.EMAIL:
        channel.settings = changes["email"]
        changed.append("email")
    if "secret" in changes:
        secret = changes["secret"]
        if secret and channel.kind in HTTP_KINDS:
            _check_config(channel.kind, None, secret)
        if not secret and channel.kind in HTTP_KINDS:
            raise BadRequestError("A webhook channel needs its link")
        channel.encrypted_secret = encrypt_secret(secret) if secret else None
        changed.append("secret")
    if any(c in changed for c in ("email", "secret")):
        channel.last_status = ChannelStatus.UNKNOWN
        channel.last_message = None
    if changed:
        audit_service.record(
            db,
            action="notification_channel.update",
            entity_type="notification_channel",
            entity_id=channel.id,
            actor=actor,
            details={"changed": changed},
            ip_address=ip_address,
        )
    db.commit()
    return channel


def delete_channel(
    db: Session, channel_id: uuid.UUID, *, actor: User, ip_address: str | None
) -> None:
    channel = get_channel(db, channel_id)
    audit_service.record(
        db,
        action="notification_channel.delete",
        entity_type="notification_channel",
        entity_id=channel.id,
        actor=actor,
        details={"name": channel.name, "kind": channel.kind},
        ip_address=ip_address,
    )
    db.delete(channel)
    db.commit()


# ---------------------------------------------------------------------- sending


def deliver(
    channel: NotificationChannel, message: Message, recipients: list[str] | None = None
) -> Delivery:
    """Send through the channel and record the outcome on it (the caller commits).

    Never raises for delivery problems: workflows record the failure and carry on.
    """
    settings = get_settings()
    try:
        if not channel.is_active:
            raise NotifyError(f"Channel {channel.name} is turned off")
        secret = _secret(channel)
        if channel.kind == ChannelKind.EMAIL:
            to = recipients or list((channel.settings or {}).get("default_recipients") or [])
            notify.send_email(
                channel.settings or {}, secret, to, message, timeout=settings.SMTP_TIMEOUT_SECONDS
            )
            detail = f"Emailed {', '.join(to)}"
        else:
            notify.send_http(
                channel.kind.value,
                secret or "",
                message,
                allowed_hosts=settings.AUTOMATION_WEBHOOK_ALLOWED_HOSTS,
                timeout=settings.AUTOMATION_WEBHOOK_TIMEOUT_SECONDS,
            )
            detail = f"Sent to {channel.name}"
    except NotifyError as exc:
        channel.last_status = ChannelStatus.FAILED
        channel.last_message = str(exc)[:500]
        channel.last_used_at = utcnow()
        return Delivery(ok=False, detail=str(exc))
    channel.last_status = ChannelStatus.OK
    channel.last_message = detail[:500]
    channel.last_used_at = utcnow()
    return Delivery(ok=True, detail=detail)


def send_test(
    db: Session,
    channel_id: uuid.UUID,
    *,
    to: list[str],
    actor: User,
    ip_address: str | None,
) -> Delivery:
    channel = get_channel(db, channel_id)
    link = get_settings().PUBLIC_APP_URL.rstrip("/") + "/connections/channels"
    result = deliver(
        channel,
        Message(
            title="Test message from Agentic Ops",
            text=f"If you can read this, the channel “{channel.name}” works.",
            link=link,
        ),
        to or None,
    )
    audit_service.record(
        db,
        action="notification_channel.test",
        entity_type="notification_channel",
        entity_id=channel.id,
        actor=actor,
        details={"ok": result.ok, "detail": result.detail},
        ip_address=ip_address,
    )
    db.commit()
    return result
