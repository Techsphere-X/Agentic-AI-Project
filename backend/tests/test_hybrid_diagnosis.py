"""aiplan1: hybrid regex + model diagnosis, ML client resilience, persistence and API."""

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.connectors.ml_client import MLClient, redact
from app.core.config import get_settings
from app.detection.types import EvidenceKind, IncidentSeverity, IncidentType
from app.diagnosis import hybrid
from app.diagnosis.hybrid import MLUnavailable
from app.models.airflow import (
    AirflowConnection,
    ConnectionKind,
    DeploymentEnvironment,
    MonitoredDag,
)
from app.models.incident import Incident, IncidentEvidence
from app.orchestration.airflow.base import AuthType
from app.services import diagnosis_service

OOM_LOG = "INFO - start\nTask exited with return code -9 (OOMKilled)"
UNKNOWN_LOG = "INFO - start\nupstream partner rejected the batch for reasons unknown"
CODE_LOG = "Traceback (most recent call last):\nValueError: bad value from requests adapter"
T = 0.85


@dataclass
class FakePrediction:
    category: str
    confidence: float
    top_predictions: list[dict[str, Any]] = field(default_factory=list)
    model_version: str | None = "20261001-0000"
    signal_line: str | None = "the signal line"
    window: str = "window text"


class FakeClient:
    def __init__(self, prediction: FakePrediction | None = None, *, fail: bool = False) -> None:
        self.enabled = True
        self.prediction = prediction
        self.fail = fail
        self.calls: list[list[str]] = []

    def classify(self, logs: list[str], top_k: int = 3) -> FakePrediction:
        self.calls.append(logs)
        if self.fail or self.prediction is None:
            raise MLUnavailable("down")
        return self.prediction


# ---------------------------------------------------------------------- decision rule


def test_specific_regex_category_skips_the_model() -> None:
    client = FakeClient(FakePrediction("AUTH", 0.99))
    payload, model = hybrid.decide([OOM_LOG], client, T)
    assert (payload["category"], payload["source"], payload["ml_status"]) == (
        "RESOURCE",
        "regex",
        "skipped",
    )
    assert payload["retryable"] is True
    assert model is None and client.calls == []


def test_confident_model_overrides_unknown() -> None:
    client = FakeClient(
        FakePrediction(
            "TRANSIENT_NETWORK",
            0.93,
            [
                {"category": "TRANSIENT_NETWORK", "score": 0.93},
            ],
        )
    )
    payload, model = hybrid.decide([UNKNOWN_LOG], client, T)
    assert payload["category"] == "TRANSIENT_NETWORK"
    assert (payload["source"], payload["ml_status"], payload["rule"]) == ("model", "used", "model")
    assert payload["retryable"] is True  # derived from RETRYABLE, not the model
    assert payload["model_version"] == "20261001-0000"
    assert payload["matched_line"] == "the signal line"
    assert payload["top_predictions"][0]["category"] == "TRANSIENT_NETWORK"
    assert model is not None


def test_confident_model_overrides_generic_code_bug() -> None:
    payload, _ = hybrid.decide([CODE_LOG], FakeClient(FakePrediction("AUTH", 0.9)), T)
    assert (payload["category"], payload["source"], payload["retryable"]) == (
        "AUTH",
        "model",
        False,
    )


def test_low_confidence_model_is_only_a_suggestion() -> None:
    payload, _ = hybrid.decide([CODE_LOG], FakeClient(FakePrediction("TIMEOUT", 0.6)), T)
    assert (payload["category"], payload["source"], payload["ml_status"]) == (
        "CODE_BUG",
        "regex",
        "used",
    )
    assert payload["suggestion"] == {
        "category": "TIMEOUT",
        "label": "Timeout",
        "confidence": 0.6,
    }


def test_model_unknown_or_agreeing_gives_no_suggestion() -> None:
    for prediction in (FakePrediction("UNKNOWN", 0.99), FakePrediction("CODE_BUG", 0.5)):
        payload, _ = hybrid.decide([CODE_LOG], FakeClient(prediction), T)
        assert payload["category"] == "CODE_BUG" and payload["suggestion"] is None


def test_unknown_regex_borrows_the_model_signal_line() -> None:
    payload, _ = hybrid.decide([UNKNOWN_LOG], FakeClient(FakePrediction("SCHEMA", 0.4)), T)
    assert payload["category"] == "UNKNOWN"
    assert payload["matched_line"] == "the signal line"


def test_unavailable_disabled_and_empty() -> None:
    down, _ = hybrid.decide([UNKNOWN_LOG], FakeClient(fail=True), T)
    assert (down["category"], down["ml_status"]) == ("UNKNOWN", "unavailable")

    disabled = FakeClient(FakePrediction("AUTH", 0.99))
    disabled.enabled = False
    off, _ = hybrid.decide([UNKNOWN_LOG], disabled, T)
    assert off["ml_status"] == "disabled" and disabled.calls == []
    assert hybrid.decide([UNKNOWN_LOG], None, T)[0]["ml_status"] == "disabled"

    client = FakeClient(FakePrediction("AUTH", 0.99))
    empty, _ = hybrid.decide([None, "  "], client, T)
    assert empty["ml_status"] == "skipped" and client.calls == []


def test_at_most_three_logs_are_sent() -> None:
    client = FakeClient(FakePrediction("UNKNOWN", 0.2))
    hybrid.decide([UNKNOWN_LOG] * 5, client, T)
    assert len(client.calls[0]) == hybrid.MAX_MODEL_LOGS


# ---------------------------------------------------------------------- ml client


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _client(handler, clock: Clock | None = None) -> MLClient:
    settings = get_settings().model_copy(
        update={"ML_SERVICE_ENABLED": True, "ML_SERVICE_TOKEN": "tok"}
    )
    return MLClient(settings, transport=httpx.MockTransport(handler), clock=clock or Clock())


def test_client_sends_token_and_redacted_logs() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["token"] = request.headers.get("X-ML-Token")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "category": "AUTH",
                "confidence": 0.91,
                "top_predictions": [],
                "model_version": "v1",
                "signal_line": "x",
                "window": "w",
            },
        )

    result = _client(handler).classify(
        ["connect postgresql://etl:hunter2@db/x password=hunter2 Authorization: Bearer abc.def"]
    )
    assert (result.category, result.confidence, result.window) == ("AUTH", 0.91, "w")
    assert seen["token"] == "tok"
    assert "hunter2" not in json.dumps(seen["body"]) and "abc.def" not in json.dumps(seen["body"])


def test_redact_bearer_and_url_credentials() -> None:
    text = redact("curl -H 'X: Bearer s3cr3t.tok' https://u:pw@host/x")
    assert "s3cr3t" not in text and ":pw@" not in text


def test_breaker_opens_after_failures_then_recovers() -> None:
    calls = {"n": 0}
    clock = Clock()

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    client = _client(handler, clock)
    failures = get_settings().ML_BREAKER_FAILURES
    for _ in range(failures):
        with pytest.raises(MLUnavailable):
            client.classify(["log"])
    assert client.breaker.is_open
    with pytest.raises(MLUnavailable, match="circuit open"):
        client.classify(["log"])
    assert calls["n"] == failures  # fail fast, no network while open

    clock.now += get_settings().ML_BREAKER_COOLDOWN_SECONDS + 1
    assert not client.breaker.is_open
    with pytest.raises(MLUnavailable):
        client.classify(["log"])
    assert calls["n"] == failures + 1


def test_bad_payload_and_connection_errors_are_unavailable() -> None:
    with pytest.raises(MLUnavailable):
        _client(lambda r: httpx.Response(200, json={"nope": 1})).classify(["log"])

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with pytest.raises(MLUnavailable):
        _client(refuse).classify(["log"])


# ---------------------------------------------------------------------- persistence


def make_incident(db: Session, *logs: str) -> Incident:
    conn = AirflowConnection(
        name=f"mock-{uuid.uuid4().hex[:6]}",
        environment=DeploymentEnvironment.DEV,
        kind=ConnectionKind.MOCK,
        base_url="http://mock-airflow",
        auth_type=AuthType.NONE,
    )
    db.add(conn)
    db.flush()
    dag = MonitoredDag(connection_id=conn.id, dag_id="orders_pipeline", is_monitored=True)
    db.add(dag)
    db.flush()
    now = datetime.now(UTC)
    incident = Incident(
        connection_id=conn.id,
        monitored_dag_id=dag.id,
        dag_id=dag.dag_id,
        type=IncidentType.DAG_RUN_FAILED,
        severity=IncidentSeverity.MEDIUM,
        title="orders_pipeline: DAG run failed",
        fingerprint=f"fp-{uuid.uuid4()}",
        occurred_at=now,
    )
    db.add(incident)
    db.flush()
    for i, log in enumerate(logs):
        db.add(
            IncidentEvidence(
                incident_id=incident.id,
                kind=EvidenceKind.TASK_LOG,
                source=f"task:{i}",
                content=log,
            )
        )
    db.commit()
    return incident


def events(db: Session, incident: Incident) -> list[str]:
    db.refresh(incident)
    return [e.event for e in incident.events]


def test_diagnose_and_store_persists_and_records_event(db: Session) -> None:
    incident = make_incident(db, UNKNOWN_LOG)
    client = FakeClient(FakePrediction("TRANSIENT_NETWORK", 0.95))
    payload = diagnosis_service.diagnose_and_store(db, incident, client=client)
    db.commit()
    assert payload is not None and incident.diagnosis == payload
    assert incident.diagnosis["source"] == "model"
    assert events(db, incident) == ["diagnosed"]
    assert incident.events[0].details["model_version"] == "20261001-0000"


def test_operator_correction_wins_and_is_not_overwritten(
    db: Session,
    client: TestClient,
    operator,
    operator_headers: dict[str, str],
) -> None:
    incident = make_incident(db, CODE_LOG)
    diagnosis_service.diagnose_and_store(db, incident, client=None)
    db.commit()

    response = client.put(
        f"/api/v1/incidents/{incident.id}/diagnosis",
        json={"category": "AUTH", "note": "  expired service account  "},
        headers=operator_headers,
    )
    assert response.status_code == 200, response.text
    diagnosis = response.json()["diagnosis"]
    assert (diagnosis["category"], diagnosis["source"], diagnosis["retryable"]) == (
        "AUTH",
        "operator",
        False,
    )
    assert diagnosis["note"] == "expired service account"
    assert diagnosis["previous"]["category"] == "CODE_BUG"

    db.expire_all()
    stored = db.get(Incident, incident.id)
    again = diagnosis_service.diagnose_and_store(
        db, stored, client=FakeClient(FakePrediction("TIMEOUT", 0.99))
    )
    assert again["source"] == "operator" and again["category"] == "AUTH"
    assert "diagnosis_corrected" in events(db, stored)


def test_override_validation_and_roles(
    db: Session,
    client: TestClient,
    operator_headers: dict[str, str],
    viewer_headers: dict[str, str],
) -> None:
    incident = make_incident(db, CODE_LOG)
    url = f"/api/v1/incidents/{incident.id}/diagnosis"
    assert client.put(url, json={"category": "AUTH"}, headers=viewer_headers).status_code == 403
    bad = client.put(url, json={"category": "NOPE"}, headers=operator_headers)
    assert bad.status_code == 400


def test_detail_backfills_regex_only_without_calling_model(
    db: Session,
    client: TestClient,
    viewer_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incident = make_incident(db, OOM_LOG)

    def boom() -> None:
        raise AssertionError("model must not be called on the read path")

    monkeypatch.setattr(diagnosis_service, "model_client", boom)
    response = client.get(f"/api/v1/incidents/{incident.id}", headers=viewer_headers)
    assert response.status_code == 200, response.text
    diagnosis = response.json()["diagnosis"]
    assert (diagnosis["category"], diagnosis["source"], diagnosis["ml_status"]) == (
        "RESOURCE",
        "regex",
        "skipped",
    )
    second = client.get(f"/api/v1/incidents/{incident.id}", headers=viewer_headers).json()
    assert (
        second["diagnosis"]["diagnosed_at"] == diagnosis["diagnosed_at"]
    )  # stored, not recomputed
    assert events(db, incident).count("diagnosed") == 1


# ---------------------------------------------------------------------- analyzer API


def test_analyzer_regex_only_when_ml_disabled(
    client: TestClient,
    viewer_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "ML_SERVICE_ENABLED", False)
    response = client.post(
        "/api/v1/diagnosis/classify", json={"log": CODE_LOG}, headers=viewer_headers
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ml_status"] == "disabled" and body["model"] is None
    assert body["regex"]["category"] == body["final"]["category"] == "CODE_BUG"


def test_analyzer_uses_model_and_returns_window(
    client: TestClient,
    viewer_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeClient(FakePrediction("AUTH", 0.97))
    monkeypatch.setattr(diagnosis_service, "model_client", lambda: fake)
    body = client.post(
        "/api/v1/diagnosis/classify", json={"log": CODE_LOG}, headers=viewer_headers
    ).json()
    assert (body["regex"]["category"], body["final"]["category"]) == ("CODE_BUG", "AUTH")
    assert body["final"]["source"] == "model" and body["window"] == "window text"
    assert body["model"]["category"] == "AUTH"


def test_analyzer_size_cap_and_auth(client: TestClient, viewer_headers: dict[str, str]) -> None:
    big = "x" * (300 * 1024)
    response = client.post("/api/v1/diagnosis/classify", json={"log": big}, headers=viewer_headers)
    assert response.status_code == 413
    assert client.post("/api/v1/diagnosis/classify", json={"log": "x"}).status_code == 401
