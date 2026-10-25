import os

# Configure the environment before any app module reads settings.
os.environ.update(
    {
        "ENVIRONMENT": "development",
        "DEBUG": "false",
        "DATABASE_URL": "sqlite://",
        "JWT_SECRET_KEY": "test-jwt-secret-key-that-is-long-enough-0123456789",
        "ENCRYPTION_KEY": "5ce7pnzbhWn6iVmlv3zGtFnEAH5OhEpOSzb_Y8f4Ehk=",
        "ADMIN_EMAIL": "admin@example.com",
        "ADMIN_PASSWORD": "CHANGE_ME",
        "AIRFLOW_ALLOWED_HOSTS": "[]",
        "DIAGNOSTIC_LLM_ENABLED": "false",
    }
)

from collections.abc import Iterator  # noqa: E402

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy.orm import Session, sessionmaker  # noqa: E402

from app import models  # noqa: E402,F401
from app.core.config import get_settings  # noqa: E402
from app.db.base import Base  # noqa: E402
from app.db.session import create_db_engine, create_session_factory  # noqa: E402
from app.main import create_app  # noqa: E402
from app.models.user import User, UserRole  # noqa: E402
from app.orchestration.airflow import client as airflow_client  # noqa: E402
from app.orchestration.airflow import factory as airflow_factory  # noqa: E402
from app.orchestration.airflow import mock as airflow_mock  # noqa: E402
from app.schemas.user import UserCreate  # noqa: E402
from app.services import (
    automation_nodes,  # noqa: E402
    user_service,  # noqa: E402
)

PASSWORD = "correct-horse-battery"


# Set TEST_DATABASE_URL (e.g. a disposable PostgreSQL database) to run the whole suite against a
# real server instead of in-memory SQLite. The schema is dropped and recreated for every test.
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture
def session_factory() -> Iterator[sessionmaker[Session]]:
    engine = create_db_engine(TEST_DATABASE_URL or "sqlite://")
    if TEST_DATABASE_URL:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield create_session_factory(engine)
    engine.dispose()


@pytest.fixture
def db(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    with session_factory() as session:
        yield session


@pytest.fixture
def client(session_factory: sessionmaker[Session]) -> Iterator[TestClient]:
    app = create_app(session_factory=session_factory, bootstrap=False, detection=False)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def _reset_globals(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    settings = get_settings()
    monkeypatch.setattr(settings, "ENVIRONMENT", "development")
    monkeypatch.setattr(settings, "AIRFLOW_ALLOWED_HOSTS", [])
    monkeypatch.setattr(airflow_factory, "live_transport", None)
    monkeypatch.setattr(automation_nodes, "webhook_transport", None)
    airflow_client.clear_token_cache()
    airflow_mock.reset_state()
    yield


def make_user(
    db: Session, role: UserRole, email: str | None = None, password: str = PASSWORD
) -> User:
    return user_service.create_user(
        db,
        UserCreate(
            email=email or f"{role.lower()}@example.com",
            full_name=f"{role.title()} User",
            password=password,
            role=role,
        ),
        actor=None,
    )


def login(client: TestClient, email: str, password: str = PASSWORD) -> dict[str, str]:
    response = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.fixture
def admin(db: Session) -> User:
    return make_user(db, UserRole.ADMIN)


@pytest.fixture
def operator(db: Session) -> User:
    return make_user(db, UserRole.OPERATOR)


@pytest.fixture
def viewer(db: Session) -> User:
    return make_user(db, UserRole.VIEWER)


@pytest.fixture
def admin_headers(client: TestClient, admin: User) -> dict[str, str]:
    return login(client, admin.email)


@pytest.fixture
def operator_headers(client: TestClient, operator: User) -> dict[str, str]:
    return login(client, operator.email)


@pytest.fixture
def viewer_headers(client: TestClient, viewer: User) -> dict[str, str]:
    return login(client, viewer.email)
