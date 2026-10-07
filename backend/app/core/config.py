import base64
import hashlib
import logging
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

BACKEND_DIR = Path(__file__).resolve().parents[2]
PLACEHOLDER = "CHANGE_ME"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BACKEND_DIR / ".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Application
    PROJECT_NAME: str = "Agentic Data Automation"
    VERSION: str = "0.1.0"
    ENVIRONMENT: Literal["development", "staging", "production"] = "development"
    DEBUG: bool = False
    API_V1_STR: str = "/api/v1"
    CORS_ORIGINS: list[str] = ["http://localhost:5173", "http://127.0.0.1:5173"]

    # Database
    DATABASE_URL: str = "sqlite:///./agentic_ai.db"
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20

    # Security & JWT
    JWT_SECRET_KEY: str = PLACEHOLDER
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    LOGIN_MAX_ATTEMPTS: int = 5
    LOGIN_LOCKOUT_MINUTES: int = 15

    # Secret encryption (Fernet); comma-separated, first key encrypts
    ENCRYPTION_KEY: str = PLACEHOLDER

    # Initial admin (only used when the users table is empty)
    ADMIN_EMAIL: str = "admin@example.com"
    ADMIN_PASSWORD: str = PLACEHOLDER

    # Airflow
    AIRFLOW_ALLOWED_HOSTS: list[str] = []
    AIRFLOW_CONNECT_TIMEOUT_SECONDS: float = 5
    AIRFLOW_READ_TIMEOUT_SECONDS: float = 10
    # Connection monitor: re-checks every active connection and refreshes its DAG list. A
    # connection counts as live only if it answered within ~2 intervals (see airflow_service).
    AIRFLOW_MONITOR_ENABLED: bool = True
    AIRFLOW_MONITOR_INTERVAL_SECONDS: int = 30
    # Zero preserves the existing every-cycle monitor behavior; raise this to reuse fresh DAG lists.
    AIRFLOW_DAG_SYNC_MIN_INTERVAL_SECONDS: int = 0

    # Database connections (catalog). Empty allowlist = any host. The connection monitor
    # re-checks them on the same interval as Airflow connections.
    DB_CONNECTION_ALLOWED_HOSTS: list[str] = []
    DB_CONNECT_TIMEOUT_SECONDS: int = 5

    # Detection
    DETECTION_ENABLED: bool = True
    DETECTION_INTERVAL_SECONDS: int = 120
    DETECTION_LOOKBACK_HOURS: int = 24
    EVIDENCE_LOG_MAX_BYTES: int = 65536
    EVIDENCE_MAX_LOGS: int = 3

    # Automation (Plan 2)
    AUTOMATION_ENABLED: bool = True
    AUTOMATION_INTERVAL_SECONDS: int = 30  # how often waiting/pending runs are advanced
    AUTOMATION_FORCE_DRY_RUN: bool = False  # True = no workflow may change anything
    AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY: int = 3
    AUTOMATION_MAX_STEPS_PER_RUN: int = 50
    AUTOMATION_VERIFY_POLL_SECONDS: int = 60
    AUTOMATION_TRIGGER_RESERVATION_SECONDS: int = 300
    AUTOMATION_WEBHOOK_ALLOWED_HOSTS: list[str] = []
    AUTOMATION_WEBHOOK_TIMEOUT_SECONDS: float = 5
    # Where people open the app; used for links in emails/Slack/Teams (run and approval pages).
    PUBLIC_APP_URL: str = "http://localhost:5173"
    SMTP_TIMEOUT_SECONDS: float = 10

    # ML failure classifier (aiplan1). Disabled = regex only, exactly as before. The model is
    # consulted only when regex says UNKNOWN or CODE_BUG, and only overrides it at
    # >= ML_MIN_CONFIDENCE; below that its answer is stored as a display-only suggestion.
    ML_SERVICE_ENABLED: bool = False
    ML_SERVICE_URL: str = "http://127.0.0.1:8001"
    ML_SERVICE_TOKEN: str = ""
    ML_SERVICE_TIMEOUT_SECONDS: float = 2
    ML_MIN_CONFIDENCE: float = 0.85
    ML_BREAKER_FAILURES: int = 3
    ML_BREAKER_COOLDOWN_SECONDS: int = 60

    # Local LLM (Ollama) that may pick the fix in "Choose the fix" blocks set to suggest/decide.
    # It only ever chooses among the fixes the rules allow; disabled = the rules decide alone.
    DECISION_LLM_ENABLED: bool = False
    DECISION_LLM_URL: str = "http://127.0.0.1:11434"
    DECISION_LLM_MODEL: str = "qwen3:4b"
    DECISION_LLM_TIMEOUT_SECONDS: float = 60  # the first call loads the model
    DECISION_LLM_BREAKER_FAILURES: int = 3
    DECISION_LLM_BREAKER_COOLDOWN_SECONDS: int = 120

    @model_validator(mode="after")
    def _check_production_safety(self) -> "Settings":
        if self.ENVIRONMENT == "development":
            return self
        problems = []
        for name in ("JWT_SECRET_KEY", "ENCRYPTION_KEY", "ADMIN_PASSWORD"):
            if getattr(self, name) == PLACEHOLDER:
                problems.append(f"{name} must be set (currently {PLACEHOLDER!r})")
        if self.DEBUG:
            problems.append("DEBUG must be False")
        if self.DATABASE_URL.startswith("sqlite"):
            problems.append("DATABASE_URL must not use SQLite")
        if len(self.JWT_SECRET_KEY) < 32:
            problems.append("JWT_SECRET_KEY must be at least 32 characters")
        if problems:
            raise ValueError(
                f"Unsafe configuration for ENVIRONMENT={self.ENVIRONMENT}: " + "; ".join(problems)
            )
        return self

    @property
    def is_development(self) -> bool:
        return self.ENVIRONMENT == "development"

    @property
    def jwt_secret(self) -> str:
        if self.JWT_SECRET_KEY == PLACEHOLDER:
            # Development only (production is rejected above): stable, non-secret dev key.
            return hashlib.sha256(b"agentic-dev-jwt-secret").hexdigest()
        return self.JWT_SECRET_KEY

    @property
    def encryption_keys(self) -> list[str]:
        if self.ENCRYPTION_KEY == PLACEHOLDER:
            digest = hashlib.sha256(b"agentic-dev-encryption-key").digest()
            return [base64.urlsafe_b64encode(digest).decode()]
        return [k.strip() for k in self.ENCRYPTION_KEY.split(",") if k.strip()]


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    if settings.is_development:
        for name in ("JWT_SECRET_KEY", "ENCRYPTION_KEY"):
            if getattr(settings, name) == PLACEHOLDER:
                logger.warning("%s is not set; using an insecure development default", name)
    return settings
