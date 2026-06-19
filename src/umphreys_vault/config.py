"""Configuration via pydantic-settings.

All knobs default to safe values; production overrides come from env vars
(loaded from ``.env`` on nix1, never committed).

The ATU API is public (no auth, no key), so unlike the Phish vault there are
no upstream API-key secrets to manage here.
"""

from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for ETL + status endpoint."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---- Postgres -----------------------------------------------------
    pg_host: str = Field(default="postgres", description="Postgres host")
    pg_port: int = Field(default=5432, description="Postgres port")
    pg_db: str = Field(default="umphreys", description="Postgres database name")
    pg_user: str = Field(default="umphreys", description="Postgres user")
    pg_password: SecretStr = Field(default=SecretStr(""), description="Postgres password")

    @property
    def pg_dsn(self) -> str:
        """asyncpg-compatible DSN. Password is unwrapped only here."""
        pw = self.pg_password.get_secret_value()
        return f"postgresql://{self.pg_user}:{pw}@{self.pg_host}:{self.pg_port}/{self.pg_db}"

    # ---- Upstream API (All Things Umphreys) ---------------------------
    atu_base_url: str = Field(
        default="https://allthings.umphreys.com/api/v2",
        description="ATU public REST API v2 base. No auth, no key.",
    )
    atu_artist_id: int = Field(default=1, description="ATU artist id (1 = Umphrey's McGee).")

    # ---- ETL behavior -------------------------------------------------
    etl_concurrency: int = Field(default=4, ge=1, le=16)
    etl_throttle_atu_rps: float = Field(
        default=3.0,
        gt=0,
        description="Polite throttle: there's no key, so keep request rate low.",
    )
    etl_request_timeout_s: float = Field(default=20.0, gt=0)
    etl_dry_run: bool = Field(default=False, description="If true, fetch + log but write nothing.")

    # ---- Status endpoint (optional) -----------------------------------
    status_host: str = Field(default="0.0.0.0")
    status_port: int = Field(default=3719)

    # ---- Logging ------------------------------------------------------
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json", description='"json" or "text"')


def get_settings() -> Settings:
    """Build settings, validating required envs at call time.

    Caller is responsible for handling validation errors so the CLI can
    print a clean message rather than a stack trace.
    """
    return Settings()
