"""Server configuration (env-driven via pydantic-settings).

Every knob has an ``ETL_``-prefixed env var (e.g. ``ETL_DATABASE_URL``). The
dev defaults let the app boot with nothing configured -- SQLite, a derived dev
master key, an insecure JWT secret -- but production MUST override the secrets
and point at PostgreSQL/Redis. The settings object also builds the two policy
objects the server hands to the engine: the SSRF policy and the secrets cipher.
"""
from __future__ import annotations

import base64
import hashlib
from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from etl_core.crypto import Cipher, make_cipher
from etl_core.ssrf import SSRFPolicy


def _dev_master_key() -> str:
    """A deterministic, INSECURE Fernet key so the app boots in dev without
    configuration. Overridden by ``ETL_MASTER_KEY`` in any real deployment."""
    return base64.urlsafe_b64encode(hashlib.sha256(b"etl-tool-dev-master-key").digest()).decode()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ETL_", env_file=".env", extra="ignore")

    # storage / queue
    database_url: str = "sqlite+aiosqlite:///./etl_server.db"
    redis_url: str = "redis://localhost:6379"

    # auth
    jwt_secret: str = "dev-insecure-jwt-secret-change-me"
    jwt_algorithm: str = "HS256"
    access_token_ttl_minutes: int = 60 * 24

    # secrets at rest
    secrets_algo: Literal["fernet", "aes-gcm"] = "fernet"
    master_key: str = Field(default_factory=_dev_master_key)

    # SSRF guard for every server-issued request (HTTP + DB), same as the engine
    ssrf_enabled: bool = True
    ssrf_allow_hosts: list[str] = Field(default_factory=list)

    # engine run options
    max_run_concurrency: int = 8

    # create tables on startup instead of Alembic (tests/dev convenience)
    create_tables_on_startup: bool = False

    @field_validator("ssrf_allow_hosts", mode="before")
    @classmethod
    def _split_hosts(cls, value: object) -> object:
        # Accept a comma-separated string from the environment as well as JSON.
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    def ssrf_policy(self) -> SSRFPolicy:
        return SSRFPolicy(enabled=self.ssrf_enabled, allow_hosts=list(self.ssrf_allow_hosts))

    def secrets_cipher(self) -> Cipher:
        return make_cipher(self.secrets_algo, self.master_key)


@lru_cache
def get_settings() -> Settings:
    return Settings()
