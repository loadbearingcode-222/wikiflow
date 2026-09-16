"""Runtime configuration, read from the environment.

Defaults match the docker-compose stack so that nothing needs configuring to run
locally, while every value stays overridable for CI or a real AWS deployment. The
S3 settings in particular are the only things that would change to point at real
S3 instead of MinIO.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="WIKIFLOW_",
        env_file=".env",
        extra="ignore",
    )

    # --- object storage ---------------------------------------------------
    s3_endpoint_url: str = "http://minio:9000"
    """Point this at AWS (or unset it) to move off MinIO. Nothing else changes."""

    s3_access_key: str = "wikiflow"
    s3_secret_key: str = "wikiflow_dev_secret"
    s3_region: str = "us-east-1"

    raw_bucket: str = "wikiflow-raw"
    lake_bucket: str = "wikiflow-lake"

    # --- source -----------------------------------------------------------
    user_agent: str = "wikiflow/0.1 (portfolio project)"
    """Wikimedia asks automated clients to identify themselves. Keep it honest."""

    request_timeout_seconds: float = 120.0
    """Generous: these are 45-70 MB files from a free, sometimes slow, public mirror."""

    # --- retention --------------------------------------------------------
    raw_retention_days: int = Field(default=7, ge=1)
    """The replay budget. Within this window a parsing bug can be fixed and the
    affected hours reprocessed without re-downloading. See ADR 0003."""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached so that repeated task-level access does not re-read the environment."""
    return Settings()
