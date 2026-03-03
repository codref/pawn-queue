"""
pawn_queue.config
~~~~~~~~~~~~~~~~~
Pydantic v2 settings model.  Loads from a YAML file, then overlays
environment variables.

Environment variable mapping:
  PAWNQUEUE_S3_ENDPOINT_URL   → s3.endpoint_url
  PAWNQUEUE_S3_BUCKET_NAME    → s3.bucket_name
  PAWNQUEUE_S3_ACCESS_KEY     → s3.aws_access_key_id
  PAWNQUEUE_S3_SECRET_KEY     → s3.aws_secret_access_key
  PAWNQUEUE_S3_REGION         → s3.region_name
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from .exceptions import ConfigError


class S3Config(BaseModel):
    endpoint_url: str
    bucket_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str = "us-east-1"
    use_ssl: bool = True


class PollingConfig(BaseModel):
    interval_seconds: float = 5.0
    max_messages_per_poll: int = 10
    visibility_timeout_seconds: float = 30.0
    lease_refresh_interval_seconds: float = 10.0
    jitter_max_ms: int = 200


class CsprngVerifyConfig(BaseModel):
    jitter_min_ms: int = 100
    jitter_max_ms: int = 400
    verify_retries: int = 2
    verify_retry_delay_ms: int = 150


class ConcurrencyConfig(BaseModel):
    strategy: Literal["auto", "conditional_write", "csprng_verify"] = "auto"
    csprng_verify: CsprngVerifyConfig = Field(default_factory=CsprngVerifyConfig)


class RegistryConfig(BaseModel):
    heartbeat_interval_seconds: float = 60.0


class PawnQueueConfig(BaseModel):
    s3: S3Config
    polling: PollingConfig = Field(default_factory=PollingConfig)
    concurrency: ConcurrencyConfig = Field(default_factory=ConcurrencyConfig)
    registry: RegistryConfig = Field(default_factory=RegistryConfig)

    # Runtime-resolved strategy (set after compat probe)
    _resolved_strategy: Literal["conditional_write", "csprng_verify"] | None = None

    @model_validator(mode="before")
    @classmethod
    def _apply_env_overrides(cls, values: dict) -> dict:
        """Overlay environment variables on top of YAML values."""
        s3 = values.get("s3", {})
        env_map = {
            "PAWNQUEUE_S3_ENDPOINT_URL": "endpoint_url",
            "PAWNQUEUE_S3_BUCKET_NAME": "bucket_name",
            "PAWNQUEUE_S3_ACCESS_KEY": "aws_access_key_id",
            "PAWNQUEUE_S3_SECRET_KEY": "aws_secret_access_key",
            "PAWNQUEUE_S3_REGION": "region_name",
        }
        for env_key, field in env_map.items():
            val = os.environ.get(env_key)
            if val is not None:
                s3[field] = val
        values["s3"] = s3
        return values

    @classmethod
    def from_yaml(cls, path: str | Path) -> "PawnQueueConfig":
        path = Path(path)
        if not path.exists():
            raise ConfigError(f"Config file not found: {path}")
        try:
            raw = yaml.safe_load(path.read_text())
        except yaml.YAMLError as exc:
            raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise ConfigError(f"Config file {path} must be a YAML mapping.")
        return cls.model_validate(raw)

    @classmethod
    def from_dict(cls, data: dict) -> "PawnQueueConfig":
        return cls.model_validate(data)

    def effective_strategy(self) -> Literal["conditional_write", "csprng_verify"]:
        """Returns the resolved strategy (set after compat probe); falls back to csprng_verify."""
        if self._resolved_strategy is not None:
            return self._resolved_strategy
        if self.concurrency.strategy != "auto":
            return self.concurrency.strategy  # type: ignore[return-value]
        return "csprng_verify"

    def set_resolved_strategy(self, strategy: Literal["conditional_write", "csprng_verify"]) -> None:
        object.__setattr__(self, "_resolved_strategy", strategy)

    model_config = {"arbitrary_types_allowed": True}
