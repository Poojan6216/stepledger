"""Configuration: `stepledger.yaml`, then environment variables (`STEPLEDGER_DSN`, ...)."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_DSN = "postgresql://stepledger:stepledger@localhost:5432/stepledger"
SCHEMA_VERSION = 1

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class LedgerConfig(BaseModel):
    store_outputs: Literal["full", "hash_only"] = "full"
    on_ledger_error: Literal["fail", "warn"] = "fail"
    snapshot_first_input: bool = True


class ChunkConfig(BaseModel):
    min: int = 4096
    avg: int = 16384
    max: int = 65536


class StorageConfig(BaseModel):
    enabled: bool = True
    payload_size_threshold: int = 64 * 1024
    dedupe: bool = True
    chunk: ChunkConfig = Field(default_factory=ChunkConfig)


class GcConfig(BaseModel):
    retention_days: int = 30
    margin_days: int = 7
    grace_hours: float = 1.0
    orphan_ref_days: int = 90


class Price(BaseModel):
    input: float
    output: float


def _default_prices() -> dict[str, Price]:
    # USD per 1M tokens, Anthropic first-party API rates as published at
    # https://platform.claude.com/docs/en/about-claude/pricing (checked 2026-09-26).
    # Prices change: set `prices:` in stepledger.yaml for the models you actually use.
    return {
        "claude-haiku-4-5": Price(input=1.0, output=5.0),
        "claude-sonnet-5": Price(input=2.0, output=10.0),
    }


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STEPLEDGER_", env_nested_delimiter="__", extra="ignore"
    )

    schema_version: int = SCHEMA_VERSION
    dsn: str = DEFAULT_DSN
    ledger: LedgerConfig = Field(default_factory=LedgerConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    gc: GcConfig = Field(default_factory=GcConfig)
    prices: dict[str, Price] = Field(default_factory=_default_prices)


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        return _ENV_REF.sub(lambda m: os.environ.get(m.group(1), ""), value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def load_settings(path: str | Path | None = None) -> Settings:
    """Load `stepledger.yaml` (or `$STEPLEDGER_CONFIG`), then let environment variables win."""
    candidate = Path(path or os.environ.get("STEPLEDGER_CONFIG", "stepledger.yaml"))
    file_values: dict[str, Any] = {}
    if candidate.is_file():
        loaded = yaml.safe_load(candidate.read_text()) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"{candidate}: expected a mapping at the top level")
        file_values = _expand(loaded)
        version = file_values.get("schema_version", SCHEMA_VERSION)
        if version != SCHEMA_VERSION:
            raise ValueError(f"{candidate}: schema_version {version} != {SCHEMA_VERSION}")
        if not file_values.get("dsn"):
            file_values.pop("dsn", None)
    env = Settings()
    merged = Settings.model_validate(file_values).model_dump()
    # Environment variables override the file, field by field.
    for name in Settings.model_fields:
        if env.model_fields_set and name in env.model_fields_set:
            merged[name] = env.model_dump()[name]
    return Settings.model_validate(merged)


def resolve_dsn(dsn: str | None = None) -> str:
    return dsn or load_settings().dsn
