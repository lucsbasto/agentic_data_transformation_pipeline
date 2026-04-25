"""Typed runtime configuration via ``pydantic-settings`` (F7.1).

Aggregates every env var the agent loop, manifest, and medallion layers need
into a single immutable object. Validation is type- and bound-checked at load
time so a malformed ``.env`` fails fast instead of crashing mid-batch.

Precedence (highest → lowest):

1. Process environment (``os.environ``)
2. ``.env`` file in the current working directory (UTF-8)
3. Field defaults declared on :class:`Settings`

This is the standard pydantic-settings precedence order; declared here in the
docstring so callers do not have to chase library docs to reason about config.
``DASHSCOPE_API_KEY`` is required (no default) and wrapped in
:class:`pydantic.SecretStr` so it never leaks via ``repr()`` / ``str()``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central typed config. Construct once per process; inject explicitly."""

    DASHSCOPE_API_KEY: SecretStr = Field(
        ..., description="DashScope API key (Anthropic-compatible endpoint)."
    )
    AGENT_RETRY_BUDGET: int = Field(3, ge=1)
    AGENT_DIAGNOSE_BUDGET: int = Field(10, ge=1)
    AGENT_LOOP_INTERVAL: float = Field(60.0, ge=1.0)
    AGENT_LOCK_PATH: Path = Path("state/agent.lock")
    MANIFEST_PATH: Path = Path("state/manifest.db")
    BRONZE_ROOT: Path = Path("data/bronze")
    SILVER_ROOT: Path = Path("data/silver")
    GOLD_ROOT: Path = Path("data/gold")
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    LOG_FORMAT: Literal["json", "console"] = "json"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )
