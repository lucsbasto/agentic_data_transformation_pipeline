"""Runtime configuration.

Backed by ``pydantic-settings``. Reads ``.env`` at startup, validates
types, and fails fast on missing required fields. Treat the resulting
``Settings`` instance as immutable; inject it explicitly into components
instead of reading env vars deep in the call stack.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import AnyHttpUrl, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipeline.errors import ConfigError
from pipeline.paths import project_root


class Settings(BaseSettings):
    """Central config object. One instance per process."""

    anthropic_api_key: SecretStr = Field(
        ..., description="LLM provider API key. Reused for DashScope via the Anthropic SDK."
    )
    anthropic_base_url: AnyHttpUrl = Field(
        default=AnyHttpUrl("https://coding-intl.dashscope.aliyuncs.com/apps/anthropic"),
        description="Anthropic-protocol-compatible endpoint.",
    )
    llm_model_primary: str = Field(default="qwen3-max")
    llm_model_fallback: str = Field(default="qwen3-coder-plus")

    pipeline_retry_budget: int = Field(default=3, ge=1, le=10)
    pipeline_loop_sleep_seconds: int = Field(default=60, ge=1, le=3600)
    pipeline_state_db: Path = Field(default=Path("state/manifest.db"))
    pipeline_log_level: str = Field(default="INFO")
    pipeline_llm_response_cap: int = Field(
        default=200_000,
        ge=1,
        description=(
            "Maximum characters accepted from a single provider response "
            "before it is cached. A hostile or broken upstream can otherwise "
            "poison the SQLite cache with an unbounded blob."
        ),
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    @field_validator("pipeline_log_level")
    @classmethod
    def _upper_log_level(cls, value: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = value.upper()
        if upper not in allowed:
            raise ValueError(f"invalid log level: {value!r} (allowed: {sorted(allowed)})")
        return upper

    @field_validator("pipeline_state_db")
    @classmethod
    def _reject_path_traversal(cls, value: Path) -> Path:
        """Block ``..`` segments in the configured state DB path.

        An attacker (or a mistyped env var) could otherwise point the
        manifest outside the project tree with ``../../tmp/evil.db``.
        Absolute paths are accepted as-is; ``..`` segments are rejected
        regardless of absolute/relative form.
        """
        if any(part == ".." for part in value.parts):
            raise ValueError(
                f"'..' segments are not allowed in PIPELINE_STATE_DB: {value!s}"
            )
        return value

    def state_db_path(self) -> Path:
        """Resolve ``pipeline_state_db`` against the project root when relative."""
        path = self.pipeline_state_db
        return path if path.is_absolute() else project_root() / path

    @classmethod
    def load(cls) -> Settings:
        """Load settings from the environment. Raise ``ConfigError`` on failure."""
        try:
            return cls()  # type: ignore[call-arg]
        except Exception as exc:
            raise ConfigError(f"failed to load settings: {exc}") from exc
