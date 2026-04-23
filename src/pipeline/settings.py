"""Runtime configuration.

Backed by ``pydantic-settings``. Reads ``.env`` at startup, validates
types, and fails fast on missing required fields. Treat the resulting
``Settings`` instance as immutable; inject it explicitly into components
instead of reading env vars deep in the call stack.
"""

from __future__ import annotations

from pathlib import Path

# LEARN: Pydantic is a schema + validation library. We import four specific
# helpers here:
#   - ``AnyHttpUrl``  — a type that rejects non-URL strings at construction.
#   - ``Field``       — attaches extra rules (``default=``, ``ge=``, ``le=``,
#                       ``description=``) to a model attribute.
#   - ``SecretStr``   — a string wrapper whose ``str(...)`` / ``repr(...)``
#                       prints ``"**********"``. Critical for LLM API keys:
#                       a stray ``print(settings)`` can never leak the key.
#   - ``field_validator`` — a decorator that turns a method into a custom
#                       validator that runs when the field is populated.
from pydantic import AnyHttpUrl, Field, SecretStr, field_validator

# LEARN: ``pydantic-settings`` is a Pydantic plug-in that reads values from
# environment variables (and optionally from ``.env`` files). By
# subclassing ``BaseSettings``, every field on our class becomes
# auto-populated from ``os.environ`` at startup, with type coercion and
# validation baked in.
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipeline.errors import ConfigError
from pipeline.paths import project_root


# LEARN: ``Settings`` is a Pydantic model — a class whose attributes are
# *declared* with type hints. Pydantic reads those hints at import time,
# generates a constructor, and validates inputs against the types. A
# mistyped ``PIPELINE_RETRY_BUDGET=abc`` in ``.env`` would raise here,
# BEFORE any pipeline code runs.
class Settings(BaseSettings):
    """Central config object. One instance per process."""

    # LEARN: ``...`` (literally three dots, the ``Ellipsis`` object) means
    # "no default, this field is required". Loading settings without the
    # env var set raises a ``ValidationError``.
    anthropic_api_key: SecretStr = Field(
        ..., description="LLM provider API key. Reused for DashScope via the Anthropic SDK."
    )
    # LEARN: ``AnyHttpUrl`` validates the string looks like an HTTP(S)
    # URL — no bare hostnames, no "ftp://". A typo in ``.env`` fails fast.
    anthropic_base_url: AnyHttpUrl = Field(
        default=AnyHttpUrl("https://coding-intl.dashscope.aliyuncs.com/apps/anthropic"),
        description="Anthropic-protocol-compatible endpoint.",
    )
    # LEARN: plain defaults for the model names. DashScope exposes two
    # Qwen variants; the code uses ``primary`` for normal calls and falls
    # back to ``fallback`` only after the retry budget is spent. See
    # ``llm/client.py`` for the fallback machinery.
    llm_model_primary: str = Field(default="qwen3-max")
    llm_model_fallback: str = Field(default="qwen3-coder-plus")

    # LEARN: ``ge`` / ``le`` stand for *greater-than-or-equal* / *less-
    # than-or-equal*. Pydantic will reject ``PIPELINE_RETRY_BUDGET=0`` or
    # ``=99``. Bounds that live on the type keep the domain rule next to
    # the definition instead of scattered as runtime ``if`` checks.
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

    # LEARN: ``model_config`` is Pydantic v2's way to configure the model
    # itself (v1 used an inner ``class Config``). The dict-like
    # ``SettingsConfigDict`` tells pydantic-settings:
    #   - read ``.env`` from the current working directory;
    #   - use UTF-8 to decode it;
    #   - silently ignore extra env vars we do not declare;
    #   - accept uppercase / lowercase names interchangeably.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # LEARN: stacked decorators read bottom-up. ``@classmethod`` makes the
    # method receive the class as ``cls`` instead of an instance as
    # ``self``. ``@field_validator("pipeline_log_level")`` tells Pydantic
    # "run this after the field's normal type-check passes". Returning a
    # value replaces the incoming one, so this validator normalizes
    # ``info`` / ``Info`` / ``INFO`` to the canonical uppercase form.
    @field_validator("pipeline_log_level")
    @classmethod
    def _upper_log_level(cls, value: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = value.upper()
        if upper not in allowed:
            # LEARN: ``!r`` inside an f-string calls ``repr()`` on the
            # value — quotes strings, shows them unambiguously. Handy for
            # error messages that must distinguish ``"INFO"`` from
            # ``INFO`` (a name).
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
        # LEARN: ``any(...)`` with a generator expression short-circuits
        # on the first True. ``value.parts`` splits a ``Path`` into its
        # components: ``Path("a/../b").parts == ("a", "..", "b")``.
        if any(part == ".." for part in value.parts):
            raise ValueError(
                f"'..' segments are not allowed in PIPELINE_STATE_DB: {value!s}"
            )
        return value

    def state_db_path(self) -> Path:
        """Resolve ``pipeline_state_db`` against the project root when relative."""
        path = self.pipeline_state_db
        # LEARN: ternary expression — ``value_if_true if cond else
        # value_if_false``. Keeps a one-line branch readable.
        return path if path.is_absolute() else project_root() / path

    @classmethod
    def load(cls) -> Settings:
        """Load settings from the environment. Raise ``ConfigError`` on failure."""
        try:
            # LEARN: ``cls()`` instantiates this class. The ``# type:
            # ignore`` tells mypy "trust me, Pydantic fills in the args
            # from the environment" — mypy cannot see that machinery.
            return cls()  # type: ignore[call-arg]
        except Exception as exc:
            # LEARN: ``raise ... from exc`` chains exceptions. The user
            # sees "ConfigError: failed to load settings: ..." with the
            # original ValidationError as the cause. Beats hiding the
            # root problem and then debugging a generic error.
            raise ConfigError(f"failed to load settings: {exc}") from exc
