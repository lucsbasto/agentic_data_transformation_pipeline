"""Structured logging configuration.

Emits JSON lines to ``stdout`` so logs are grep-friendly and ship-ready
for any log aggregator. Context (``run_id``, ``batch_id``,
``conversation_id``) is bound to the logger by callers and automatically
included in every event.

Import :func:`get_logger` everywhere instead of the stdlib ``logging``
module.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.typing import FilteringBoundLogger

# Module-level flag stored in a dict to dodge ``global`` (ruff PLW0603).
_state: dict[str, bool] = {"configured": False}


def configure_logging(level: str = "INFO") -> None:
    """Configure structlog + stdlib logging. Idempotent."""
    if _state["configured"]:
        return

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
        force=True,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=False,
    )

    _state["configured"] = True


def get_logger(name: str | None = None) -> FilteringBoundLogger:
    """Return a structlog bound logger. Call :func:`configure_logging` first."""
    logger: FilteringBoundLogger = (
        structlog.get_logger(name) if name else structlog.get_logger()
    )
    return logger


def bind_context(**kwargs: Any) -> None:
    """Bind correlation ids onto the context-local logger for this task."""
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Reset the context-local logger. Call between top-level units of work."""
    structlog.contextvars.clear_contextvars()


def _reset_for_tests() -> None:
    """Test-only helper: let tests re-configure logging from scratch."""
    _state["configured"] = False
    structlog.reset_defaults()
