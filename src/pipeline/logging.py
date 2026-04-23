"""Structured logging configuration.

Emits JSON lines to ``stdout`` so logs are grep-friendly and ship-ready
for any log aggregator. Context (``run_id``, ``batch_id``,
``conversation_id``) is bound to the logger by callers and automatically
included in every event.

Import :func:`get_logger` everywhere instead of the stdlib ``logging``
module.
"""

from __future__ import annotations

# LEARN: ``logging`` is Python's built-in logging framework. We keep it
# because structlog delegates level-filtering to it under the hood.
import logging
import re
import sys
from typing import Any

# LEARN: ``structlog`` is a layer on top of ``logging`` that produces
# structured events (key/value pairs) instead of plain strings. JSON logs
# are machine-readable; ``grep 'batch_id=xyz'`` on them is trivial.
import structlog
from pydantic import SecretStr
from structlog.typing import EventDict, FilteringBoundLogger, WrappedLogger

# LEARN: we need a module-scoped flag for "was logging configured yet?".
# Pure ``_configured = False`` + reassignment would require the
# ``global`` keyword inside the function. Storing the flag as a dict key
# sidesteps ``global`` (the dict *object* is never reassigned, only
# mutated), which keeps the linter (ruff PLW0603) happy.
_state: dict[str, bool] = {"configured": False}

# LEARN: ``re.compile`` builds a reusable regular-expression object.
# Compiling once at module load is faster than recompiling per call. The
# pattern matches any key that *looks* like a credential name — ``secret``,
# ``password``, ``api_key``, ``api-key``, ``apikey``, ``token``, ``auth``.
_SECRET_KEY_RE: re.Pattern[str] = re.compile(
    r"(secret|password|api[_\-]?key|token|auth)",
    re.IGNORECASE,
)
_REDACTED: str = "***REDACTED***"


# LEARN: this is a structlog *processor*. Processors form a pipeline —
# each one receives the event dict, can transform it, and returns the
# next version. The signature is fixed by structlog:
#   - ``logger``       the underlying wrapped logger (unused here);
#   - ``method_name``  the level name such as ``"info"`` (unused here);
#   - ``event_dict``   the accumulated key/value payload we can mutate.
# Unused parameters are prefixed with ``_`` by Python convention so the
# reader (and the linter) know we are honoring the signature, not using
# the value.
def _redact_secrets(
    _logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Scrub values whose keys hint at credentials before rendering.

    Runs before ``JSONRenderer`` so redacted values never hit stdout. Any
    :class:`pydantic.SecretStr` value is redacted regardless of key name.
    """
    # LEARN: we iterate over ``list(event_dict.items())`` rather than
    # ``event_dict.items()`` directly because we *mutate* the dict inside
    # the loop — changing a dict while iterating its live view raises
    # ``RuntimeError: dictionary changed size during iteration`` in some
    # paths. ``list(...)`` takes a snapshot first.
    for key, value in list(event_dict.items()):
        # LEARN: ``isinstance`` checks type membership. We catch two
        # cases: (a) the value is a Pydantic SecretStr (always redact),
        # (b) the key name matches our "looks like a credential" regex.
        # ``_SECRET_KEY_RE.search(str(key))`` returns a truthy match
        # object or ``None`` — perfect for a boolean ``or``.
        if isinstance(value, SecretStr) or _SECRET_KEY_RE.search(str(key)):
            event_dict[key] = _REDACTED
    return event_dict


def configure_logging(level: str = "INFO") -> None:
    """Configure structlog + stdlib logging. Idempotent."""
    # LEARN: idempotent = calling twice has the same effect as once. The
    # guard avoids re-registering processors if the CLI re-enters the
    # setup path in the same process (tests, hot reload).
    if _state["configured"]:
        return

    # LEARN: stdlib ``logging`` uses numeric levels (DEBUG=10, INFO=20,
    # WARNING=30, ...). ``getattr(logging, "INFO", ...)`` is a dynamic
    # attribute lookup with a fallback — cheaper than a dict + KeyError
    # dance. We uppercase the user input to match the stdlib naming.
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # LEARN: ``basicConfig`` wires the root logger so everything that
    # uses stdlib logging (many libraries do) flows through our config.
    # ``force=True`` resets any prior handlers — essential in tests where
    # another module may have set up logging already.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
        force=True,
    )

    # LEARN: structlog's processor *list* is the core of the library.
    # Each event flows through it in order. Reading the chain like a
    # pipeline:
    #   1. merge_contextvars    pull ``run_id`` / ``batch_id`` etc. from
    #                           the context-local store into the event;
    #   2. add_log_level        stamp the severity onto the event;
    #   3. TimeStamper          add an ISO-8601 UTC timestamp;
    #   4. StackInfoRenderer    render Python stack info when requested;
    #   5. format_exc_info      turn ``exc_info=True`` into readable text;
    #   6. _redact_secrets      OUR processor — scrubs credential-ish
    #                           fields BEFORE serialization;
    #   7. JSONRenderer         turn the dict into a JSON line.
    # Order matters: redaction must run *before* the renderer so the
    # secret never reaches the output at all.
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _redact_secrets,  # scrub before rendering
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=False,
    )

    _state["configured"] = True


def get_logger(name: str | None = None) -> FilteringBoundLogger:
    """Return a structlog bound logger. Call :func:`configure_logging` first."""
    # LEARN: ``X | None`` is Python 3.10+ syntax for "X or None". Older
    # code wrote ``Optional[str]`` from ``typing``. Same meaning.
    logger: FilteringBoundLogger = (
        structlog.get_logger(name) if name else structlog.get_logger()
    )
    return logger


# LEARN: ``**kwargs`` captures arbitrary keyword arguments into a dict.
# Callers say ``bind_context(run_id="abc", batch_id="xyz")``; the
# function receives ``kwargs = {"run_id": "abc", "batch_id": "xyz"}``.
# structlog's ``bind_contextvars`` writes them into Python's
# ``contextvars`` — a thread-safe, async-safe store so every log event
# in THIS task picks them up, without leaking into other tasks.
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
