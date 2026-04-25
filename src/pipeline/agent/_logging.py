"""JSON event sink for the F4 agent loop (design §13).

Loop / observer / planner / executor modules all share the same
``AgentEventLogger`` so every observable lifecycle moment ends up in
``logs/agent.jsonl`` as a one-line JSON record AND on stdout via
``structlog`` for human-readable summaries.

Determinism: the wall clock is injected as a ``Callable[[],
datetime]`` so tests can pin ``ts`` and assert byte-identical log
output across runs (NFR-06).
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import structlog

# F7.3 — secret redaction primitives.
_REDACTED: Final[str] = "<redacted>"
_SECRET_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r".*(_KEY|_TOKEN|_SECRET|_PASSWORD)$",
    re.IGNORECASE,
)
"""Match any field name ending in `_KEY`, `_TOKEN`, `_SECRET`, or
`_PASSWORD` (case-insensitive). Examples that match:
``DASHSCOPE_API_KEY``, ``slack_webhook_token``, ``foo_secret``,
``DB_PASSWORD``."""

_SECRET_VALUE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"sk-[A-Za-z0-9]{20,}",
)
"""Match Anthropic / DashScope / OpenAI-style API keys that may have
leaked into a payload via an exception message or upstream response."""


def redact_secrets(value: Any, *, _key: str | None = None) -> Any:
    """Recursively mask secret-bearing values in ``value``.

    Two heuristics:

    1. If ``_key`` matches :data:`_SECRET_KEY_PATTERN`, return
       ``"<redacted>"`` regardless of the value's content.
    2. If ``value`` is a string and contains a substring matching
       :data:`_SECRET_VALUE_PATTERN` (e.g. ``sk-...``), return
       ``"<redacted>"`` even when the key itself looks innocuous.

    Dicts and lists / tuples are walked depth-first; other primitive
    types pass through unchanged. Designed to be cheap on the agent
    loop hot path (regex pre-compiled, no copy when nothing needs
    masking).
    """
    if _key is not None and _SECRET_KEY_PATTERN.fullmatch(_key):
        return _REDACTED
    if isinstance(value, str):
        return _REDACTED if _SECRET_VALUE_PATTERN.search(value) else value
    if isinstance(value, dict):
        return {k: redact_secrets(v, _key=k) for k, v in value.items()}
    if isinstance(value, list | tuple):
        walked = [redact_secrets(item) for item in value]
        return tuple(walked) if isinstance(value, tuple) else walked
    return value


def redact_secrets_processor(
    _logger: Any, _method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Structlog processor wrapper around :func:`redact_secrets`.

    Signature matches structlog's ``Processor`` protocol so this can
    be slotted into a ``structlog.configure(processors=[...])`` chain.
    """
    return redact_secrets(event_dict)  # type: ignore[no-any-return]

DEFAULT_LOG_PATH: Final[Path] = Path("logs/agent.jsonl")
"""Sink for structured JSON events. Same path as the escalator's
single-event writer (design §9 + §13)."""

# ---------------------------------------------------------------------------
# Canonical event names (design §13 table). Pinned constants so a
# rename is a one-place edit + a test failure rather than a silent
# log-shape break for downstream consumers.
# ---------------------------------------------------------------------------

EVENT_LOOP_STARTED: Final[str] = "loop_started"
EVENT_LOOP_ITERATION: Final[str] = "loop_iteration"
EVENT_BATCH_STARTED: Final[str] = "batch_started"
EVENT_LAYER_STARTED: Final[str] = "layer_started"
EVENT_LAYER_COMPLETED: Final[str] = "layer_completed"
EVENT_FAILURE_DETECTED: Final[str] = "failure_detected"
EVENT_FIX_APPLIED: Final[str] = "fix_applied"
EVENT_ESCALATION: Final[str] = "escalation"
EVENT_LOOP_STOPPED: Final[str] = "loop_stopped"

CANONICAL_EVENTS: Final[tuple[str, ...]] = (
    EVENT_LOOP_STARTED,
    EVENT_LOOP_ITERATION,
    EVENT_BATCH_STARTED,
    EVENT_LAYER_STARTED,
    EVENT_LAYER_COMPLETED,
    EVENT_FAILURE_DETECTED,
    EVENT_FIX_APPLIED,
    EVENT_ESCALATION,
    EVENT_LOOP_STOPPED,
)


Clock = Callable[[], datetime]
"""Wall-clock provider. ``default_clock`` returns ``datetime.now(tz=UTC)``;
tests pass a fixed-value lambda so log output stays byte-stable."""


def default_clock() -> datetime:
    return datetime.now(tz=UTC)


def fixed_clock(when: datetime) -> Clock:
    """Return a clock that always reports ``when`` — convenience for
    tests that pin ``ts`` to make log diffs byte-identical."""
    return lambda: when


class AgentEventLogger:
    """Structured JSON event sink used by the agent loop modules.

    Every call to :meth:`event` appends one JSON line to
    ``log_path`` AND emits a parallel ``structlog.info`` event so
    operators tailing stdout see a human-readable summary.
    """

    def __init__(
        self,
        *,
        log_path: Path = DEFAULT_LOG_PATH,
        clock: Clock | None = None,
        stdout_logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self._log_path = log_path
        self._clock = clock or default_clock
        self._stdout = stdout_logger or structlog.get_logger("pipeline.agent")

    @property
    def log_path(self) -> Path:
        return self._log_path

    def event(self, name: str, **fields: Any) -> dict[str, Any]:
        """Append one canonical event. Returns the payload that was
        written so callers (and tests) can assert on it directly.

        ``ts`` is computed from the injected clock — never from
        ``datetime.now`` directly — so deterministic-replay tests
        can pin it.
        """
        # F7.3: scrub secret-bearing keys/values BEFORE the payload
        # touches disk or stdout. Catches stray API keys or tokens
        # that may have been spliced in via runner-level **fields.
        safe_fields = redact_secrets(fields)
        payload: dict[str, Any] = {
            "event": name,
            "ts": self._clock().isoformat(),
            **safe_fields,
        }
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self._stdout.info(name, **safe_fields)
        return payload
