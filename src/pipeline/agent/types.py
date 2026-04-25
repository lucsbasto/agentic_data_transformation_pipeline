"""Public enums + frozen dataclasses for the agent core (F4 design §3).

Kept dependency-free so every other agent module (`observer`, `planner`,
`executor`, `diagnoser`, `escalator`, `state`) can import these without
pulling in Polars / SQLite. Values mirror the manifest column contracts
in F4 spec §4 — change one, change the other.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum


class ErrorKind(StrEnum):
    """Failure taxonomy for the agent loop (F4 spec §3 F4-RF-05).

    The first four are auto-correctable by a deterministic
    :class:`Fix`; ``UNKNOWN`` short-circuits to immediate escalation
    (no retry budget consumed beyond the first attempt).
    """

    SCHEMA_DRIFT = "schema_drift"
    REGEX_BREAK = "regex_break"
    PARTITION_MISSING = "partition_missing"
    OUT_OF_RANGE = "out_of_range"
    UNKNOWN = "unknown"


class Layer(StrEnum):
    """Pipeline layer the agent currently drives."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class RunStatus(StrEnum):
    """Lifecycle states for an ``agent_runs`` row (F4 spec §4.1).

    ``IN_PROGRESS`` is the row state while the loop body executes.
    The other three are terminal — ``end_run`` writes one of them.
    """

    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    INTERRUPTED = "INTERRUPTED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class Fix:
    """A deterministic recovery action emitted by the diagnoser.

    ``apply`` is a zero-arg callable that must be idempotent — the
    executor re-runs the layer entrypoint after ``apply`` returns, and
    a partial second application must not corrupt manifest or data.
    """

    kind: str
    description: str
    apply: Callable[[], None]
    requires_llm: bool = False


@dataclass(frozen=True)
class FailureRecord:
    """One row of ``manifest.agent_failures`` (F4 spec §4.2).

    ``last_error_msg`` is sanitized + capped to 512 chars by the
    caller (escalator / state writer); this dataclass does not enforce
    the cap so unit tests can round-trip raw values.
    """

    batch_id: str
    layer: Layer
    error_class: ErrorKind
    attempts: int
    last_fix_kind: str | None
    last_error_msg: str
    escalated: bool


@dataclass(frozen=True)
class AgentResult:
    """Return value of :func:`pipeline.agent.run_once` (F4 design §3).

    ``status`` is the terminal :class:`RunStatus` written to
    ``agent_runs.status``; ``IN_PROGRESS`` never appears here.
    """

    agent_run_id: str
    batches_processed: int
    failures_recovered: int
    escalations: int
    status: RunStatus
