"""FIC.8 — Budget exhaustion per fixable ErrorKind.

For each of the 4 fixable kinds, injects the fault, supplies a
fix_builder whose Fix.apply() always raises, runs the agent with
retry_budget=3, and asserts:
- exactly 3 agent_failures rows for (batch_id, layer, error_class)
- exactly 1 escalation event in the JSONL log
- the agent_run terminated with status COMPLETED (loop-level)
- result.escalations == 1
"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from pipeline.agent._logging import EVENT_ESCALATION, AgentEventLogger, fixed_clock
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS

from .conftest import (
    PipelineTree,
    assert_agent_failure_count,
    run_agent_once,
    tail_jsonl,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIXABLE_KINDS = [
    ErrorKind.SCHEMA_DRIFT,
    ErrorKind.REGEX_BREAK,
    ErrorKind.PARTITION_MISSING,
    ErrorKind.OUT_OF_RANGE,
]

_FIXED_TS = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


def _always_raise_fix() -> None:
    raise RuntimeError("fix intentionally broken — budget exhaustion test")


def _broken_fix_builder(
    _exc: BaseException,
    kind: ErrorKind,
    _layer: Layer,
    _batch_id: str,
) -> Fix | None:
    """Return a Fix whose apply() always raises — forces retry without progress."""
    return Fix(
        kind=kind.value + "_repair",
        description="broken fix for budget exhaustion test",
        apply=_always_raise_fix,
    )


def _classify_as(kind: ErrorKind):
    def _classify(
        _exc: BaseException, _layer: Layer, _batch_id: str
    ) -> ErrorKind:
        return kind

    return _classify


# ---------------------------------------------------------------------------
# Parametrized test
# ---------------------------------------------------------------------------


@pytest.mark.fault_campaign
@pytest.mark.parametrize("error_kind", _FIXABLE_KINDS, ids=lambda k: k.value)
def test_budget_exhaustion_per_kind(
    pipeline_tree: PipelineTree,
    error_kind: ErrorKind,
) -> None:
    """Budget=3, fix always fails -> 3 failure rows + 1 escalation."""
    # Create a source parquet so the agent picks up the batch.
    raw = pipeline_tree.root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    src = raw / "conversations.parquet"

    rows = {col: ["seed"] * 5 for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"] * 5
    rows["direction"] = ["inbound"] * 5
    rows["message_type"] = ["text"] * 5
    rows["status"] = ["delivered"] * 5
    rows["message_body"] = [f"mensagem_{i}" for i in range(5)]
    if "message_id" in rows:
        rows["message_id"] = [f"seed_msg_{i}" for i in range(5)]
    pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String)).write_parquet(src)

    batch_id = compute_batch_identity(src).batch_id
    layer = Layer.BRONZE  # test fault at bronze layer

    # Runner always fails with a generic exception the classifier will tag
    # with the parametrized error_kind.
    def _always_fails() -> None:
        raise RuntimeError(f"synthetic {error_kind.value} fault")

    # Build an event logger wired to the same JSONL path the test tails.
    logger = AgentEventLogger(
        log_path=pipeline_tree.logs / "agent.jsonl",
        clock=fixed_clock(_FIXED_TS),
    )

    escalated_calls: list[tuple[ErrorKind, Layer, str]] = []

    def _escalate(exc: BaseException, kind: ErrorKind, lyr: Layer, bid: str) -> None:
        escalated_calls.append((kind, lyr, bid))
        logger.event(
            EVENT_ESCALATION,
            batch_id=bid,
            layer=lyr.value,
            error_class=kind.value,
        )

    result = run_agent_once(
        tree=pipeline_tree,
        runners_for=lambda b: {layer: _always_fails} if b == batch_id else {},
        classify=_classify_as(error_kind),
        build_fix=_broken_fix_builder,
        escalate=_escalate,
        retry_budget=3,
    )

    # Loop-level status: COMPLETED (the loop finished; the batch escalated).
    assert result.status is RunStatus.COMPLETED
    assert result.escalations == 1
    assert result.failures_recovered == 0

    # Exactly 3 failure rows for the triple.
    assert_agent_failure_count(
        pipeline_tree.manifest, batch_id, layer, error_kind, expected=3
    )

    # Escalator invoked exactly once with the right triple.
    assert escalated_calls == [(error_kind, layer, batch_id)]

    # JSONL: exactly 1 escalation event.
    events = tail_jsonl(pipeline_tree.logs / "agent.jsonl")
    escalation_events = [e for e in events if e.get("event") == EVENT_ESCALATION]
    assert len(escalation_events) == 1, (
        f"expected 1 escalation event in JSONL, got {len(escalation_events)}"
    )
