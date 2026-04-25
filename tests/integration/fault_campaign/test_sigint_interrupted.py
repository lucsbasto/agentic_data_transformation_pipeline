"""FIC.18 — SIGINT mid-loop marks INTERRUPTED.

Exercises the ``KeyboardInterrupt → INTERRUPTED`` path closed in commit 846cbca
(spec F4-RF-09).  The test does NOT send a real OS signal to avoid killing the
test process.  Instead it injects ``KeyboardInterrupt`` from inside a runner,
which is the equivalent code path that ``run_once`` handles via its
``except KeyboardInterrupt`` clause.

``run_forever`` re-raises ``KeyboardInterrupt``, so the test catches it.

Asserts:
- Latest ``agent_runs`` row has ``status == INTERRUPTED``.
- JSONL log contains a ``loop_stopped`` event with ``status == "INTERRUPTED"``.
- Lock file is absent after the run (released in the ``finally`` block).

Known follow-up: SIGTERM handler integration (F4-RF-09-extend) is NOT tested
here because the signal-handler wiring is not yet shipped.  That gap is
tracked as a follow-up to SEC-M3 / F4-RF-09-extend.
"""

from __future__ import annotations

import json

import pytest

from pipeline.agent._logging import EVENT_LOOP_STOPPED, AgentEventLogger, fixed_clock
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus

from .conftest import (
    PipelineTree,
    _FIXED_TS,
    tail_jsonl,
)


@pytest.mark.fault_campaign
def test_keyboard_interrupt_marks_interrupted(pipeline_tree: PipelineTree) -> None:
    """KeyboardInterrupt raised inside a runner causes run_once to seal the
    agent_runs row as INTERRUPTED and release the lock."""

    import polars as pl
    from pipeline.ingest.batch import compute_batch_identity
    from pipeline.schemas.bronze import SOURCE_COLUMNS

    # Seed a batch so the bronze runner is actually invoked.
    src = pipeline_tree.root / "raw" / "conversations.parquet"
    src.parent.mkdir(parents=True, exist_ok=True)
    rows = {col: ["x"] * 5 for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"] * 5
    rows["direction"] = ["inbound"] * 5
    rows["message_type"] = ["text"] * 5
    rows["status"] = ["delivered"] * 5
    rows["message_body"] = [f"msg_{i}" for i in range(5)]
    pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String)).write_parquet(src)

    lock_path = pipeline_tree.state / "agent.lock"
    lock = AgentLock(lock_path)
    log_path = pipeline_tree.logs / "agent.jsonl"
    logger = AgentEventLogger(log_path=log_path, clock=fixed_clock(_FIXED_TS))

    def interrupt_runner() -> None:
        raise KeyboardInterrupt

    def runners_for(bid: str) -> dict[Layer, object]:
        return {Layer.BRONZE: interrupt_runner}

    def _noop_classify(exc: BaseException, layer: Layer, bid: str) -> ErrorKind:
        return ErrorKind.UNKNOWN

    def _no_fix(exc: BaseException, kind: ErrorKind, layer: Layer, bid: str) -> Fix | None:
        return None

    def _no_escalate(exc: BaseException, kind: ErrorKind, layer: Layer, bid: str) -> None:
        pass

    # run_once re-raises KeyboardInterrupt after sealing the row.
    with pytest.raises(KeyboardInterrupt):
        run_once(
            manifest=pipeline_tree.manifest,
            source_root=pipeline_tree.root / "raw",
            runners_for=runners_for,
            classify=_noop_classify,
            build_fix=_no_fix,
            escalate=_no_escalate,
            lock=lock,
            retry_budget=1,
            event_logger=logger,
        )

    # ------------------------------------------------------------------ assert manifest
    conn = pipeline_tree.manifest._require_conn()
    row = conn.execute(
        "SELECT status FROM agent_runs ORDER BY started_at DESC LIMIT 1;"
    ).fetchone()
    assert row is not None, "no agent_runs row found"
    assert row["status"] == RunStatus.INTERRUPTED.value, (
        f"expected INTERRUPTED, got {row['status']!r}"
    )

    # ------------------------------------------------------------------ assert JSONL event
    events = tail_jsonl(log_path)
    stopped_events = [e for e in events if e.get("event") == EVENT_LOOP_STOPPED]
    assert stopped_events, "no loop_stopped event in JSONL"
    assert stopped_events[-1]["status"] == RunStatus.INTERRUPTED.value, (
        f"loop_stopped status mismatch: {stopped_events[-1]}"
    )

    # ------------------------------------------------------------------ assert lock released
    assert not lock_path.exists(), "lock file not released after INTERRUPTED"
