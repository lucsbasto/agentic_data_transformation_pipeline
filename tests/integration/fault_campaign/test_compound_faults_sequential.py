"""FIC.14 — compound faults sequential resolution.

Scenario: two error kinds injected across two layers (bronze = schema_drift,
silver = out_of_range).  The loop must fix bronze first; only after bronze
succeeds does silver run and surface its fault.  Both layers ultimately land
COMPLETED and exactly one ``agent_failures`` row exists per kind.
"""

from __future__ import annotations

import pytest

from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.errors import SchemaDriftError, SilverOutOfRangeError

from .conftest import (
    PipelineTree,
    assert_agent_failure_count,
    assert_run_status,
    run_agent_once,
)


@pytest.mark.fault_campaign
def test_compound_faults_bronze_then_silver(pipeline_tree: PipelineTree) -> None:
    """Bronze schema_drift is resolved on iteration 1; silver out_of_range
    is resolved on iteration 2.  Both layers land COMPLETED with exactly
    one failure row each and no double-counting."""

    # ------------------------------------------------------------------ state
    bronze_fixed = False
    silver_fixed = False

    # Call counters track what each runner observes on each invocation.
    bronze_calls: list[int] = []
    silver_calls: list[int] = []

    # seeded_batch places a raw parquet under tree.root/raw/ and returns
    # (path, batch_id).  We only need the batch_id for manifest queries.
    src = pipeline_tree.root / "raw" / "conversations.parquet"
    src.parent.mkdir(parents=True, exist_ok=True)

    from pipeline.ingest.batch import compute_batch_identity
    from pipeline.schemas.bronze import SOURCE_COLUMNS
    import polars as pl

    rows = {col: ["x"] * 5 for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"] * 5
    rows["direction"] = ["inbound"] * 5
    rows["message_type"] = ["text"] * 5
    rows["status"] = ["delivered"] * 5
    rows["message_body"] = [f"msg_{i}" for i in range(5)]
    pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String)).write_parquet(src)
    batch_id = compute_batch_identity(src).batch_id

    # ------------------------------------------------------------------ runners

    def bronze_runner() -> None:
        bronze_calls.append(1)
        if not bronze_fixed:
            raise SchemaDriftError("injected schema_drift on bronze")

    def silver_runner() -> None:
        silver_calls.append(1)
        if not silver_fixed:
            raise SilverOutOfRangeError("injected out_of_range on silver")

    def runners_for(bid: str) -> dict[Layer, object]:
        return {Layer.BRONZE: bronze_runner, Layer.SILVER: silver_runner}

    # ------------------------------------------------------------------ classify / fix

    def classify(exc: BaseException, layer: Layer, bid: str) -> ErrorKind:
        if isinstance(exc, SchemaDriftError):
            return ErrorKind.SCHEMA_DRIFT
        if isinstance(exc, SilverOutOfRangeError):
            return ErrorKind.OUT_OF_RANGE
        return ErrorKind.UNKNOWN

    def build_fix(exc: BaseException, kind: ErrorKind, layer: Layer, bid: str) -> Fix | None:
        nonlocal bronze_fixed, silver_fixed
        if kind is ErrorKind.SCHEMA_DRIFT:
            def _apply() -> None:
                nonlocal bronze_fixed
                bronze_fixed = True
            return Fix(kind="schema_drift_fix", description="fix bronze schema", apply=_apply)
        if kind is ErrorKind.OUT_OF_RANGE:
            def _apply_oor() -> None:
                nonlocal silver_fixed
                silver_fixed = True
            return Fix(kind="out_of_range_fix", description="fix silver oor", apply=_apply_oor)
        return None

    def escalate(exc: BaseException, kind: ErrorKind, layer: Layer, bid: str) -> None:
        pass  # no escalation in this scenario

    # ------------------------------------------------------------------ run twice

    from pipeline.agent._logging import AgentEventLogger, fixed_clock
    from pipeline.agent.lock import AgentLock
    from pipeline.agent.loop import run_forever
    from datetime import UTC, datetime

    fixed_ts = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)
    logger = AgentEventLogger(
        log_path=pipeline_tree.logs / "agent.jsonl",
        clock=fixed_clock(fixed_ts),
    )

    results = run_forever(
        manifest=pipeline_tree.manifest,
        source_root=pipeline_tree.root / "raw",
        runners_for=runners_for,
        classify=classify,
        build_fix=build_fix,
        escalate=escalate,
        lock=AgentLock(pipeline_tree.state / "agent.lock"),
        retry_budget=3,
        event_logger=logger,
        interval=0.0,
        max_iters=2,
    )

    # ------------------------------------------------------------------ assertions

    # Both iterations completed without hard failure.
    assert len(results) == 2
    for r in results:
        assert r.status is RunStatus.COMPLETED

    # Exactly one failure row per kind — no double-counting.
    assert_agent_failure_count(
        pipeline_tree.manifest, batch_id, Layer.BRONZE, ErrorKind.SCHEMA_DRIFT, 1
    )
    assert_agent_failure_count(
        pipeline_tree.manifest, batch_id, Layer.SILVER, ErrorKind.OUT_OF_RANGE, 1
    )

    # Bronze runner was called before silver runner on iteration 1 (at
    # least 2 bronze calls total: 1 fail + 1 success; silver fails on iter 1
    # only if bronze passes, which happens after the fix).
    assert len(bronze_calls) >= 2, "bronze must have been retried after fix"
    # Silver must also have been called (iter 1 fail, iter 2 success).
    assert len(silver_calls) >= 2, "silver must have been called across iterations"

    # Both layers ultimately succeeded (no escalations across all runs).
    total_escalations = sum(r.escalations for r in results)
    assert total_escalations == 0
