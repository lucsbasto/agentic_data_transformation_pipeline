"""FIC.15 — cascading kind during fix.

Scenario: a single layer surfaces kind A (SCHEMA_DRIFT) on call 1; after the
fix is applied, call 2 surfaces kind B (OUT_OF_RANGE); call 3 succeeds.

Asserts:
- Two distinct ``agent_failures`` rows (one per kind).
- Each kind's counter is exactly 1 (no bleeding).
- Final run status is COMPLETED.
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
def test_cascading_kind_schema_then_out_of_range(pipeline_tree: PipelineTree) -> None:
    """Bronze runner raises SCHEMA_DRIFT on call 1, OUT_OF_RANGE on call 2,
    succeeds on call 3.  Each kind has exactly one failure row; run is COMPLETED."""

    # ------------------------------------------------------------------ setup source
    import polars as pl
    from pipeline.ingest.batch import compute_batch_identity
    from pipeline.schemas.bronze import SOURCE_COLUMNS

    src = pipeline_tree.root / "raw" / "conversations.parquet"
    src.parent.mkdir(parents=True, exist_ok=True)
    rows = {col: ["x"] * 5 for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"] * 5
    rows["direction"] = ["inbound"] * 5
    rows["message_type"] = ["text"] * 5
    rows["status"] = ["delivered"] * 5
    rows["message_body"] = [f"msg_{i}" for i in range(5)]
    pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String)).write_parquet(src)
    batch_id = compute_batch_identity(src).batch_id

    # ------------------------------------------------------------------ runner state
    call_count = 0

    def bronze_runner() -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise SchemaDriftError("call 1: schema_drift")
        if call_count == 2:
            raise SilverOutOfRangeError("call 2: out_of_range")
        # call 3+: succeed

    def runners_for(bid: str) -> dict[Layer, object]:
        return {Layer.BRONZE: bronze_runner}

    # ------------------------------------------------------------------ classify / fix

    def classify(exc: BaseException, layer: Layer, bid: str) -> ErrorKind:
        if isinstance(exc, SchemaDriftError):
            return ErrorKind.SCHEMA_DRIFT
        if isinstance(exc, SilverOutOfRangeError):
            return ErrorKind.OUT_OF_RANGE
        return ErrorKind.UNKNOWN

    def build_fix(exc: BaseException, kind: ErrorKind, layer: Layer, bid: str) -> Fix | None:
        if kind is ErrorKind.SCHEMA_DRIFT:
            return Fix(kind="schema_drift_fix", description="no-op fix", apply=lambda: None)
        if kind is ErrorKind.OUT_OF_RANGE:
            return Fix(kind="out_of_range_fix", description="no-op fix", apply=lambda: None)
        return None

    def escalate(exc: BaseException, kind: ErrorKind, layer: Layer, bid: str) -> None:
        raise AssertionError(f"unexpected escalation: {kind} on {layer}")

    # ------------------------------------------------------------------ run
    result = run_agent_once(
        tree=pipeline_tree,
        runners_for=runners_for,
        classify=classify,
        build_fix=build_fix,
        escalate=escalate,
        retry_budget=5,
    )

    # ------------------------------------------------------------------ assertions
    assert result.status is RunStatus.COMPLETED
    assert_run_status(pipeline_tree.manifest, result.agent_run_id, RunStatus.COMPLETED)

    # Exactly one failure row per kind — no cross-contamination.
    assert_agent_failure_count(
        pipeline_tree.manifest, batch_id, Layer.BRONZE, ErrorKind.SCHEMA_DRIFT, 1
    )
    assert_agent_failure_count(
        pipeline_tree.manifest, batch_id, Layer.BRONZE, ErrorKind.OUT_OF_RANGE, 1
    )

    # Runner was called exactly 3 times.
    assert call_count == 3

    # Two failures recovered (one per kind fix applied).
    assert result.failures_recovered == 2
