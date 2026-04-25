"""FIC.19 — State survives restart.

Simulates two separate "agent processes" sharing the same ManifestDB
SQLite file.  After process 1 exhausts its retry budget, process 2
opens a fresh connection and runs the same batch.  We assert that
``count_agent_attempts`` reflects the *accumulated* total from both
processes (MED-2 cumulative semantics observation).
"""

from __future__ import annotations

import pytest

import polars as pl

from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS
from pipeline.state.manifest import ManifestDB

from .conftest import (
    PipelineTree,
    assert_agent_failure_count,
    run_agent_once,
)

_LAYER = Layer.BRONZE
_ERROR_KIND = ErrorKind.SCHEMA_DRIFT


def _write_source(tree: PipelineTree) -> str:
    """Write a minimal source parquet; return batch_id."""
    raw = tree.root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    src = raw / "conversations.parquet"
    rows: dict[str, list[str]] = {col: ["seed"] * 5 for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"] * 5
    rows["direction"] = ["inbound"] * 5
    rows["message_type"] = ["text"] * 5
    rows["status"] = ["delivered"] * 5
    rows["message_body"] = [f"msg_{i}" for i in range(5)]
    if "message_id" in rows:
        rows["message_id"] = [f"seed_msg_{i}" for i in range(5)]
    pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String)).write_parquet(src)
    return compute_batch_identity(src).batch_id


def _always_raise() -> None:
    raise RuntimeError("synthetic schema_drift fault")


def _broken_fix(_exc: BaseException, _kind: ErrorKind, _layer: Layer, _bid: str) -> Fix | None:
    return Fix(
        kind="schema_drift_repair",
        description="broken fix",
        apply=_always_raise,
    )


def _classify_schema_drift(_exc: BaseException, _layer: Layer, _bid: str) -> ErrorKind:
    return _ERROR_KIND


@pytest.mark.fault_campaign
def test_state_survives_restart(pipeline_tree: PipelineTree) -> None:
    """count_agent_attempts accumulates across two separate DB connections.

    MED-2 observation: if the executor's retry budget is evaluated
    per-call (not per-triple cumulative), process 2 will see the budget
    already spent on process 1 only if count_agent_attempts is consulted
    afresh.  This test documents the OBSERVED semantics — it asserts
    the raw row count accumulates (which is all count_agent_attempts
    guarantees), not that process 2 skips retrying.
    """
    batch_id = _write_source(pipeline_tree)
    manifest_path = str(pipeline_tree.state / "manifest.db")

    # ------------------------------------------------------------------ process 1
    result1 = run_agent_once(
        tree=pipeline_tree,
        runners_for=lambda b: {_LAYER: _always_raise} if b == batch_id else {},
        classify=_classify_schema_drift,
        build_fix=_broken_fix,
        retry_budget=2,
    )
    assert result1.status is RunStatus.COMPLETED

    # Record how many failures process 1 wrote.
    n1 = pipeline_tree.manifest.count_agent_attempts(
        batch_id=batch_id,
        layer=_LAYER.value,
        error_class=_ERROR_KIND.value,
    )
    assert n1 == 2, f"process 1 expected 2 failure rows, got {n1}"

    # ------------------------------------------------------------------ process 2
    # Fresh ManifestDB connection on the same file — simulates a restart.
    manifest2 = ManifestDB(manifest_path).open()
    try:
        # Swap the tree's manifest so run_agent_once uses the fresh conn.
        original_manifest = pipeline_tree.manifest
        pipeline_tree.manifest = manifest2

        result2 = run_agent_once(
            tree=pipeline_tree,
            runners_for=lambda b: {_LAYER: _always_raise} if b == batch_id else {},
            classify=_classify_schema_drift,
            build_fix=_broken_fix,
            retry_budget=2,
        )
        assert result2.status is RunStatus.COMPLETED

        n2 = manifest2.count_agent_attempts(
            batch_id=batch_id,
            layer=_LAYER.value,
            error_class=_ERROR_KIND.value,
        )

        # Cumulative: process 2 adds its own failure rows on top of process 1's.
        # The exact total depends on MED-2 semantics: if cumulative, total == n1+2.
        # We assert >= n1 to document that rows are NOT reset across restarts.
        assert n2 >= n1, (
            f"agent_failures must not reset across restarts: "
            f"process 1 wrote {n1} row(s), process 2 sees {n2}"
        )

        # Document observed total (for MED-2 review).
        # With per-call budget (non-cumulative), n2 == n1 + 2.
        # With cumulative budget, process 2 would escalate immediately on attempt 1.
        # Either way, n2 >= n1 must hold.
        assert n2 == n1 + 2, (
            f"MED-2 observed: per-call budget semantics. "
            f"Process 2 added 2 more failures (total {n2}). "
            f"Cumulative semantics (F4-RF-04) would yield immediate escalation."
        )
    finally:
        pipeline_tree.manifest = original_manifest
        manifest2.close()
