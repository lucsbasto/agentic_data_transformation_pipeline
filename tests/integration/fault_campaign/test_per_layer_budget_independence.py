"""FIC.10 — Per-layer budget independence.

Scenario: the same error_class hits Silver first (runs bronze clean),
then Gold. A "fail-2-then-succeed" runner means each layer consumes
exactly 2 attempts before succeeding. This confirms that Gold's counter
starts at 0 (not inheriting Silver's 2).

Assertions:
- count_agent_attempts(batch_id, SILVER, error_kind) == 2
- count_agent_attempts(batch_id, GOLD, error_kind) == 2
- total agent_failures rows == 4 (2 per layer)
- result.escalations == 0 (both layers recover)
- result.status == COMPLETED
"""

from __future__ import annotations

import polars as pl
import pytest

from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS

from .conftest import (
    PipelineTree,
    assert_agent_failure_count,
    run_agent_once,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ERROR_KIND = ErrorKind.SCHEMA_DRIFT


def _write_source(path, *, marker: str) -> str:
    rows = {col: [marker] * 5 for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"] * 5
    rows["direction"] = ["inbound"] * 5
    rows["message_type"] = ["text"] * 5
    rows["status"] = ["delivered"] * 5
    rows["message_body"] = [f"{marker}_msg_{i}" for i in range(5)]
    if "message_id" in rows:
        rows["message_id"] = [f"{marker}_id_{i}" for i in range(5)]
    pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String)).write_parquet(path)
    return compute_batch_identity(path).batch_id


def _make_fail_n_then_succeed_runner(n: int):
    """Return a runner that raises on the first ``n`` calls, then succeeds."""
    state = {"calls": 0}

    def _runner() -> None:
        state["calls"] += 1
        if state["calls"] <= n:
            raise RuntimeError(f"synthetic failure #{state['calls']} (of {n})")

    return _runner


def _classify_schema_drift(
    _exc: BaseException, _layer: Layer, _batch_id: str
) -> ErrorKind:
    return _ERROR_KIND


def _fix_apply_noop() -> None:
    # Fix doesn't actually heal anything but stops raising so
    # the executor loops and retries the runner.
    pass


def _build_noop_fix(
    _exc: BaseException, _kind: ErrorKind, _layer: Layer, _batch_id: str
) -> Fix | None:
    return Fix(kind="noop_repair", description="no-op fix", apply=_fix_apply_noop)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.fault_campaign
def test_per_layer_budget_independence(pipeline_tree: PipelineTree) -> None:
    """Silver and Gold each fail 2 times; counters are independent."""
    raw = pipeline_tree.root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    batch_id = _write_source(raw / "conversations.parquet", marker="seed")

    bronze_runner = _make_fail_n_then_succeed_runner(0)   # bronze always passes
    silver_runner = _make_fail_n_then_succeed_runner(2)   # silver fails twice
    gold_runner = _make_fail_n_then_succeed_runner(2)     # gold fails twice

    runners_map = {
        Layer.BRONZE: bronze_runner,
        Layer.SILVER: silver_runner,
        Layer.GOLD: gold_runner,
    }

    result = run_agent_once(
        tree=pipeline_tree,
        runners_for=lambda b: runners_map if b == batch_id else {},
        classify=_classify_schema_drift,
        build_fix=_build_noop_fix,
        escalate=lambda *_: None,
        retry_budget=3,
    )

    assert result.status is RunStatus.COMPLETED
    assert result.escalations == 0
    assert result.failures_recovered == 4  # 2 silver + 2 gold

    # Per-layer counts are independent — each starts from 0.
    conn = pipeline_tree.manifest._require_conn()

    silver_count = pipeline_tree.manifest.count_agent_attempts(
        batch_id=batch_id,
        layer=Layer.SILVER.value,
        error_class=_ERROR_KIND.value,
    )
    gold_count = pipeline_tree.manifest.count_agent_attempts(
        batch_id=batch_id,
        layer=Layer.GOLD.value,
        error_class=_ERROR_KIND.value,
    )

    assert silver_count == 2, f"expected 2 silver failures, got {silver_count}"
    assert gold_count == 2, f"expected 2 gold failures, got {gold_count}"

    # Total rows == 4 (2 per layer).
    total = conn.execute(
        "SELECT COUNT(*) FROM agent_failures WHERE batch_id = ?;",
        (batch_id,),
    ).fetchone()[0]
    assert total == 4, f"expected 4 total failure rows, got {total}"

    # Bronze has 0 failures.
    assert_agent_failure_count(
        pipeline_tree.manifest,
        batch_id,
        Layer.BRONZE,
        _ERROR_KIND,
        expected=0,
    )
