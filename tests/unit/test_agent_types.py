"""Coverage for the agent type primitives (F4.1).

These dataclasses + enums are dependency-free and intentionally tiny;
the tests pin the value contracts (manifest column strings, frozen
behavior) so a careless rename is caught before observer / planner /
executor modules start consuming them.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from pipeline.agent import (
    AgentResult,
    ErrorKind,
    FailureRecord,
    Fix,
    Layer,
    RunStatus,
)

# ---------------------------------------------------------------------------
# ErrorKind / Layer / RunStatus — string values are the manifest contract.
# ---------------------------------------------------------------------------


def test_error_kind_values_match_manifest_contract() -> None:
    """``error_class`` strings stored in ``agent_failures`` (F4 spec
    §4.2) must match these enum values verbatim — change one, change
    the SQL writer."""
    assert ErrorKind.SCHEMA_DRIFT == "schema_drift"
    assert ErrorKind.REGEX_BREAK == "regex_break"
    assert ErrorKind.PARTITION_MISSING == "partition_missing"
    assert ErrorKind.OUT_OF_RANGE == "out_of_range"
    assert ErrorKind.UNKNOWN == "unknown"


def test_error_kind_is_exhaustive_for_design_taxonomy() -> None:
    """Spec §3 fixes the taxonomy at exactly five classes — any
    addition demands a new ``Fix`` module + a diagnoser branch, so
    we pin the size."""
    assert len(ErrorKind) == 5


def test_layer_values_match_pipeline_directories() -> None:
    """``Layer`` strings double as path segments under ``data/`` and
    column values in ``runs.layer`` — keep them lowercase and stable."""
    assert Layer.BRONZE == "bronze"
    assert Layer.SILVER == "silver"
    assert Layer.GOLD == "gold"
    assert len(Layer) == 3


def test_run_status_values_match_manifest_contract() -> None:
    assert RunStatus.IN_PROGRESS == "IN_PROGRESS"
    assert RunStatus.COMPLETED == "COMPLETED"
    assert RunStatus.INTERRUPTED == "INTERRUPTED"
    assert RunStatus.FAILED == "FAILED"
    assert len(RunStatus) == 4


# ---------------------------------------------------------------------------
# Fix — frozen dataclass with a callable + default flag.
# ---------------------------------------------------------------------------


def test_fix_holds_apply_callable_and_defaults_to_no_llm() -> None:
    calls: list[int] = []

    def _apply() -> None:
        calls.append(1)

    fix = Fix(kind="recreate_partition", description="re-emit Bronze parquet", apply=_apply)
    assert fix.requires_llm is False
    fix.apply()
    assert calls == [1]


def test_fix_is_frozen() -> None:
    fix = Fix(kind="noop", description="noop", apply=lambda: None)
    with pytest.raises(FrozenInstanceError):
        fix.kind = "mutated"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# FailureRecord — frozen dataclass mirroring agent_failures row.
# ---------------------------------------------------------------------------


def test_failure_record_round_trips_all_columns() -> None:
    rec = FailureRecord(
        batch_id="bid01",
        layer=Layer.SILVER,
        error_class=ErrorKind.REGEX_BREAK,
        attempts=2,
        last_fix_kind="regenerate_regex",
        last_error_msg="no match for /R\\$\\d+/",
        escalated=False,
    )
    assert rec.layer is Layer.SILVER
    assert rec.error_class is ErrorKind.REGEX_BREAK
    assert rec.attempts == 2
    assert rec.last_fix_kind == "regenerate_regex"
    assert rec.escalated is False


def test_failure_record_allows_null_last_fix_kind() -> None:
    """First-attempt records have no fix yet; ``last_fix_kind`` is
    optional and matches the SQL ``TEXT NULL`` column."""
    rec = FailureRecord(
        batch_id="bid01",
        layer=Layer.GOLD,
        error_class=ErrorKind.UNKNOWN,
        attempts=1,
        last_fix_kind=None,
        last_error_msg="undiagnosable",
        escalated=True,
    )
    assert rec.last_fix_kind is None


def test_failure_record_is_frozen() -> None:
    rec = FailureRecord(
        batch_id="bid01",
        layer=Layer.BRONZE,
        error_class=ErrorKind.SCHEMA_DRIFT,
        attempts=1,
        last_fix_kind=None,
        last_error_msg="missing col",
        escalated=False,
    )
    with pytest.raises(FrozenInstanceError):
        rec.attempts = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# AgentResult — terminal value of run_once.
# ---------------------------------------------------------------------------


def test_agent_result_carries_terminal_status() -> None:
    res = AgentResult(
        agent_run_id="run-123",
        batches_processed=4,
        failures_recovered=1,
        escalations=0,
        status=RunStatus.COMPLETED,
    )
    assert res.status is RunStatus.COMPLETED
    assert res.batches_processed == 4
    assert res.failures_recovered == 1


def test_agent_result_is_frozen() -> None:
    res = AgentResult(
        agent_run_id="run-1",
        batches_processed=0,
        failures_recovered=0,
        escalations=0,
        status=RunStatus.COMPLETED,
    )
    with pytest.raises(FrozenInstanceError):
        res.escalations = 5  # type: ignore[misc]
