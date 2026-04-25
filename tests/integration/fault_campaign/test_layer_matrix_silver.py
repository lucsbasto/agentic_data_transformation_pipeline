"""FIC.12 — Silver-layer fault matrix.

Three parametrized scenarios at SILVER (bronze passes cleanly):

- regex_break: SilverRegexMissError -> ErrorKind.REGEX_BREAK, layer=silver.
- out_of_range: SilverOutOfRangeError -> ErrorKind.OUT_OF_RANGE, layer=silver.
- schema_drift at silver: ColumnNotFoundError -> ErrorKind.SCHEMA_DRIFT, layer=silver.

Each scenario: fault on first call, fix applied (noop sufficient since second
call succeeds), run ends COMPLETED.

FakeLLMClient is threaded in for completeness but stage-1 handles all three
kinds deterministically so no LLM call fires.
"""

from __future__ import annotations

import polars as pl
import pytest
from polars.exceptions import ColumnNotFoundError

from pipeline.agent.diagnoser import classify as real_classify
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.errors import SilverOutOfRangeError, SilverRegexMissError
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS

from .conftest import (
    FakeLLMClient,
    PipelineTree,
    assert_agent_failure_count,
    run_agent_once,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


# Scenario IDs.
REGEX_BREAK_ID = "regex_break"
OUT_OF_RANGE_ID = "out_of_range"
SCHEMA_DRIFT_SILVER_ID = "schema_drift_at_silver"

_SCENARIOS = [REGEX_BREAK_ID, OUT_OF_RANGE_ID, SCHEMA_DRIFT_SILVER_ID]

_EXPECTED_KIND = {
    REGEX_BREAK_ID: ErrorKind.REGEX_BREAK,
    OUT_OF_RANGE_ID: ErrorKind.OUT_OF_RANGE,
    SCHEMA_DRIFT_SILVER_ID: ErrorKind.SCHEMA_DRIFT,
}


def _exc_for_scenario(scenario: str) -> Exception:
    if scenario == REGEX_BREAK_ID:
        return SilverRegexMissError("regex did not match: ❌ R$ 1.500,00")
    if scenario == OUT_OF_RANGE_ID:
        return SilverOutOfRangeError("valor_pago_atual_brl=-999 is out of range")
    if scenario == SCHEMA_DRIFT_SILVER_ID:
        return ColumnNotFoundError("column 'expected_silver_col' not found in silver input")
    raise ValueError(f"unknown scenario: {scenario}")


# ---------------------------------------------------------------------------
# Parametrized test
# ---------------------------------------------------------------------------


@pytest.mark.fault_campaign
@pytest.mark.parametrize("scenario", _SCENARIOS)
def test_silver_layer_fault_matrix(
    pipeline_tree: PipelineTree,
    fake_llm_client: FakeLLMClient,
    scenario: str,
) -> None:
    """Each silver fault is classified correctly and run ends COMPLETED."""
    raw = pipeline_tree.root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    src = raw / "conversations.parquet"
    batch_id = _write_source(src, marker="silver_matrix")

    expected_kind = _EXPECTED_KIND[scenario]
    exc_to_raise = _exc_for_scenario(scenario)
    silver_call_count = {"n": 0}

    def _bronze_runner() -> None:
        # Bronze always succeeds (no fault injected here).
        pass

    def _silver_runner() -> None:
        silver_call_count["n"] += 1
        if silver_call_count["n"] == 1:
            raise exc_to_raise
        # Second call: success.

    classified_kinds: list[ErrorKind] = []
    classified_layers: list[Layer] = []

    def _tracking_classify(
        exc: BaseException, layer: Layer, batch_id_: str
    ) -> ErrorKind:
        # Use real stage-1 classifier; FakeLLMClient would handle stage-2
        # if needed (it won't be for these deterministic kinds).
        kind = real_classify(
            exc, layer=layer, batch_id=batch_id_, llm_client=fake_llm_client
        )
        classified_kinds.append(kind)
        classified_layers.append(layer)
        return kind

    def _build_noop_fix(
        _exc: BaseException, _kind: ErrorKind, _layer: Layer, _batch_id: str
    ) -> Fix | None:
        return Fix(kind="noop_repair", description="noop", apply=lambda: None)

    result = run_agent_once(
        tree=pipeline_tree,
        runners_for=lambda b: (
            {Layer.BRONZE: _bronze_runner, Layer.SILVER: _silver_runner}
            if b == batch_id
            else {}
        ),
        classify=_tracking_classify,
        build_fix=_build_noop_fix,
        escalate=lambda *_: None,
        retry_budget=3,
    )

    assert result.status is RunStatus.COMPLETED, f"expected COMPLETED, got {result.status}"
    assert result.escalations == 0, f"unexpected escalation in {scenario}"

    # At least one classification fired, all at silver.
    assert len(classified_kinds) >= 1
    assert classified_kinds[0] is expected_kind, (
        f"scenario {scenario}: expected {expected_kind}, got {classified_kinds[0]}"
    )
    assert classified_layers[0] is Layer.SILVER, (
        f"scenario {scenario}: expected layer=silver, got {classified_layers[0]}"
    )

    # Exactly 1 failure row for (batch_id, silver, expected_kind).
    assert_agent_failure_count(
        pipeline_tree.manifest, batch_id, Layer.SILVER, expected_kind, expected=1
    )

    # No LLM call was needed (stage-1 handled it).
    assert len(fake_llm_client.calls) == 0, (
        f"scenario {scenario}: unexpected LLM calls: {fake_llm_client.calls}"
    )
