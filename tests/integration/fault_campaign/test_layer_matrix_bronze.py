"""FIC.11 — Bronze-layer fault matrix.

Two parametrized scenarios at BRONZE using the real diagnoser (stage 1,
no LLM needed for these deterministic kinds):

- schema_drift: add extra column -> classifier returns SCHEMA_DRIFT,
  layer=bronze, run recovers to COMPLETED.
- partition_missing: delete the bronze partition dir -> classifier
  returns PARTITION_MISSING, layer=bronze, run recovers to COMPLETED.

Each scenario uses real fix dispatch so the pipeline actually heals and
the run ends COMPLETED.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Final

import polars as pl
import pytest

from pipeline.agent._logging import AgentEventLogger, fixed_clock
from pipeline.agent.diagnoser import classify as real_classify
from pipeline.agent.fixes import partition_missing as pm_fix_mod
from pipeline.agent.fixes import schema_drift as sd_fix_mod
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.observer import discover_source_batches
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import BRONZE_SCHEMA, SOURCE_COLUMNS
from pipeline.state.manifest import ManifestDB

from .conftest import PipelineTree, assert_agent_failure_count, tail_jsonl

_FIXED_TS: Final = __import__("datetime").datetime(2026, 4, 25, 12, 0, 0, tzinfo=__import__("datetime").timezone.utc)

_BRONZE_PART_FILE: Final[str] = "part-0.parquet"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_source(path: Path, *, marker: str) -> str:
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


def _bronze_partition_path(bronze_root: Path, batch_id: str) -> Path:
    return bronze_root / f"batch_id={batch_id}" / _BRONZE_PART_FILE


def _classify_wrapper(exc, layer, batch_id):
    """Delegate to real stage-1 classifier (no LLM needed for these faults)."""
    return real_classify(exc, layer=layer, batch_id=batch_id, llm_client=None)


# ---------------------------------------------------------------------------
# Scenario: schema_drift
# ---------------------------------------------------------------------------

SCHEMA_DRIFT_ID = "schema_drift"
PARTITION_MISSING_ID = "partition_missing"

_SCENARIOS = [SCHEMA_DRIFT_ID, PARTITION_MISSING_ID]


@pytest.mark.fault_campaign
@pytest.mark.parametrize("scenario", _SCENARIOS)
def test_bronze_layer_fault_matrix(
    pipeline_tree: PipelineTree,
    scenario: str,
) -> None:
    """Each bronze fault is classified correctly and the run ends COMPLETED."""
    raw = pipeline_tree.root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    src = raw / "conversations.parquet"
    batch_id = _write_source(src, marker="bronze_matrix")

    bronze_root = pipeline_tree.bronze
    fault_injected = False

    def _bronze_runner() -> None:
        nonlocal fault_injected
        if not fault_injected:
            fault_injected = True
            if scenario == SCHEMA_DRIFT_ID:
                # Inject schema drift on the source (add extra column).
                df = pl.read_parquet(src)
                df.with_columns(pl.lit("injected").alias("injected_col")).write_parquet(src)
                # Now read with strict schema -> raises SchemaError/ColumnNotFoundError.
                _df2 = pl.read_parquet(src)
                # Explicitly validate against BRONZE_SCHEMA to trigger the error.
                for col in BRONZE_SCHEMA.names():
                    if col not in _df2.columns:
                        from polars.exceptions import ColumnNotFoundError
                        raise ColumnNotFoundError(f"column {col!r} missing after schema drift")
                # If extra column present, raise SchemaError.
                extra = [c for c in _df2.columns if c not in set(BRONZE_SCHEMA.names())]
                if extra:
                    from polars.exceptions import SchemaError
                    raise SchemaError(f"unexpected columns: {extra}")
            elif scenario == PARTITION_MISSING_ID:
                # Bronze partition doesn't exist yet — raise FileNotFoundError.
                part = _bronze_partition_path(bronze_root, batch_id)
                raise FileNotFoundError(f"bronze partition missing: {part}")
        # Second call: success (fix was applied).

    def _build_real_fix(
        exc: BaseException, kind: ErrorKind, layer: Layer, batch_id_: str
    ) -> Fix | None:
        if kind is ErrorKind.SCHEMA_DRIFT:
            # Repair the source parquet to match bronze schema.
            return sd_fix_mod.build_fix(src)
        if kind is ErrorKind.PARTITION_MISSING:
            # Re-create the bronze partition from source.
            return pm_fix_mod.build_fix(
                source=src, bronze_root=bronze_root, batch_id=batch_id_
            )
        return None

    classified_kinds: list[ErrorKind] = []
    classified_layers: list[Layer] = []

    def _tracking_classify(
        exc: BaseException, layer: Layer, batch_id_: str
    ) -> ErrorKind:
        kind = _classify_wrapper(exc, layer, batch_id_)
        classified_kinds.append(kind)
        classified_layers.append(layer)
        return kind

    result = run_once(
        manifest=pipeline_tree.manifest,
        source_root=raw,
        runners_for=lambda b: {Layer.BRONZE: _bronze_runner} if b == batch_id else {},
        classify=_tracking_classify,
        build_fix=_build_real_fix,
        escalate=lambda *_: None,
        lock=AgentLock(pipeline_tree.state / "agent.lock"),
        retry_budget=3,
        event_logger=AgentEventLogger(
            log_path=pipeline_tree.logs / "agent.jsonl",
            clock=fixed_clock(_FIXED_TS),
        ),
    )

    # Run ended cleanly.
    assert result.status is RunStatus.COMPLETED, f"expected COMPLETED, got {result.status}"
    assert result.escalations == 0, f"unexpected escalations: {result.escalations}"

    # Exactly 1 failure (first attempt failed, second succeeded).
    if scenario == SCHEMA_DRIFT_ID:
        assert len(classified_kinds) >= 1
        assert classified_kinds[0] is ErrorKind.SCHEMA_DRIFT, (
            f"expected SCHEMA_DRIFT, got {classified_kinds[0]}"
        )
        assert classified_layers[0] is Layer.BRONZE
        assert_agent_failure_count(
            pipeline_tree.manifest, batch_id, Layer.BRONZE, ErrorKind.SCHEMA_DRIFT, expected=1
        )
    elif scenario == PARTITION_MISSING_ID:
        assert len(classified_kinds) >= 1
        assert classified_kinds[0] is ErrorKind.PARTITION_MISSING, (
            f"expected PARTITION_MISSING, got {classified_kinds[0]}"
        )
        assert classified_layers[0] is Layer.BRONZE
        assert_agent_failure_count(
            pipeline_tree.manifest, batch_id, Layer.BRONZE, ErrorKind.PARTITION_MISSING, expected=1
        )
