"""FIC.3 — schema_drift recovers under budget.

Seed a clean bronze partition, compute SHA256 of the original parquet,
inject schema_drift (adds an extra column), run the agent, then assert:

- ``agent_failures`` row exists for (batch_id, BRONZE, schema_drift).
- ``agent_failures.last_fix_kind == "schema_drift_repair"``.
- BRONZE run = COMPLETED.
- Bronze parquet SHA256 equals pre-injection SHA256 (byte-stable repair).
- jsonl has ``layer_started`` before ``layer_completed`` events (the loop
  logger emits these; failure_detected/fix_applied are structlog-only).
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import polars as pl
import pytest

from pipeline.agent._logging import AgentEventLogger, fixed_clock
from pipeline.agent.diagnoser import classify as real_classify
from pipeline.agent.fixes import schema_drift as sd_fix_mod
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.ingest.batch import compute_batch_identity
from pipeline.ingest.reader import scan_source, validate_source_columns
from pipeline.ingest.transform import collect_bronze, transform_to_bronze
from pipeline.ingest.writer import write_bronze
from pipeline.schemas.bronze import BRONZE_SCHEMA, SOURCE_COLUMNS

from .conftest import PipelineTree, tail_jsonl

_FIXED_TS: Final = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)

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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.fault_campaign
def test_schema_drift_recovers_under_budget(pipeline_tree: PipelineTree) -> None:
    """schema_drift fix rewrites the partition to canonical schema; SHA256 matches."""
    raw = pipeline_tree.root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    src = raw / "conversations.parquet"
    batch_id = _write_source(src, marker="fic3")

    # Pre-ingest: write a canonical bronze partition so the fix has something to repair.
    bronze_part = _bronze_partition_path(pipeline_tree.bronze, batch_id)
    lf = scan_source(src)
    validate_source_columns(lf)
    identity = compute_batch_identity(src)
    typed = transform_to_bronze(
        lf,
        batch_id=batch_id,
        source_hash=identity.source_hash,
        ingested_at=_FIXED_TS,
    )
    df = collect_bronze(typed)
    write_bronze(df, bronze_root=pipeline_tree.bronze, batch_id=batch_id)

    # SHA256 of the original clean partition.
    pre_sha = _sha256(bronze_part)

    # Inject schema drift: add an extra column to the bronze partition.
    from scripts.inject_fault import inject_schema_drift
    inject_schema_drift(bronze_part)
    assert _sha256(bronze_part) != pre_sha, "injection must change the file"

    # Track classifications.
    classified_kinds: list[ErrorKind] = []
    classified_layers: list[Layer] = []
    fault_injected = False

    def _bronze_runner() -> None:
        nonlocal fault_injected
        if not fault_injected:
            fault_injected = True
            _df = pl.read_parquet(bronze_part)
            extra = [c for c in _df.columns if c not in set(BRONZE_SCHEMA.names())]
            if extra:
                from polars.exceptions import SchemaError
                raise SchemaError(f"unexpected columns after schema drift: {extra}")
        # Second call: success.

    def _classify(exc: BaseException, layer: Layer, batch_id_: str) -> ErrorKind:
        kind = real_classify(exc, layer=layer, batch_id=batch_id_, llm_client=None)
        classified_kinds.append(kind)
        classified_layers.append(layer)
        return kind

    def _build_fix(
        exc: BaseException, kind: ErrorKind, layer: Layer, batch_id_: str
    ) -> Fix | None:
        if kind is ErrorKind.SCHEMA_DRIFT:
            return sd_fix_mod.build_fix(bronze_part)
        return None

    result = run_once(
        manifest=pipeline_tree.manifest,
        source_root=raw,
        runners_for=lambda b: {Layer.BRONZE: _bronze_runner} if b == batch_id else {},
        classify=_classify,
        build_fix=_build_fix,
        escalate=lambda *_: None,
        lock=AgentLock(pipeline_tree.state / "agent.lock"),
        retry_budget=3,
        event_logger=AgentEventLogger(
            log_path=pipeline_tree.logs / "agent.jsonl",
            clock=fixed_clock(_FIXED_TS),
        ),
    )

    # 1. Run completed.
    assert result.status is RunStatus.COMPLETED, f"expected COMPLETED, got {result.status}"
    assert result.escalations == 0

    # 2. Classification: at least one schema_drift at bronze.
    assert len(classified_kinds) >= 1
    assert classified_kinds[0] is ErrorKind.SCHEMA_DRIFT
    assert classified_layers[0] is Layer.BRONZE

    # 3. agent_failures row exists for (batch_id, bronze, schema_drift).
    conn = pipeline_tree.manifest._require_conn()
    row = conn.execute(
        "SELECT last_fix_kind FROM agent_failures "
        "WHERE batch_id = ? AND layer = ? AND error_class = ? "
        "ORDER BY attempts DESC LIMIT 1;",
        (batch_id, Layer.BRONZE.value, ErrorKind.SCHEMA_DRIFT.value),
    ).fetchone()
    assert row is not None, "agent_failures row must exist"
    assert row["last_fix_kind"] == "schema_drift_repair", (
        f"expected last_fix_kind='schema_drift_repair', got {row['last_fix_kind']!r}"
    )

    # 4. Bronze parquet SHA256 equals pre-injection SHA256 (byte-stable repair).
    post_sha = _sha256(bronze_part)
    assert post_sha == pre_sha, (
        f"repaired bronze parquet must be byte-stable: pre={pre_sha}, post={post_sha}"
    )

    # 5. jsonl: layer_started appears before layer_completed for bronze.
    #    (The loop logger emits these; failure_detected/fix_applied are structlog-only.)
    events = tail_jsonl(pipeline_tree.logs / "agent.jsonl")
    event_names = [e["event"] for e in events]
    ls_idx = next(
        (i for i, e in enumerate(events)
         if e["event"] == "layer_started" and e.get("layer") == "bronze"),
        None,
    )
    lc_idx = next(
        (i for i, e in enumerate(events)
         if e["event"] == "layer_completed" and e.get("layer") == "bronze"),
        None,
    )
    assert ls_idx is not None, f"expected layer_started(bronze) in jsonl; events={event_names}"
    assert lc_idx is not None, f"expected layer_completed(bronze) in jsonl; events={event_names}"
    assert ls_idx < lc_idx, (
        f"layer_started must precede layer_completed; got indices {ls_idx}, {lc_idx}"
    )
