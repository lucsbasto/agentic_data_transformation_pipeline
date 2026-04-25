"""Coverage for the F4 ``schema_drift`` fix (F4.5).

Each test writes a fixture parquet under ``tmp_path`` whose schema
deliberately disagrees with :data:`pipeline.schemas.bronze.BRONZE_SCHEMA`,
then runs the fix and asserts the rewritten file conforms.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.agent.fixes.schema_drift import (
    SchemaDelta,
    SchemaDriftFixError,
    build_fix,
    detect_delta,
    format_delta_message,
    repair_bronze_partition,
)
from pipeline.schemas.bronze import BRONZE_SCHEMA

# ---------------------------------------------------------------------------
# Fixture helpers.
# ---------------------------------------------------------------------------


def _canonical_row() -> dict[str, object]:
    """One Bronze row that already conforms to ``BRONZE_SCHEMA`` —
    used as the baseline; each test mutates a copy."""
    return {
        "message_id": "m1",
        "conversation_id": "c1",
        "timestamp": datetime(2026, 4, 25, 12, 0, 0),
        "direction": "inbound",
        "sender_phone": "+5511999999999",
        "sender_name": "Lead",
        "message_type": "text",
        "message_body": "olá",
        "status": "delivered",
        "channel": "whatsapp",
        "campaign_id": "camp1",
        "agent_id": "agent1",
        "conversation_outcome": None,
        "metadata": "{}",
        "batch_id": "bid01",
        "ingested_at": datetime(2026, 4, 25, 12, 0, 1, tzinfo=UTC),
        "source_file_hash": "abc",
    }


def _write_parquet(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


# ---------------------------------------------------------------------------
# detect_delta.
# ---------------------------------------------------------------------------


def test_detect_delta_returns_empty_for_canonical_schema() -> None:
    df = pl.DataFrame([_canonical_row()], schema=BRONZE_SCHEMA)
    delta = detect_delta(df.schema)
    assert delta.is_empty


def test_detect_delta_lists_extra_columns() -> None:
    base = _canonical_row()
    base["injected_col"] = 42
    df = pl.DataFrame([base])
    delta = detect_delta(df.schema)
    assert delta.extra_cols == ("injected_col",)
    assert delta.missing_cols == ()


def test_detect_delta_lists_missing_columns() -> None:
    base = _canonical_row()
    del base["channel"]
    df = pl.DataFrame([base])
    delta = detect_delta(df.schema)
    assert delta.missing_cols == ("channel",)


def test_detect_delta_lists_type_mismatches() -> None:
    """``message_id`` declared as Int64 instead of String — a drift
    we'd see if the source vendor changed the column shape."""
    bad = pl.DataFrame({"message_id": [1, 2, 3]}, schema={"message_id": pl.Int64})
    delta = detect_delta(bad.schema)
    mismatches = {name for name, _, _ in delta.type_mismatches}
    assert "message_id" in mismatches


# ---------------------------------------------------------------------------
# format_delta_message.
# ---------------------------------------------------------------------------


def test_format_delta_message_lists_each_kind() -> None:
    delta = SchemaDelta(
        extra_cols=("a", "b"),
        missing_cols=("c",),
        type_mismatches=(("d", "Int64", "String"),),
    )
    msg = format_delta_message(delta)
    assert "extra=a,b" in msg
    assert "missing=c" in msg
    assert "type_mismatch=d(Int64->String)" in msg


def test_format_delta_message_caps_at_512_chars() -> None:
    """Many extras → message must still fit
    ``agent_failures.last_error_msg`` (spec §4.2)."""
    extras = tuple(f"col{i}" for i in range(500))
    msg = format_delta_message(SchemaDelta(extra_cols=extras))
    assert len(msg) == 512


def test_format_delta_message_handles_empty_delta() -> None:
    msg = format_delta_message(SchemaDelta())
    assert "no delta" in msg


# ---------------------------------------------------------------------------
# repair_bronze_partition.
# ---------------------------------------------------------------------------


def test_repair_drops_extra_columns(tmp_path: Path) -> None:
    base = _canonical_row()
    base["injected_col"] = "garbage"
    parquet_path = tmp_path / "bronze" / "batch_id=bid01" / "part-0.parquet"
    _write_parquet(parquet_path, pl.DataFrame([base]))
    delta = repair_bronze_partition(parquet_path)
    assert delta.extra_cols == ("injected_col",)
    repaired = pl.read_parquet(parquet_path)
    assert "injected_col" not in repaired.columns
    assert repaired.schema == BRONZE_SCHEMA


def test_repair_fills_missing_columns_with_typed_nulls(tmp_path: Path) -> None:
    base = _canonical_row()
    del base["channel"]
    del base["campaign_id"]
    parquet_path = tmp_path / "bronze" / "batch_id=bid01" / "part-0.parquet"
    _write_parquet(parquet_path, pl.DataFrame([base]))
    delta = repair_bronze_partition(parquet_path)
    assert set(delta.missing_cols) == {"channel", "campaign_id"}
    repaired = pl.read_parquet(parquet_path)
    assert repaired.schema == BRONZE_SCHEMA
    assert repaired["channel"][0] is None
    assert repaired["campaign_id"][0] is None


def test_repair_reorders_columns_to_canonical_order(tmp_path: Path) -> None:
    """Polars writes whatever order the frame carries — reordering is
    what makes a second repair byte-stable."""
    base = _canonical_row()
    df = pl.DataFrame([base]).select(sorted(base.keys()))  # alphabetical, not canonical
    parquet_path = tmp_path / "bronze" / "batch_id=bid01" / "part-0.parquet"
    _write_parquet(parquet_path, df)
    repair_bronze_partition(parquet_path)
    repaired = pl.read_parquet(parquet_path)
    assert tuple(repaired.columns) == tuple(BRONZE_SCHEMA.names())


def test_repair_is_idempotent(tmp_path: Path) -> None:
    """Two repairs must produce byte-identical output — invariant
    that lets the executor retry safely after a partial recovery."""
    base = _canonical_row()
    base["injected_col"] = "garbage"
    parquet_path = tmp_path / "bronze" / "batch_id=bid01" / "part-0.parquet"
    _write_parquet(parquet_path, pl.DataFrame([base]))
    repair_bronze_partition(parquet_path)
    first_bytes = parquet_path.read_bytes()
    delta_two = repair_bronze_partition(parquet_path)
    assert delta_two.is_empty
    assert parquet_path.read_bytes() == first_bytes


def test_repair_writes_atomically_via_tmp_rename(tmp_path: Path) -> None:
    """No ``.tmp`` sidecar must be left behind after a successful
    repair (the executor relies on the partition dir staying clean)."""
    parquet_path = tmp_path / "bronze" / "batch_id=bid01" / "part-0.parquet"
    _write_parquet(parquet_path, pl.DataFrame([_canonical_row()], schema=BRONZE_SCHEMA))
    repair_bronze_partition(parquet_path)
    leftover = list(parquet_path.parent.glob("*.tmp"))
    assert leftover == []


def test_repair_raises_when_partition_missing(tmp_path: Path) -> None:
    missing = tmp_path / "bronze" / "batch_id=ghost" / "part-0.parquet"
    with pytest.raises(SchemaDriftFixError, match="bronze partition missing"):
        repair_bronze_partition(missing)


# ---------------------------------------------------------------------------
# build_fix.
# ---------------------------------------------------------------------------


def test_build_fix_returns_named_apply_callable(tmp_path: Path) -> None:
    parquet_path = tmp_path / "bronze" / "batch_id=bid01" / "part-0.parquet"
    base = _canonical_row()
    base["injected_col"] = "x"
    _write_parquet(parquet_path, pl.DataFrame([base]))
    fix = build_fix(parquet_path)
    assert fix.kind == "schema_drift_repair"
    assert fix.requires_llm is False
    fix.apply()
    repaired = pl.read_parquet(parquet_path)
    assert "injected_col" not in repaired.columns


def test_build_fix_apply_returns_none(tmp_path: Path) -> None:
    """``Fix.apply`` is typed as ``Callable[[], None]`` — the wrapper
    must not leak the internal ``SchemaDelta`` return value."""
    parquet_path = tmp_path / "bronze" / "batch_id=bid01" / "part-0.parquet"
    _write_parquet(parquet_path, pl.DataFrame([_canonical_row()], schema=BRONZE_SCHEMA))
    fix = build_fix(parquet_path)
    assert fix.apply() is None
