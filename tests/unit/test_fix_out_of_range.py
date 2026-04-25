"""Coverage for the F4 ``out_of_range`` fix (F4.8)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.agent.fixes.out_of_range import (
    OutOfRangeFixError,
    acknowledge_quarantine,
    build_fix,
    quarantine_partition_path,
    quarantine_row_count,
)
from pipeline.silver.quarantine import REJECTED_SCHEMA

# ---------------------------------------------------------------------------
# Fixture helpers — write rejected parquet matching F1's schema.
# ---------------------------------------------------------------------------


def _rejected_row(*, batch_id: str = "bid01") -> dict[str, object]:
    return {
        "message_id": "m1",
        "conversation_id": "c1",
        "timestamp": datetime(2026, 4, 25, 12, 0, 0),
        "direction": "inbound",
        "sender_phone": "+5511999",
        "sender_name": "Lead",
        "message_type": "text",
        "message_body": "valor_pago_atual_brl=-999",
        "status": "delivered",
        "channel": "whatsapp",
        "campaign_id": "camp1",
        "agent_id": "agent1",
        "conversation_outcome": None,
        "metadata": "{}",
        "batch_id": batch_id,
        "ingested_at": datetime(2026, 4, 25, 12, 0, 1, tzinfo=UTC),
        "source_file_hash": "abc",
        "reject_reason": "out_of_range",
        "rejected_at": datetime(2026, 4, 25, 12, 0, 2, tzinfo=UTC),
    }


def _write_rejected_parquet(silver_root: Path, batch_id: str, rows: int = 1) -> Path:
    path = quarantine_partition_path(silver_root, batch_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        [_rejected_row(batch_id=batch_id) for _ in range(rows)],
        schema=REJECTED_SCHEMA,
    )
    df.write_parquet(path)
    return path


# ---------------------------------------------------------------------------
# quarantine_partition_path / quarantine_row_count.
# ---------------------------------------------------------------------------


def test_quarantine_partition_path_uses_canonical_layout(tmp_path: Path) -> None:
    expected = tmp_path / "batch_id=bid01" / "rejected" / "part-0.parquet"
    assert quarantine_partition_path(tmp_path, "bid01") == expected


def test_quarantine_row_count_returns_zero_when_partition_missing(tmp_path: Path) -> None:
    assert quarantine_row_count(tmp_path, "bid01") == 0


def test_quarantine_row_count_returns_actual_count(tmp_path: Path) -> None:
    _write_rejected_parquet(tmp_path, "bid01", rows=3)
    assert quarantine_row_count(tmp_path, "bid01") == 3


# ---------------------------------------------------------------------------
# acknowledge_quarantine.
# ---------------------------------------------------------------------------


def test_acknowledge_quarantine_returns_ack_when_rows_exist(tmp_path: Path) -> None:
    _write_rejected_parquet(tmp_path, "bid01", rows=2)
    ack = acknowledge_quarantine(tmp_path, "bid01")
    assert ack.batch_id == "bid01"
    assert ack.rejected_rows == 2
    assert ack.rejected_path.exists()


def test_acknowledge_quarantine_raises_when_partition_missing(tmp_path: Path) -> None:
    with pytest.raises(OutOfRangeFixError, match="no quarantine evidence"):
        acknowledge_quarantine(tmp_path, "bid01")


def test_acknowledge_quarantine_raises_when_partition_empty(tmp_path: Path) -> None:
    """A zero-row quarantine file should not count as evidence — this
    catches a half-recovered Silver run that opened the file without
    actually routing rows."""
    path = quarantine_partition_path(tmp_path, "bid01")
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(schema=REJECTED_SCHEMA).write_parquet(path)
    with pytest.raises(OutOfRangeFixError, match="no quarantine evidence"):
        acknowledge_quarantine(tmp_path, "bid01")


# ---------------------------------------------------------------------------
# build_fix.
# ---------------------------------------------------------------------------


def test_build_fix_apply_returns_none_when_quarantine_present(tmp_path: Path) -> None:
    _write_rejected_parquet(tmp_path, "bid01")
    fix = build_fix(silver_root=tmp_path, batch_id="bid01")
    assert fix.kind == "acknowledge_quarantine"
    assert fix.requires_llm is False
    assert fix.apply() is None


def test_build_fix_apply_raises_when_quarantine_absent(tmp_path: Path) -> None:
    fix = build_fix(silver_root=tmp_path, batch_id="bid01")
    with pytest.raises(OutOfRangeFixError, match="no quarantine evidence"):
        fix.apply()


def test_build_fix_description_carries_batch_id(tmp_path: Path) -> None:
    fix = build_fix(silver_root=tmp_path, batch_id="bidXYZ")
    assert "bidXYZ" in fix.description
