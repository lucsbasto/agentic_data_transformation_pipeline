"""Coverage for the F4 ``partition_missing`` fix (F4.7).

The repair re-runs the F1 ingest pipeline. Each test creates a tiny
source parquet under ``tmp_path``, runs the fix, and checks the
restored partition is byte-equivalent (rowcount + schema)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pipeline.agent.fixes.partition_missing import (
    PartitionMissingFixError,
    build_fix,
    recreate_partition,
)
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS

# ---------------------------------------------------------------------------
# Fixture helpers — mirror what the real raw parquet looks like.
# ---------------------------------------------------------------------------


def _write_source_parquet(path: Path) -> None:
    """Two-row source parquet with every Bronze source column populated.
    The values respect the closed-set Enum members so the cast inside
    ``transform_to_bronze`` succeeds."""
    rows = {
        "message_id": ["m1", "m2"],
        "conversation_id": ["c1", "c1"],
        "timestamp": ["2026-04-25 12:00:00", "2026-04-25 12:01:00"],
        "direction": ["inbound", "outbound"],
        "sender_phone": ["+551199", "+551188"],
        "sender_name": ["Lead", "Bot"],
        "message_type": ["text", "text"],
        "message_body": ["olá", "obrigado"],
        "status": ["delivered", "sent"],
        "channel": ["whatsapp", "whatsapp"],
        "campaign_id": ["camp1", "camp1"],
        "agent_id": ["agent1", "agent1"],
        "conversation_outcome": [None, None],
        "metadata": ["{}", "{}"],
    }
    df = pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


# ---------------------------------------------------------------------------
# recreate_partition.
# ---------------------------------------------------------------------------


def test_recreate_partition_writes_bronze_when_missing(tmp_path: Path) -> None:
    source = tmp_path / "raw" / "conversations.parquet"
    _write_source_parquet(source)
    bronze_root = tmp_path / "bronze"
    identity = compute_batch_identity(source)
    result = recreate_partition(
        source=source, bronze_root=bronze_root, batch_id=identity.batch_id
    )
    assert result.no_op is False
    assert result.rows_written == 2
    assert result.bronze_path.exists()
    df = pl.read_parquet(result.bronze_path)
    assert df.height == 2
    assert "batch_id" in df.columns


def test_recreate_partition_is_no_op_when_partition_exists(tmp_path: Path) -> None:
    source = tmp_path / "raw" / "conversations.parquet"
    _write_source_parquet(source)
    bronze_root = tmp_path / "bronze"
    identity = compute_batch_identity(source)
    recreate_partition(source=source, bronze_root=bronze_root, batch_id=identity.batch_id)
    bronze_path = bronze_root / f"batch_id={identity.batch_id}" / "part-0.parquet"
    bytes_before = bronze_path.read_bytes()
    second = recreate_partition(
        source=source, bronze_root=bronze_root, batch_id=identity.batch_id
    )
    assert second.no_op is True
    assert second.rows_written == 0
    assert bronze_path.read_bytes() == bytes_before


def test_recreate_partition_refuses_mismatched_source(tmp_path: Path) -> None:
    """Passing a source whose hash produces a different ``batch_id``
    than the one the executor asked for must NOT silently relabel
    rows — refuse."""
    source = tmp_path / "raw" / "conversations.parquet"
    _write_source_parquet(source)
    bronze_root = tmp_path / "bronze"
    with pytest.raises(PartitionMissingFixError, match="refusing to recreate"):
        recreate_partition(
            source=source, bronze_root=bronze_root, batch_id="not-the-real-id"
        )


def test_recreate_partition_uses_canonical_partition_layout(tmp_path: Path) -> None:
    """Hive-style ``batch_id=<id>/part-0.parquet`` layout matches what
    the rest of the pipeline expects."""
    source = tmp_path / "raw" / "conversations.parquet"
    _write_source_parquet(source)
    bronze_root = tmp_path / "bronze"
    identity = compute_batch_identity(source)
    result = recreate_partition(
        source=source, bronze_root=bronze_root, batch_id=identity.batch_id
    )
    assert result.bronze_path.parent.name == f"batch_id={identity.batch_id}"
    assert result.bronze_path.name == "part-0.parquet"


# ---------------------------------------------------------------------------
# build_fix.
# ---------------------------------------------------------------------------


def test_build_fix_apply_returns_none_and_recreates(tmp_path: Path) -> None:
    source = tmp_path / "raw" / "conversations.parquet"
    _write_source_parquet(source)
    bronze_root = tmp_path / "bronze"
    identity = compute_batch_identity(source)
    fix = build_fix(source=source, bronze_root=bronze_root, batch_id=identity.batch_id)
    assert fix.kind == "recreate_partition"
    assert fix.requires_llm is False
    assert fix.apply() is None
    assert (bronze_root / f"batch_id={identity.batch_id}" / "part-0.parquet").exists()


def test_build_fix_description_carries_batch_id_and_source(tmp_path: Path) -> None:
    fix = build_fix(
        source=tmp_path / "raw.parquet",
        bronze_root=tmp_path / "bronze",
        batch_id="bidXYZ",
    )
    assert "bidXYZ" in fix.description
    assert "raw.parquet" in fix.description
