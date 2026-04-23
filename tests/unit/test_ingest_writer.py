"""Tests for ``pipeline.ingest.writer.write_bronze``."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.errors import IngestError, SchemaDriftError
from pipeline.ingest.transform import transform_to_bronze
from pipeline.ingest.writer import write_bronze


def _bronze_df(src: pl.DataFrame) -> pl.DataFrame:
    return transform_to_bronze(
        src.lazy(),
        batch_id="b-write-0001",
        source_hash="deadbeef" * 8,
        ingested_at=datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC),
    ).collect()


def test_write_bronze_creates_partition(
    tiny_source_df: pl.DataFrame, tmp_path: Path
) -> None:
    df = _bronze_df(tiny_source_df)
    result = write_bronze(df, bronze_root=tmp_path, batch_id="b-write-0001")
    assert result.bronze_path == tmp_path / "batch_id=b-write-0001" / "part-0.parquet"
    assert result.bronze_path.is_file()
    assert result.rows_written == df.height


def test_write_bronze_tmp_dir_is_cleaned(
    tiny_source_df: pl.DataFrame, tmp_path: Path
) -> None:
    df = _bronze_df(tiny_source_df)
    write_bronze(df, bronze_root=tmp_path, batch_id="b-clean")
    leftover = tmp_path / ".tmp-batch_id=b-clean"
    assert not leftover.exists()


def test_write_bronze_rewrite_replaces_existing_partition(
    tiny_source_df: pl.DataFrame, tmp_path: Path
) -> None:
    df1 = _bronze_df(tiny_source_df)
    write_bronze(df1, bronze_root=tmp_path, batch_id="b-rewrite")
    half = tiny_source_df.head(4)
    df2 = _bronze_df(half)
    result = write_bronze(df2, bronze_root=tmp_path, batch_id="b-rewrite")
    reloaded = pl.scan_parquet(result.bronze_path).collect()
    assert reloaded.height == half.height


def test_write_bronze_rejects_non_bronze_schema(tmp_path: Path) -> None:
    bad = pl.DataFrame({"only": ["col"]})
    with pytest.raises(SchemaDriftError):
        write_bronze(bad, bronze_root=tmp_path, batch_id="b-bad")


def test_write_bronze_cleans_tmp_on_failure(
    tiny_source_df: pl.DataFrame,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = _bronze_df(tiny_source_df)

    def _boom(*_a: object, **_kw: object) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr("polars.DataFrame.write_parquet", _boom)
    with pytest.raises(IngestError, match="failed to write Bronze partition"):
        write_bronze(df, bronze_root=tmp_path, batch_id="b-fail")
    assert not (tmp_path / ".tmp-batch_id=b-fail").exists()
    assert not (tmp_path / "batch_id=b-fail").exists()


def test_write_bronze_round_trip_preserves_schema(
    tiny_source_df: pl.DataFrame, tmp_path: Path
) -> None:
    df = _bronze_df(tiny_source_df)
    result = write_bronze(df, bronze_root=tmp_path, batch_id="b-rt")
    reloaded = pl.scan_parquet(result.bronze_path).collect()
    # All closed-set columns survive as strings on re-read (Polars loses Enum
    # metadata in the on-disk round trip unless we persist it). Verify the
    # data values match instead of the dtypes.
    assert reloaded.height == df.height
    for col in ("direction", "status", "message_type"):
        assert sorted(reloaded[col].unique().to_list()) == sorted(
            df[col].cast(pl.String).unique().to_list()
        )
