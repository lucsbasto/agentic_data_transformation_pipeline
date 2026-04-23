"""Tests for ``pipeline.ingest.reader``."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pipeline.errors import IngestError, SchemaDriftError
from pipeline.ingest.reader import scan_source, validate_source_columns


def test_scan_source_returns_lazyframe(tiny_source_parquet: Path) -> None:
    lf = scan_source(tiny_source_parquet)
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect_schema().names()


def test_scan_source_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(IngestError, match="not found"):
        scan_source(tmp_path / "does-not-exist.parquet")


def test_validate_source_columns_passes_for_tiny_fixture(
    tiny_source_parquet: Path,
) -> None:
    lf = scan_source(tiny_source_parquet)
    validate_source_columns(lf)  # must not raise


def test_validate_source_columns_detects_missing_column(
    tmp_path: Path,
) -> None:
    # Build a parquet with one source column dropped.
    df = pl.DataFrame(
        {
            "message_id": ["m1"],
            # missing: conversation_id, timestamp, ... every other col.
        }
    )
    path = tmp_path / "partial.parquet"
    df.write_parquet(path)
    lf = scan_source(path)
    with pytest.raises(SchemaDriftError, match="missing required columns"):
        validate_source_columns(lf)
