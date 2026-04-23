"""Tests for ``pipeline.ingest.transform``."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from pipeline.errors import SchemaDriftError
from pipeline.ingest.transform import (
    assert_bronze_schema,
    collect_bronze,
    transform_to_bronze,
)
from pipeline.schemas.bronze import BRONZE_SCHEMA


def _ingested_at() -> datetime:
    return datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC)


def test_transform_matches_bronze_schema(tiny_source_df: pl.DataFrame) -> None:
    lf = tiny_source_df.lazy()
    out = transform_to_bronze(
        lf,
        batch_id="b-test-0001",
        source_hash="deadbeef" * 8,
        ingested_at=_ingested_at(),
    )
    df = out.collect()
    assert df.schema == BRONZE_SCHEMA
    assert df.height == tiny_source_df.height


def test_transform_appends_lineage_columns(tiny_source_df: pl.DataFrame) -> None:
    df = transform_to_bronze(
        tiny_source_df.lazy(),
        batch_id="b-lineage",
        source_hash="a" * 64,
        ingested_at=_ingested_at(),
    ).collect()
    assert df["batch_id"].unique().to_list() == ["b-lineage"]
    assert df["source_file_hash"].unique().to_list() == ["a" * 64]
    assert df["ingested_at"].dtype.time_zone == "UTC"  # type: ignore[attr-defined]


def test_transform_parses_timestamp(tiny_source_df: pl.DataFrame) -> None:
    df = transform_to_bronze(
        tiny_source_df.lazy(),
        batch_id="b",
        source_hash="x" * 64,
        ingested_at=_ingested_at(),
    ).collect()
    ts_dtype = df.schema["timestamp"]
    assert isinstance(ts_dtype, pl.Datetime)
    assert ts_dtype.time_unit == "us"
    assert ts_dtype.time_zone is None


def test_transform_rejects_unknown_enum_value(
    tiny_source_df: pl.DataFrame,
) -> None:
    poisoned = tiny_source_df.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("sideways"))
        .otherwise(pl.col("direction"))
        .alias("direction")
    )
    lf = transform_to_bronze(
        poisoned.lazy(),
        batch_id="b",
        source_hash="x" * 64,
        ingested_at=_ingested_at(),
    )
    with pytest.raises(SchemaDriftError, match="violates Bronze contract"):
        collect_bronze(lf)


def test_collect_bronze_returns_dataframe_for_valid_input(
    tiny_source_df: pl.DataFrame,
) -> None:
    lf = transform_to_bronze(
        tiny_source_df.lazy(),
        batch_id="b",
        source_hash="x" * 64,
        ingested_at=_ingested_at(),
    )
    df = collect_bronze(lf)
    assert df.height == tiny_source_df.height
    assert df.schema == BRONZE_SCHEMA


def test_assert_bronze_schema_detects_extra_column(
    tiny_source_df: pl.DataFrame,
) -> None:
    df = transform_to_bronze(
        tiny_source_df.lazy(),
        batch_id="b",
        source_hash="x" * 64,
        ingested_at=_ingested_at(),
    ).collect()
    bad = df.with_columns(pl.lit("extra").alias("bonus"))
    with pytest.raises(SchemaDriftError, match="unexpected column: bonus"):
        assert_bronze_schema(bad)
