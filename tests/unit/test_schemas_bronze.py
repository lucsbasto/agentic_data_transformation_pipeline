"""Tests for ``pipeline.schemas.bronze``."""

from __future__ import annotations

import polars as pl

from pipeline.schemas.bronze import (
    BRONZE_SCHEMA,
    DIRECTION_VALUES,
    LINEAGE_COLUMNS,
    MESSAGE_TYPE_VALUES,
    SOURCE_COLUMNS,
    STATUS_VALUES,
    TIMESTAMP_SOURCE_FORMAT,
)


def test_schema_contains_all_source_columns() -> None:
    for col in SOURCE_COLUMNS:
        assert col in BRONZE_SCHEMA, f"missing source column: {col}"


def test_schema_contains_lineage_columns() -> None:
    for col in LINEAGE_COLUMNS:
        assert col in BRONZE_SCHEMA, f"missing lineage column: {col}"


def test_source_plus_lineage_matches_total_columns() -> None:
    assert len(BRONZE_SCHEMA) == len(SOURCE_COLUMNS) + len(LINEAGE_COLUMNS)


def test_timestamp_is_naive_datetime() -> None:
    dtype = BRONZE_SCHEMA["timestamp"]
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_unit == "us"
    assert dtype.time_zone is None


def test_ingested_at_is_utc_datetime() -> None:
    dtype = BRONZE_SCHEMA["ingested_at"]
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_unit == "us"
    assert dtype.time_zone == "UTC"


def test_direction_enum_closed() -> None:
    dtype = BRONZE_SCHEMA["direction"]
    assert isinstance(dtype, pl.Enum)
    assert tuple(dtype.categories.to_list()) == DIRECTION_VALUES


def test_status_enum_closed() -> None:
    dtype = BRONZE_SCHEMA["status"]
    assert isinstance(dtype, pl.Enum)
    assert tuple(dtype.categories.to_list()) == STATUS_VALUES


def test_message_type_enum_closed() -> None:
    dtype = BRONZE_SCHEMA["message_type"]
    assert isinstance(dtype, pl.Enum)
    assert tuple(dtype.categories.to_list()) == MESSAGE_TYPE_VALUES


def test_enum_value_sets_are_sorted() -> None:
    # Alphabetical ordering avoids diff churn when the set grows.
    for values in (DIRECTION_VALUES, STATUS_VALUES, MESSAGE_TYPE_VALUES):
        assert list(values) == sorted(values)


def test_timestamp_source_format_is_the_measured_one() -> None:
    assert TIMESTAMP_SOURCE_FORMAT == "%Y-%m-%d %H:%M:%S"


def test_enums_reject_unknown_value() -> None:
    dtype = BRONZE_SCHEMA["direction"]
    assert isinstance(dtype, pl.Enum)
    s = pl.Series(["inbound", "outbound"], dtype=dtype)
    assert s.dtype == dtype
    # Unknown value must raise.
    try:
        pl.Series(["sideways"], dtype=dtype)
    except Exception:  # pragma: no cover - exact exception varies by Polars version
        return
    raise AssertionError("pl.Enum accepted an unknown value")
