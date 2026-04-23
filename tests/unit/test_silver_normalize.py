"""Tests for ``pipeline.silver.normalize``."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from pipeline.schemas.silver import METADATA_STRUCT
from pipeline.silver.normalize import (
    normalize_name,
    normalize_name_expr,
    parse_metadata_expr,
    parse_timestamp_utc,
)

# ---------------------------------------------------------------------------
# parse_timestamp_utc
# ---------------------------------------------------------------------------


def test_parse_timestamp_utc_tags_not_converts() -> None:
    """Naive wall-clock must end up with UTC declared and the SAME
    instant. Tagging, not converting.
    """
    df = pl.DataFrame(
        {"ts": [datetime(2026, 4, 23, 12, 0, 0), datetime(2026, 4, 23, 13, 30, 0)]}
    ).with_columns(pl.col("ts").cast(pl.Datetime("us")))
    out = df.select(parse_timestamp_utc(pl.col("ts")).alias("ts"))
    dtype = out["ts"].dtype
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_zone == "UTC"
    # Instant unchanged: 2026-04-23 12:00:00 -> 2026-04-23 12:00:00+00:00
    assert out["ts"][0] == datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# parse_metadata_expr
# ---------------------------------------------------------------------------


def test_parse_metadata_decodes_valid_json() -> None:
    payload = (
        '{"device":"android","city":"Sao Paulo","state":"SP",'
        '"response_time_sec":12,"is_business_hours":true,"lead_source":"meta"}'
    )
    df = pl.DataFrame({"m": [payload]})
    out = df.select(parse_metadata_expr(pl.col("m")).alias("m"))
    assert out.schema["m"] == METADATA_STRUCT
    row = out["m"][0]
    assert row["device"] == "android"
    assert row["city"] == "Sao Paulo"
    assert row["response_time_sec"] == 12
    assert row["is_business_hours"] is True
    assert row["lead_source"] == "meta"


def test_parse_metadata_null_on_invalid_json() -> None:
    df = pl.DataFrame({"m": ["not json at all"]})
    out = df.select(parse_metadata_expr(pl.col("m")).alias("m"))
    assert out.schema["m"] == METADATA_STRUCT
    # Invalid JSON -> null struct. struct.field(...) on null returns null.
    assert out["m"][0] is None


def test_parse_metadata_null_on_non_object_json() -> None:
    df = pl.DataFrame({"m": ["42"]})  # valid JSON but not an object
    out = df.select(parse_metadata_expr(pl.col("m")).alias("m"))
    assert out["m"][0] is None


def test_parse_metadata_null_on_empty_string() -> None:
    df = pl.DataFrame({"m": ["", "   "]})
    out = df.select(parse_metadata_expr(pl.col("m")).alias("m"))
    assert out["m"].to_list() == [None, None]


def test_parse_metadata_missing_fields_become_null() -> None:
    """A partial JSON blob still decodes — the struct has nulls where
    the blob is missing a declared field.
    """
    df = pl.DataFrame({"m": ['{"device":"ios"}']})
    out = df.select(parse_metadata_expr(pl.col("m")).alias("m"))
    row = out["m"][0]
    assert row["device"] == "ios"
    assert row["city"] is None
    assert row["response_time_sec"] is None


# ---------------------------------------------------------------------------
# normalize_name
# ---------------------------------------------------------------------------


def test_normalize_name_casefolds_and_strips_accents() -> None:
    assert normalize_name("Ana Paula Ribeiro") == "ana paula ribeiro"
    assert normalize_name("ANA PAULA") == "ana paula"
    assert normalize_name("João") == "joao"
    assert normalize_name("MARIA FERNANDÊZ") == "maria fernandez"


def test_normalize_name_strips_outer_whitespace_only() -> None:
    # Outer whitespace goes, inner whitespace stays so "Ana P" and
    # "Ana Paula" remain distinguishable for reconciliation.
    assert normalize_name("   Ana  Paula   ") == "ana  paula"


def test_normalize_name_handles_nullish_inputs() -> None:
    assert normalize_name(None) is None
    assert normalize_name("") is None
    assert normalize_name("   ") is None


def test_normalize_name_casefold_beats_lower_on_eszett() -> None:
    # Typical lower() keeps "ß" as "ß"; casefold maps it to "ss".
    # Matters for German-style names spelled differently in different
    # systems.
    assert normalize_name("Straße") == "strasse"


def test_normalize_name_expr_over_column() -> None:
    df = pl.DataFrame({"name": ["Ana", "JOSÉ", None, "   ", "  Carlos Dürer  "]})
    out = df.select(normalize_name_expr(pl.col("name")).alias("norm"))
    assert out["norm"].to_list() == ["ana", "jose", None, None, "carlos durer"]
