"""Tests for ``pipeline.schemas.silver``.

These tests lock the Silver schema down. Every dtype, every enum, every
lineage column is asserted so any future edit to ``schemas/silver.py``
that breaks the contract fails in CI instead of silently shipping bad
parquet files.
"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from pipeline.schemas.bronze import (
    DIRECTION_VALUES,
    MESSAGE_TYPE_VALUES,
    STATUS_VALUES,
)
from pipeline.schemas.silver import (
    METADATA_FIELDS,
    METADATA_STRUCT,
    SILVER_COLUMNS,
    SILVER_DEDUP_KEY,
    SILVER_SCHEMA,
    STATUS_PRIORITY,
)


def test_silver_columns_match_schema_order() -> None:
    assert tuple(SILVER_SCHEMA.names()) == SILVER_COLUMNS


def test_silver_has_core_grain_keys() -> None:
    for col in ("message_id", "conversation_id"):
        assert col in SILVER_SCHEMA


def test_dedup_key_is_conversation_and_message() -> None:
    assert SILVER_DEDUP_KEY == ("conversation_id", "message_id")
    for col in SILVER_DEDUP_KEY:
        assert col in SILVER_SCHEMA


def test_timestamp_is_utc_aware() -> None:
    dtype = SILVER_SCHEMA["timestamp"]
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_unit == "us"
    assert dtype.time_zone == "UTC"


def test_ingested_at_is_utc_aware() -> None:
    dtype = SILVER_SCHEMA["ingested_at"]
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_zone == "UTC"


def test_transformed_at_is_utc_aware() -> None:
    dtype = SILVER_SCHEMA["transformed_at"]
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_zone == "UTC"


def test_direction_enum_mirrors_bronze() -> None:
    dtype = SILVER_SCHEMA["direction"]
    assert isinstance(dtype, pl.Enum)
    assert tuple(dtype.categories.to_list()) == DIRECTION_VALUES


def test_status_enum_mirrors_bronze() -> None:
    dtype = SILVER_SCHEMA["status"]
    assert isinstance(dtype, pl.Enum)
    assert tuple(dtype.categories.to_list()) == STATUS_VALUES


def test_message_type_enum_mirrors_bronze() -> None:
    dtype = SILVER_SCHEMA["message_type"]
    assert isinstance(dtype, pl.Enum)
    assert tuple(dtype.categories.to_list()) == MESSAGE_TYPE_VALUES


def test_metadata_is_struct_with_declared_fields() -> None:
    dtype = SILVER_SCHEMA["metadata"]
    assert isinstance(dtype, pl.Struct)
    assert dtype == METADATA_STRUCT
    got = {f.name: f.dtype for f in dtype.fields}
    assert got == METADATA_FIELDS


def test_lead_id_phone_and_body_masked_are_strings() -> None:
    for col in ("lead_id", "sender_phone_masked", "message_body_masked"):
        assert SILVER_SCHEMA[col] == pl.String()


def test_has_content_is_boolean() -> None:
    assert SILVER_SCHEMA["has_content"] == pl.Boolean()


def test_status_priority_covers_all_bronze_values() -> None:
    # Every Bronze status has a dedup priority. If Bronze widens
    # STATUS_VALUES without updating STATUS_PRIORITY, this test fails
    # instead of the dedup expression silently collapsing rows with a
    # priority of 0.
    for value in STATUS_VALUES:
        assert value in STATUS_PRIORITY
    # And read beats delivered beats sent, by design.
    assert STATUS_PRIORITY["read"] > STATUS_PRIORITY["delivered"]
    assert STATUS_PRIORITY["delivered"] > STATUS_PRIORITY["sent"]


def test_schema_round_trips_through_dataframe() -> None:
    """A hand-built row must be accepted by ``pl.DataFrame(schema=...)``
    and re-emit the declared schema unchanged.

    This catches the whole class of bugs where a dtype compiles in
    Python but is rejected by Polars at dataframe-construction time
    (for example a struct field that isn't a ``DataType`` instance).
    """
    now = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)
    row = {
        "message_id": "m1",
        "conversation_id": "c1",
        "timestamp": now,
        "sender_phone_masked": "+55 (11) *****-**42",
        "sender_name_normalized": "ana paula",
        "lead_id": "abc123def4567890",
        "message_body_masked": "hi ***.***.***-**",
        "has_content": True,
        "campaign_id": "cmp_fev2026",
        "agent_id": "a1",
        "direction": "inbound",
        "message_type": "text",
        "status": "read",
        "channel": "whatsapp",
        "conversation_outcome": "em_negociacao",
        "metadata": {
            "device": "android",
            "city": "São Paulo",
            "state": "SP",
            "response_time_sec": 12,
            "is_business_hours": True,
            "lead_source": "meta",
        },
        "ingested_at": now,
        "silver_batch_id": "b1",
        "source_file_hash": "deadbeef",
        "transformed_at": now,
    }
    df = pl.DataFrame([row], schema=SILVER_SCHEMA)
    assert df.schema == SILVER_SCHEMA
    assert df.height == 1
