"""Tests for ``pipeline.silver.transform``."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from pipeline.errors import SchemaDriftError
from pipeline.schemas.bronze import BRONZE_SCHEMA
from pipeline.schemas.silver import SILVER_COLUMNS, SILVER_SCHEMA
from pipeline.silver.transform import (
    assert_silver_schema,
    silver_transform,
)

SECRET = "unit-test-lead-secret-0123456789abcdef"
SILVER_BATCH_ID = "b-unit-test"
NOW = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Bronze fixture builders.
# ---------------------------------------------------------------------------


def _bronze_row(
    *,
    message_id: str = "m1",
    conversation_id: str = "c1",
    timestamp: datetime | None = None,
    direction: str = "inbound",
    sender_phone: str | None = "+5511987654321",
    sender_name: str | None = "Ana Paula",
    message_type: str = "text",
    message_body: str | None = "olá! meu cpf é 123.456.789-01",
    status: str = "sent",
    channel: str = "whatsapp",
    campaign_id: str = "camp1",
    agent_id: str = "a1",
    conversation_outcome: str | None = "venda_fechada",
    metadata: str = '{"device":"android"}',
    batch_id: str = "b-unit-test",
    ingested_at: datetime | None = None,
    source_file_hash: str = "deadbeef",
) -> dict[str, object]:
    """Synthesize one typed Bronze row as a plain ``dict``."""
    return {
        "message_id": message_id,
        "conversation_id": conversation_id,
        "timestamp": timestamp or datetime(2026, 4, 23, 12, 0, 0),
        "direction": direction,
        "sender_phone": sender_phone,
        "sender_name": sender_name,
        "message_type": message_type,
        "message_body": message_body,
        "status": status,
        "channel": channel,
        "campaign_id": campaign_id,
        "agent_id": agent_id,
        "conversation_outcome": conversation_outcome,
        "metadata": metadata,
        "batch_id": batch_id,
        "ingested_at": ingested_at or datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC),
        "source_file_hash": source_file_hash,
    }


def _bronze_frame(rows: list[dict[str, object]]) -> pl.LazyFrame:
    """Materialize rows against the real :data:`BRONZE_SCHEMA`."""
    return pl.DataFrame(rows, schema=BRONZE_SCHEMA).lazy()


def _run_silver(lf: pl.LazyFrame) -> pl.DataFrame:
    return silver_transform(
        lf,
        secret=SECRET,
        silver_batch_id=SILVER_BATCH_ID,
        transformed_at=NOW,
    ).collect()


# ---------------------------------------------------------------------------
# Schema round-trip.
# ---------------------------------------------------------------------------


def test_silver_transform_matches_declared_schema() -> None:
    """Every row must round-trip cleanly into ``SILVER_SCHEMA``."""
    lf = _bronze_frame([_bronze_row()])
    out = _run_silver(lf)
    assert out.schema == SILVER_SCHEMA
    assert tuple(out.columns) == SILVER_COLUMNS


def test_silver_transform_preserves_row_count_when_no_dupes() -> None:
    """Unique ``(conversation_id, message_id)`` pairs are not collapsed."""
    lf = _bronze_frame(
        [
            _bronze_row(message_id="m1"),
            _bronze_row(message_id="m2"),
            _bronze_row(message_id="m3", conversation_id="c2"),
        ]
    )
    out = _run_silver(lf)
    assert out.height == 3


# ---------------------------------------------------------------------------
# Dedup wiring.
# ---------------------------------------------------------------------------


def test_silver_transform_collapses_status_lifecycle() -> None:
    """sent / delivered / read for the same message collapse to read."""
    lf = _bronze_frame(
        [
            _bronze_row(status="sent", timestamp=datetime(2026, 4, 23, 12, 0, 0)),
            _bronze_row(
                status="delivered", timestamp=datetime(2026, 4, 23, 12, 0, 1)
            ),
            _bronze_row(status="read", timestamp=datetime(2026, 4, 23, 12, 0, 2)),
        ]
    )
    out = _run_silver(lf)
    assert out.height == 1
    assert out["status"][0] == "read"


# ---------------------------------------------------------------------------
# Lead + reconciliation.
# ---------------------------------------------------------------------------


def test_silver_transform_assigns_lead_id() -> None:
    lf = _bronze_frame([_bronze_row()])
    out = _run_silver(lf)
    lead = out["lead_id"][0]
    assert lead is not None
    assert len(lead) == 16


def test_silver_transform_collides_leads_on_phone_formatting() -> None:
    """Same underlying phone written two ways → same lead_id."""
    lf = _bronze_frame(
        [
            _bronze_row(
                message_id="m1",
                conversation_id="c1",
                sender_phone="+55 11 98765-4321",
            ),
            _bronze_row(
                message_id="m2",
                conversation_id="c1",
                sender_phone="11987654321",
            ),
        ]
    )
    out = _run_silver(lf).sort("message_id")
    leads = out["lead_id"].to_list()
    assert leads[0] == leads[1]


def test_silver_transform_reconciles_name_by_lead() -> None:
    """Rows from the same lead receive the most-frequent normalized name."""
    lf = _bronze_frame(
        [
            _bronze_row(
                message_id="m1",
                conversation_id="c1",
                sender_name="Ana Paula Ribeiro",
            ),
            _bronze_row(
                message_id="m2",
                conversation_id="c1",
                sender_name="Ana Paula",
            ),
            _bronze_row(
                message_id="m3",
                conversation_id="c1",
                sender_name="Ana Paula",
            ),
        ]
    )
    out = _run_silver(lf)
    names = set(out["sender_name_normalized"].to_list())
    assert names == {"ana paula"}  # highest count wins


# ---------------------------------------------------------------------------
# PII masking.
# ---------------------------------------------------------------------------


def test_silver_transform_masks_cpf_in_body() -> None:
    lf = _bronze_frame([_bronze_row(message_body="meu CPF é 123.456.789-01 ok")])
    out = _run_silver(lf)
    body = out["message_body_masked"][0]
    assert "123.456.789-01" not in body
    assert "***.***.***-**" in body


def test_silver_transform_extracts_analytical_dimensions() -> None:
    """The regex extraction lane populates all five new columns while
    the raw PII never survives onto the masked body.
    """
    body = (
        "email ana@gmail.com, CPF 123.456.789-01, CEP 01310-930, "
        "phone 11987654321, plate ABC1D23"
    )
    lf = _bronze_frame([_bronze_row(message_body=body)])
    out = _run_silver(lf)
    assert out["email_domain"][0] == "gmail.com"
    assert out["has_cpf"][0] is True
    assert out["cep_prefix"][0] == "01310"
    assert out["has_phone_mention"][0] is True
    assert out["plate_format"][0] == "mercosul"
    # Sanity: masked body has none of the raw identifiers left.
    masked = out["message_body_masked"][0]
    assert "ana@gmail.com" not in masked
    assert "123.456.789-01" not in masked
    assert "01310-930" not in masked
    assert "ABC1D23" not in masked


def test_silver_transform_extractors_null_safe_on_empty_body() -> None:
    lf = _bronze_frame([_bronze_row(message_body=None)])
    out = _run_silver(lf)
    assert out["email_domain"][0] is None
    assert out["has_cpf"][0] is False
    assert out["cep_prefix"][0] is None
    assert out["has_phone_mention"][0] is False
    assert out["plate_format"][0] is None


def test_silver_transform_masks_sender_phone() -> None:
    lf = _bronze_frame([_bronze_row(sender_phone="11987654321")])
    out = _run_silver(lf)
    masked = out["sender_phone_masked"][0]
    assert "11987654321" not in masked
    # Mask keeps the last two digits per ``pii.mask_phone_only`` contract.
    assert masked.endswith("21")


# ---------------------------------------------------------------------------
# has_content flag.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("hello", True),
        ("   ", False),
        ("", False),
        (None, False),
    ],
)
def test_silver_transform_has_content_flag(
    body: str | None, expected: bool
) -> None:
    lf = _bronze_frame([_bronze_row(message_body=body)])
    out = _run_silver(lf)
    assert bool(out["has_content"][0]) is expected


# ---------------------------------------------------------------------------
# Metadata + timestamp + lineage constants.
# ---------------------------------------------------------------------------


def test_silver_transform_parses_metadata() -> None:
    lf = _bronze_frame(
        [
            _bronze_row(
                metadata='{"device":"android","city":"Sao Paulo","state":"SP",'
                '"response_time_sec":12,"is_business_hours":true,'
                '"lead_source":"meta"}'
            )
        ]
    )
    out = _run_silver(lf)
    meta = out["metadata"][0]
    assert meta["device"] == "android"
    assert meta["response_time_sec"] == 12


def test_silver_transform_tags_timestamp_utc() -> None:
    lf = _bronze_frame(
        [_bronze_row(timestamp=datetime(2026, 4, 23, 12, 0, 0))]
    )
    out = _run_silver(lf)
    assert out["timestamp"][0] == datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def test_silver_transform_stamps_lineage_constants() -> None:
    lf = _bronze_frame([_bronze_row()])
    out = _run_silver(lf)
    assert out["silver_batch_id"][0] == SILVER_BATCH_ID
    assert out["transformed_at"][0] == NOW
    # Lineage from Bronze passes through untouched.
    assert out["source_file_hash"][0] == "deadbeef"


# ---------------------------------------------------------------------------
# assert_silver_schema.
# ---------------------------------------------------------------------------


def test_assert_silver_schema_accepts_transform_output() -> None:
    lf = _bronze_frame([_bronze_row()])
    out = _run_silver(lf)
    assert_silver_schema(out)  # should not raise


def test_assert_silver_schema_rejects_drift() -> None:
    lf = _bronze_frame([_bronze_row()])
    out = _run_silver(lf).drop("has_content")
    with pytest.raises(SchemaDriftError, match="missing column"):
        assert_silver_schema(out)
