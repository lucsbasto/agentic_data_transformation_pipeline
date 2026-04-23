"""Tests for ``pipeline.silver.quarantine``."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from pipeline.schemas.bronze import BRONZE_SCHEMA
from pipeline.silver.quarantine import (
    REJECT_REASON_NULL_CONVERSATION_ID,
    REJECT_REASON_NULL_MESSAGE_ID,
    REJECTED_SCHEMA,
    partition_rows,
)

REJECTED_AT = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _row(
    *,
    message_id: str | None = "m1",
    conversation_id: str | None = "c1",
) -> dict[str, object]:
    """Minimal Bronze row — only the fields quarantine inspects matter
    for correctness; everything else is constant filler to satisfy the
    Bronze schema.
    """
    return {
        "message_id": message_id,
        "conversation_id": conversation_id,
        "timestamp": datetime(2026, 4, 23, 12, 0, 0),
        "direction": "inbound",
        "sender_phone": "+5511987654321",
        "sender_name": "Ana",
        "message_type": "text",
        "message_body": "hi",
        "status": "sent",
        "channel": "whatsapp",
        "campaign_id": "c",
        "agent_id": "a",
        "conversation_outcome": None,
        "metadata": "{}",
        "batch_id": "b1",
        "ingested_at": datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC),
        "source_file_hash": "deadbeef",
    }


def _frame(rows: list[dict[str, object]]) -> pl.LazyFrame:
    return pl.DataFrame(rows, schema=BRONZE_SCHEMA).lazy()


def test_partition_keeps_valid_rows_unchanged() -> None:
    lf = _frame([_row(message_id="m1"), _row(message_id="m2")])
    valid, rejected = partition_rows(lf, rejected_at=REJECTED_AT)
    assert valid.collect().height == 2
    assert rejected.collect().height == 0


def test_partition_rejects_null_message_id() -> None:
    lf = _frame([_row(message_id=None), _row(message_id="m_ok")])
    valid, rejected = partition_rows(lf, rejected_at=REJECTED_AT)
    valid_df = valid.collect()
    rejected_df = rejected.collect()
    assert valid_df.height == 1
    assert valid_df["message_id"][0] == "m_ok"
    assert rejected_df.height == 1
    assert rejected_df["reject_reason"][0] == REJECT_REASON_NULL_MESSAGE_ID


def test_partition_rejects_null_conversation_id() -> None:
    lf = _frame([_row(conversation_id=None), _row(conversation_id="c_ok")])
    _, rejected = partition_rows(lf, rejected_at=REJECTED_AT)
    df = rejected.collect()
    assert df.height == 1
    assert df["reject_reason"][0] == REJECT_REASON_NULL_CONVERSATION_ID


def test_partition_null_both_keys_labels_message_id_first() -> None:
    """Order is deterministic when both keys are null — we check
    ``message_id`` first so the reason is stable across runs."""
    lf = _frame([_row(message_id=None, conversation_id=None)])
    _, rejected = partition_rows(lf, rejected_at=REJECTED_AT)
    df = rejected.collect()
    assert df["reject_reason"][0] == REJECT_REASON_NULL_MESSAGE_ID


def test_partition_stamps_rejected_at() -> None:
    lf = _frame([_row(message_id=None)])
    _, rejected = partition_rows(lf, rejected_at=REJECTED_AT)
    df = rejected.collect()
    assert df["rejected_at"][0] == REJECTED_AT


def test_partition_output_schema_matches_rejected_schema() -> None:
    lf = _frame([_row(message_id=None)])
    _, rejected = partition_rows(lf, rejected_at=REJECTED_AT)
    df = rejected.collect()
    assert df.schema == REJECTED_SCHEMA


def test_partition_preserves_total_row_count() -> None:
    """Valid + rejected always equals input — nothing is silently dropped."""
    lf = _frame(
        [
            _row(message_id="m1"),
            _row(message_id=None),
            _row(conversation_id=None),
            _row(message_id="m4"),
        ]
    )
    valid, rejected = partition_rows(lf, rejected_at=REJECTED_AT)
    assert valid.collect().height + rejected.collect().height == 4


def test_partition_valid_keeps_input_schema() -> None:
    """Valid lane must stay on the Bronze schema so silver_transform
    can consume it without a cast."""
    lf = _frame([_row(message_id="m1")])
    valid, _ = partition_rows(lf, rejected_at=REJECTED_AT)
    assert valid.collect().schema == BRONZE_SCHEMA
