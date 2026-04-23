"""Bronze layer schema.

The Bronze layer is a 1:1 typed landing of the raw source. Every source
column is retained. Three lineage columns (``batch_id``, ``ingested_at``,
``source_file_hash``) are added by the ingest writer.

Closed-set columns are declared as :class:`polars.Enum` so that any new
value in the source parquet raises a :class:`SchemaDriftError` at write
time instead of silently polluting downstream analytics.
"""

from __future__ import annotations

from typing import Final

import polars as pl

# ---------------------------------------------------------------------------
# Closed-set categorical values (frozen from the 2026-04-22 measurement).
# Extending these is a schema change; bump D- entry in STATE.md.
# ---------------------------------------------------------------------------
DIRECTION_VALUES: Final[tuple[str, ...]] = ("inbound", "outbound")

STATUS_VALUES: Final[tuple[str, ...]] = ("delivered", "read", "sent")

MESSAGE_TYPE_VALUES: Final[tuple[str, ...]] = (
    "audio",
    "contact",
    "document",
    "image",
    "location",
    "sticker",
    "text",
    "video",
)

# ---------------------------------------------------------------------------
# Typed Bronze schema.
# ---------------------------------------------------------------------------
_BRONZE_FIELDS: dict[str, pl.DataType] = {
    # Source-origin columns
    "message_id": pl.String(),
    "conversation_id": pl.String(),
    "timestamp": pl.Datetime("us"),
    "direction": pl.Enum(list(DIRECTION_VALUES)),
    "sender_phone": pl.String(),
    "sender_name": pl.String(),
    "message_type": pl.Enum(list(MESSAGE_TYPE_VALUES)),
    "message_body": pl.String(),
    "status": pl.Enum(list(STATUS_VALUES)),
    "channel": pl.String(),
    "campaign_id": pl.String(),
    "agent_id": pl.String(),
    "conversation_outcome": pl.String(),
    "metadata": pl.String(),
    # Lineage columns added by the writer
    "batch_id": pl.String(),
    "ingested_at": pl.Datetime("us", time_zone="UTC"),
    "source_file_hash": pl.String(),
}

BRONZE_SCHEMA: Final[pl.Schema] = pl.Schema(_BRONZE_FIELDS)

SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "message_id",
    "conversation_id",
    "timestamp",
    "direction",
    "sender_phone",
    "sender_name",
    "message_type",
    "message_body",
    "status",
    "channel",
    "campaign_id",
    "agent_id",
    "conversation_outcome",
    "metadata",
)
"""Columns expected in the raw parquet before ingest adds lineage fields."""

LINEAGE_COLUMNS: Final[tuple[str, ...]] = (
    "batch_id",
    "ingested_at",
    "source_file_hash",
)
"""Columns appended by the Bronze writer."""

TIMESTAMP_SOURCE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
"""Measured format of the ``timestamp`` string in the raw parquet."""
