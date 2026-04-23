"""Bronze layer schema.

The Bronze layer is a 1:1 typed landing of the raw source. Every source
column is retained. Three lineage columns (``batch_id``, ``ingested_at``,
``source_file_hash``) are added by the ingest writer.

Closed-set columns are declared as :class:`polars.Enum` so that any new
value in the source parquet raises a :class:`SchemaDriftError` at write
time instead of silently polluting downstream analytics.
"""

from __future__ import annotations

# LEARN: ``typing.Final`` marks a name as "constant — do not reassign".
# Python does not enforce it at runtime, but mypy and ruff will flag any
# later mutation. It also documents intent: a reader knows
# ``BRONZE_SCHEMA`` is a read-only export.
from typing import Final

# LEARN: ``polars`` is the DataFrame engine this repo uses. The ``pl``
# alias is the community convention (like ``import pandas as pd``).
import polars as pl

# ---------------------------------------------------------------------------
# Closed-set categorical values (frozen from the 2026-04-22 measurement).
# Extending these is a schema change; bump D- entry in STATE.md.
# ---------------------------------------------------------------------------
# LEARN: a ``tuple`` is an *immutable* sequence — once built, its items
# cannot change. That makes it the right container for "values I measured
# once and never want callers to mutate". A ``list`` would allow
# ``DIRECTION_VALUES.append("diagonal")`` and silently break every
# downstream Enum check.
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
# LEARN: a Python ``dict`` literal maps column name -> polars dtype. We
# declare this as a plain dict first, then wrap it in a ``pl.Schema`` so
# Polars accepts it everywhere a schema is expected.
_BRONZE_FIELDS: dict[str, pl.DataType] = {
    # Source-origin columns
    "message_id": pl.String(),
    "conversation_id": pl.String(),
    "timestamp": pl.Datetime("us"),
    # LEARN: ``pl.Enum`` is Polars' CLOSED-SET categorical type. Any value
    # outside the declared list raises an error at cast time. That is
    # exactly the *drift detection* we want: if WhatsApp invents a new
    # ``"direction"`` value next year, our ingest pipeline fails LOUDLY
    # at the cast step instead of quietly writing it to Bronze.
    # ``list(DIRECTION_VALUES)`` converts the tuple to a list because
    # ``pl.Enum`` wants a mutable sequence (historical Polars quirk).
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
    # Lineage columns added by the writer. Medallion architecture calls
    # these "provenance" — they let Silver/Gold answer "which batch and
    # which source file produced this row?" without a separate join.
    "batch_id": pl.String(),
    # LEARN: ``time_zone="UTC"`` tags the timestamp as *aware*. Mixing
    # aware + naive timestamps in the same column is a classic bug; we
    # set lineage timestamps to UTC up front and keep raw source naive.
    "ingested_at": pl.Datetime("us", time_zone="UTC"),
    "source_file_hash": pl.String(),
}

# LEARN: ``pl.Schema`` is Polars' authoritative "column order + types"
# object. Wrapping the dict lets callers compare against it with
# ``df.schema == BRONZE_SCHEMA`` rather than iterating manually.
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

# LEARN: a strftime-style format string. ``%Y-%m-%d %H:%M:%S`` matches
# ``2026-04-22 15:30:45``. Polars uses this to parse the source column.
# We measured this format from the raw parquet — do not change without
# re-measuring; a mismatch raises at the cast step.
TIMESTAMP_SOURCE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
"""Measured format of the ``timestamp`` string in the raw parquet."""
