"""Quarantine / row-level validation for the Silver step.

PRD §RF-08 calls for a pipeline that *never stops on a bad row*. Bronze
is a 1:1 typed landing of the source, so it will happily store a row
with ``message_id = NULL`` — the Bronze schema only types the column,
it does not demand presence. Silver, on the other hand, must produce
exactly one row per ``(conversation_id, message_id)``; rows missing
either key would break that contract.

Contract
--------

- A row is **valid** when both ``message_id`` and ``conversation_id``
  are non-null.
- A row is **rejected** otherwise. The bad row, plus a short machine-
  readable ``reject_reason`` string and a ``rejected_at`` UTC
  timestamp, is written to a sibling quarantine partition
  (``silver_root/batch_id=<id>/rejected/part-0.parquet``).

What ends up here vs. in the CLI
--------------------------------

This module is pure: it takes a :class:`polars.LazyFrame` with the
Bronze schema and returns ``(valid_lf, rejected_lf)`` so callers can
collect / write each lane independently. Writing the rejected parquet,
updating the manifest's ``rows_rejected`` counter, and emitting logs
are the CLI's responsibility.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final

import polars as pl

from pipeline.schemas.bronze import BRONZE_SCHEMA

__all__ = [
    "REJECTED_COLUMNS",
    "REJECTED_SCHEMA",
    "REJECT_REASON_NULL_CONVERSATION_ID",
    "REJECT_REASON_NULL_MESSAGE_ID",
    "partition_rows",
]


# ---------------------------------------------------------------------------
# Reject reason taxonomy.
# ---------------------------------------------------------------------------
# LEARN: reasons are ``str`` constants so Python code referring to
# them stays type-checkable and grep-able. Future reasons (bad phone,
# schema drift flag, etc.) can be added without touching the CLI — the
# ``reject_reason`` column on the parquet is plain text.

REJECT_REASON_NULL_MESSAGE_ID: Final[str] = "null_message_id"
REJECT_REASON_NULL_CONVERSATION_ID: Final[str] = "null_conversation_id"


# ---------------------------------------------------------------------------
# Rejected parquet schema.
# ---------------------------------------------------------------------------
# LEARN: we keep every Bronze column on the rejected row — the whole
# point of quarantine is to hand an operator enough context to
# reproduce the failure. Appending ``reject_reason`` + ``rejected_at``
# gives the run-level reason and the wall clock of the decision. This
# lives here rather than in ``schemas/silver.py`` because the shape is
# an output contract of this module, not of the main Silver transform.

_REJECTED_FIELDS: dict[str, pl.DataType] = {
    **dict(BRONZE_SCHEMA),
    "reject_reason": pl.String(),
    "rejected_at": pl.Datetime("us", time_zone="UTC"),
}

REJECTED_SCHEMA: Final[pl.Schema] = pl.Schema(_REJECTED_FIELDS)
"""Authoritative schema for rejected parquet files."""

REJECTED_COLUMNS: Final[tuple[str, ...]] = tuple(_REJECTED_FIELDS.keys())


# ---------------------------------------------------------------------------
# partition_rows
# ---------------------------------------------------------------------------


def partition_rows(
    lf: pl.LazyFrame,
    *,
    rejected_at: datetime,
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Split ``lf`` into ``(valid, rejected)`` LazyFrames.

    The ``rejected`` LazyFrame has the schema declared by
    :data:`REJECTED_SCHEMA` — original Bronze columns plus
    ``reject_reason`` and ``rejected_at``. The ``valid`` LazyFrame
    keeps its input schema unchanged, so callers can feed it straight
    into :func:`pipeline.silver.transform.silver_transform`.

    ``rejected_at`` is passed in rather than read from a clock inside
    this function so the result is deterministic — tests can pin the
    timestamp, and the CLI passes a single ``datetime.now(tz=UTC)``
    value that matches the ``transformed_at`` it gives to
    ``silver_transform`` for the same run.
    """
    # LEARN: one boolean mask drives both branches. Building it once
    # lets Polars fuse the scans: ``filter(invalid_mask)`` and
    # ``filter(~invalid_mask)`` share the same plan node.
    invalid_mask = (
        pl.col("message_id").is_null() | pl.col("conversation_id").is_null()
    )

    # LEARN: the reason column uses ``when/then/otherwise`` so every
    # rejected row carries exactly one reason. Order matters — we
    # check ``message_id`` first so a row that is null on BOTH keys
    # still produces a deterministic label instead of a random pick.
    reason_expr = (
        pl.when(pl.col("message_id").is_null())
        .then(pl.lit(REJECT_REASON_NULL_MESSAGE_ID))
        .when(pl.col("conversation_id").is_null())
        .then(pl.lit(REJECT_REASON_NULL_CONVERSATION_ID))
        .otherwise(pl.lit(""))
    )

    rejected = (
        lf.filter(invalid_mask)
        .with_columns(
            reason_expr.alias("reject_reason"),
            pl.lit(rejected_at)
            .cast(pl.Datetime("us", time_zone="UTC"))
            .alias("rejected_at"),
        )
        # Pin column order to REJECTED_SCHEMA so parquet writers get
        # a stable layout regardless of how the ``with_columns`` call
        # reshuffled things internally.
        .select(list(REJECTED_COLUMNS))
    )

    valid = lf.filter(~invalid_mask)
    return valid, rejected
