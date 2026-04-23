"""Source-to-Bronze cast.

Takes the string-typed source and projects it into the typed Bronze
schema declared in :mod:`pipeline.schemas.bronze`. Closed-set columns
(``direction``, ``status``, ``message_type``) are cast to
:class:`polars.Enum`; any unknown value triggers a
:class:`SchemaDriftError` with enough context to diagnose the drift.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import polars.exceptions as pl_exc

from pipeline.errors import SchemaDriftError
from pipeline.schemas.bronze import (
    BRONZE_SCHEMA,
    DIRECTION_VALUES,
    MESSAGE_TYPE_VALUES,
    SOURCE_COLUMNS,
    STATUS_VALUES,
    TIMESTAMP_SOURCE_FORMAT,
)


def transform_to_bronze(
    lf: pl.LazyFrame,
    *,
    batch_id: str,
    source_hash: str,
    ingested_at: datetime,
) -> pl.LazyFrame:
    """Project the source ``LazyFrame`` into the Bronze schema.

    Lineage columns (``batch_id``, ``ingested_at``, ``source_file_hash``)
    are appended last so their position is stable in the output parquet.
    The result is still lazy; the caller is responsible for ``collect``.
    """
    typed = lf.select(
        pl.col("message_id").cast(pl.String),
        pl.col("conversation_id").cast(pl.String),
        pl.col("timestamp").str.to_datetime(TIMESTAMP_SOURCE_FORMAT, time_unit="us"),
        pl.col("direction").cast(pl.Enum(list(DIRECTION_VALUES))),
        pl.col("sender_phone").cast(pl.String),
        pl.col("sender_name").cast(pl.String),
        pl.col("message_type").cast(pl.Enum(list(MESSAGE_TYPE_VALUES))),
        pl.col("message_body").cast(pl.String),
        pl.col("status").cast(pl.Enum(list(STATUS_VALUES))),
        pl.col("channel").cast(pl.String),
        pl.col("campaign_id").cast(pl.String),
        pl.col("agent_id").cast(pl.String),
        pl.col("conversation_outcome").cast(pl.String),
        pl.col("metadata").cast(pl.String),
    ).with_columns(
        pl.lit(batch_id, dtype=pl.String).alias("batch_id"),
        pl.lit(ingested_at).dt.cast_time_unit("us").dt.convert_time_zone("UTC").alias(
            "ingested_at"
        ),
        pl.lit(source_hash, dtype=pl.String).alias("source_file_hash"),
    )

    return typed


def assert_bronze_schema(df: pl.DataFrame) -> None:
    """Assert that a concrete :class:`polars.DataFrame` matches BRONZE_SCHEMA.

    Called by the writer (see :mod:`pipeline.ingest.writer`) right before
    parquet is written. Mismatches surface as :class:`SchemaDriftError`.
    """
    actual = df.schema
    if actual != BRONZE_SCHEMA:
        mismatches = _schema_diff(actual, BRONZE_SCHEMA)
        raise SchemaDriftError(
            "bronze schema mismatch:\n" + "\n".join(f"  - {m}" for m in mismatches)
        )


def collect_bronze(lf: pl.LazyFrame) -> pl.DataFrame:
    """Materialize a Bronze lazy plan, converting Polars cast errors.

    Every Bronze collect in the pipeline goes through this helper so that
    enum drift, timestamp parse failures, and other cast issues surface
    as :class:`SchemaDriftError` instead of raw Polars exceptions.
    """
    try:
        return lf.collect()
    except pl_exc.PolarsError as exc:
        raise SchemaDriftError(
            f"source violates Bronze contract during cast: {exc}"
        ) from exc


__all__ = [
    "SOURCE_COLUMNS",
    "assert_bronze_schema",
    "collect_bronze",
    "transform_to_bronze",
]


def _schema_diff(
    actual: pl.Schema, expected: pl.Schema
) -> list[str]:
    diffs: list[str] = []
    actual_keys = set(actual.names())
    expected_keys = set(expected.names())
    for missing in sorted(expected_keys - actual_keys):
        diffs.append(f"missing column: {missing}")
    for extra in sorted(actual_keys - expected_keys):
        diffs.append(f"unexpected column: {extra}")
    for name in actual.names():
        if name in expected and actual[name] != expected[name]:
            diffs.append(
                f"{name}: got {actual[name]!r}, expected {expected[name]!r}"
            )
    return diffs
