"""Source-to-Bronze cast.

Takes the string-typed source and projects it into the typed Bronze
schema declared in :mod:`pipeline.schemas.bronze`. Closed-set columns
(``direction``, ``status``, ``message_type``) are cast to
:class:`polars.Enum`; any unknown value triggers a
:class:`SchemaDriftError` with enough context to diagnose the drift.
"""

from __future__ import annotations

from datetime import datetime

# LEARN: ``polars.exceptions as pl_exc`` (second import below) gives us
# the namespace to catch Polars-specific errors such as ``PolarsError``
# and its subclasses in ``collect_bronze``.
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
    # LEARN: ``lf.select(...)`` returns a new LazyFrame containing ONLY
    # the columns you name, in the order you name them. That is both a
    # "keep these columns" filter and a "set the column order" step.
    # Each argument is a Polars *expression* built from ``pl.col(...)``.
    typed = lf.select(
        # LEARN: ``pl.col("x").cast(pl.String)`` builds an expression
        # that reads column ``x`` and casts its values. ``.cast`` at
        # lazy time plans the cast; it doesn't run until ``collect``.
        pl.col("message_id").cast(pl.String),
        pl.col("conversation_id").cast(pl.String),
        # LEARN: ``.str.to_datetime(fmt, time_unit="us")`` parses a
        # string column using the ``strftime``-style format we captured
        # in ``schemas/bronze.TIMESTAMP_SOURCE_FORMAT``. Microsecond
        # precision matches the declared Bronze dtype.
        pl.col("timestamp").str.to_datetime(TIMESTAMP_SOURCE_FORMAT, time_unit="us"),
        # LEARN: casting to ``pl.Enum(...)`` is where drift detection
        # kicks in. Any source value NOT in the enum list raises
        # ``InvalidOperationError`` at ``collect`` time — exactly what
        # we want for a Bronze layer that should refuse surprise values.
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
        # LEARN: ``pl.lit(value)`` creates a literal column — the same
        # value for every row. ``.alias(name)`` names it. ``with_columns``
        # appends them without touching existing ones, which is why
        # lineage columns always land at the end.
        pl.lit(batch_id, dtype=pl.String).alias("batch_id"),
        # LEARN: ``.dt.cast_time_unit("us").dt.convert_time_zone("UTC")``
        # chains two Polars datetime helpers on the literal so it matches
        # the ``Datetime("us", time_zone="UTC")`` dtype declared in the
        # Bronze schema. Dtype mismatches at write-time are exactly the
        # errors ``assert_bronze_schema`` catches below.
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
    # LEARN: ``pl.Schema`` supports equality (``==``). If it differs,
    # ``_schema_diff`` produces a human-readable list of what changed.
    if actual != BRONZE_SCHEMA:
        mismatches = _schema_diff(actual, BRONZE_SCHEMA)
        # LEARN: ``"\n".join(...)`` is the Python way to build
        # multi-line strings. The generator expression ``f"  - {m}"
        # for m in mismatches`` produces one bullet per diff.
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
        # LEARN: ``.collect()`` is where a LazyFrame materializes into
        # an eager DataFrame — this is where ALL the work happens:
        # file read, casts, expressions, the lot. Any schema violation
        # surfaces here.
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


# LEARN: leading underscore marks this helper as module-private. Callers
# inside this module use it freely; external imports should not. The
# Python "private by convention" rule.
def _schema_diff(
    actual: pl.Schema, expected: pl.Schema
) -> list[str]:
    # LEARN: we build a plain ``list[str]`` and append to it. Simpler
    # than a generator when the result is modest in size and you want
    # to collect several kinds of diffs in one pass.
    diffs: list[str] = []
    actual_keys = set(actual.names())
    expected_keys = set(expected.names())
    # LEARN: ``expected - actual`` is set difference: names in expected
    # but missing in actual. ``sorted(...)`` makes the diff ordering
    # deterministic so tests can assert on the exact message.
    for missing in sorted(expected_keys - actual_keys):
        diffs.append(f"missing column: {missing}")
    for extra in sorted(actual_keys - expected_keys):
        diffs.append(f"unexpected column: {extra}")
    # LEARN: iterate actual column order so dtype mismatches surface in
    # the order the user would see them in the DataFrame. ``actual[name]``
    # indexing into a ``pl.Schema`` returns that column's dtype.
    for name in actual.names():
        if name in expected and actual[name] != expected[name]:
            diffs.append(
                f"{name}: got {actual[name]!r}, expected {expected[name]!r}"
            )
    return diffs
