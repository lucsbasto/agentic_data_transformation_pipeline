"""Source reader: lazy scan + schema sanity check."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pipeline.errors import IngestError, SchemaDriftError
from pipeline.schemas.bronze import SOURCE_COLUMNS


def scan_source(source: Path) -> pl.LazyFrame:
    """Return a :class:`polars.LazyFrame` over the raw source parquet.

    No I/O beyond parquet metadata is performed; the full scan runs when
    the caller collects. ``IngestError`` wraps any filesystem failure so
    the agent loop can classify it.
    """
    # LEARN: fail fast with a clear domain error instead of letting
    # Polars raise a lower-level ``FileNotFoundError`` deep in its
    # internals. Callers catch ``IngestError`` and surface the operator
    # the useful "source not found" message.
    if not source.is_file():
        raise IngestError(f"source parquet not found: {source}")
    try:
        # LEARN: ``scan_parquet`` builds a LAZY plan — it reads parquet
        # metadata (schema, statistics) but NOT the row data. Nothing
        # happens until someone calls ``.collect()``. That lets Polars
        # push filters and column selections down into the read, so a
        # pipeline that only needs 3 of 14 columns can skip the rest.
        return pl.scan_parquet(source)
    except Exception as exc:
        # LEARN: broad ``except Exception`` is OK at a *layer boundary*
        # — we want every kind of Polars/IO error to arrive as our
        # domain error. The ``from exc`` keeps the original traceback
        # attached for debugging.
        raise IngestError(f"failed to scan source {source}: {exc}") from exc


def validate_source_columns(lf: pl.LazyFrame) -> None:
    """Raise :class:`SchemaDriftError` if the source is missing any expected column.

    Extra columns are tolerated at this stage — the transform step drops
    anything that is not declared in :data:`SOURCE_COLUMNS`.
    """
    # LEARN: ``collect_schema()`` reads ONLY the parquet metadata —
    # cheap, no row data. ``.names()`` returns an iterable of column
    # names. Wrapping in ``set(...)`` unlocks Python's set-algebra:
    #   expected - actual  = what is missing
    #   actual - expected  = what is extra
    actual = set(lf.collect_schema().names())
    expected = set(SOURCE_COLUMNS)
    missing = expected - actual
    if missing:
        # LEARN: ``sorted(missing)`` converts the set into a stable list
        # so the error message is deterministic. Tests assert on it.
        raise SchemaDriftError(
            f"source is missing required columns: {sorted(missing)}"
        )
