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
    if not source.is_file():
        raise IngestError(f"source parquet not found: {source}")
    try:
        return pl.scan_parquet(source)
    except Exception as exc:
        raise IngestError(f"failed to scan source {source}: {exc}") from exc


def validate_source_columns(lf: pl.LazyFrame) -> None:
    """Raise :class:`SchemaDriftError` if the source is missing any expected column.

    Extra columns are tolerated at this stage — the transform step drops
    anything that is not declared in :data:`SOURCE_COLUMNS`.
    """
    actual = set(lf.collect_schema().names())
    expected = set(SOURCE_COLUMNS)
    missing = expected - actual
    if missing:
        raise SchemaDriftError(
            f"source is missing required columns: {sorted(missing)}"
        )
