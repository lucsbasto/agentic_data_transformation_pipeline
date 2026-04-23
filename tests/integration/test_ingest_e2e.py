"""Library-level end-to-end ingest test on a synthetic fixture.

Drives the three-step Bronze flow (scan → transform → write) plus the
manifest row-keeping that the CLI entrypoint will orchestrate in F1.6.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.ingest import (
    compute_batch_identity,
    scan_source,
    transform_to_bronze,
    validate_source_columns,
    write_bronze,
)
from pipeline.state.manifest import ManifestDB

pytestmark = pytest.mark.integration


def _ingest(
    *, source: Path, bronze_root: Path, manifest: ManifestDB
) -> str:
    """Minimal stand-in for the eventual CLI ingest command.

    Returns the ``batch_id`` that was written.
    """
    identity = compute_batch_identity(source)
    if manifest.is_batch_completed(identity.batch_id):
        return identity.batch_id

    started = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest.insert_batch(
        batch_id=identity.batch_id,
        source_path=str(source),
        source_hash=identity.source_hash,
        source_mtime=identity.source_mtime,
        started_at=started,
    )
    try:
        lf = scan_source(source)
        validate_source_columns(lf)
        typed = transform_to_bronze(
            lf,
            batch_id=identity.batch_id,
            source_hash=identity.source_hash,
            ingested_at=datetime.now(tz=UTC),
        )
        df = typed.collect()
        result = write_bronze(df, bronze_root=bronze_root, batch_id=identity.batch_id)
    except Exception as exc:
        manifest.mark_failed(
            batch_id=identity.batch_id,
            finished_at=datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_ms=0,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise

    manifest.mark_completed(
        batch_id=identity.batch_id,
        rows_read=df.height,
        rows_written=result.rows_written,
        bronze_path=str(result.bronze_path),
        finished_at=datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        duration_ms=0,
    )
    return identity.batch_id


def test_ingest_end_to_end_produces_bronze_and_manifest(
    tiny_source_parquet: Path, tmp_path: Path
) -> None:
    bronze_root = tmp_path / "bronze"
    db_path = tmp_path / "manifest.db"

    with ManifestDB(db_path) as manifest:
        batch_id = _ingest(
            source=tiny_source_parquet,
            bronze_root=bronze_root,
            manifest=manifest,
        )
        row = manifest.get_batch(batch_id)

    assert row is not None
    assert row.is_completed
    assert row.rows_written is not None
    assert row.rows_written > 0
    bronze_file = bronze_root / f"batch_id={batch_id}" / "part-0.parquet"
    assert bronze_file.is_file()

    reloaded = pl.scan_parquet(bronze_file).collect()
    assert reloaded.height == row.rows_written


def test_ingest_is_idempotent_on_second_run(
    tiny_source_parquet: Path, tmp_path: Path
) -> None:
    bronze_root = tmp_path / "bronze"
    db_path = tmp_path / "manifest.db"

    with ManifestDB(db_path) as manifest:
        first = _ingest(
            source=tiny_source_parquet,
            bronze_root=bronze_root,
            manifest=manifest,
        )
        # Second run returns the same batch_id without doing work.
        second = _ingest(
            source=tiny_source_parquet,
            bronze_root=bronze_root,
            manifest=manifest,
        )
    assert first == second
