"""Tests for ``pipeline.state.manifest.ManifestDB``."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.errors import ManifestError
from pipeline.state.manifest import ManifestDB


@pytest.fixture
def db() -> ManifestDB:
    with ManifestDB(":memory:") as manifest:
        yield manifest


def _insert(db: ManifestDB, batch_id: str = "b1") -> None:
    db.insert_batch(
        batch_id=batch_id,
        source_path="/tmp/src.parquet",
        source_hash="deadbeef",
        source_mtime=1_700_000_000,
        started_at="2026-04-22T12:00:00Z",
    )


def test_ensure_schema_creates_tables() -> None:
    with ManifestDB(":memory:") as db:
        assert db.get_batch("missing") is None


def test_insert_batch_and_read_back(db: ManifestDB) -> None:
    _insert(db)
    row = db.get_batch("b1")
    assert row is not None
    assert row.batch_id == "b1"
    assert row.status == "IN_PROGRESS"
    assert row.is_completed is False
    assert row.is_failed is False


def test_insert_duplicate_batch_raises(db: ManifestDB) -> None:
    _insert(db)
    with pytest.raises(ManifestError, match="already exists"):
        _insert(db)


def test_mark_completed_transitions_status(db: ManifestDB) -> None:
    _insert(db)
    db.mark_completed(
        batch_id="b1",
        rows_read=100,
        rows_written=100,
        bronze_path="data/bronze/batch_id=b1/part-0.parquet",
        finished_at="2026-04-22T12:00:05Z",
        duration_ms=5000,
    )
    row = db.get_batch("b1")
    assert row is not None
    assert row.is_completed
    assert row.rows_read == 100
    assert row.rows_written == 100
    assert row.bronze_path == "data/bronze/batch_id=b1/part-0.parquet"
    assert row.duration_ms == 5000


def test_mark_failed_records_error(db: ManifestDB) -> None:
    _insert(db)
    db.mark_failed(
        batch_id="b1",
        finished_at="2026-04-22T12:00:01Z",
        duration_ms=1000,
        error_type="SchemaDriftError",
        error_message="unexpected value: sideways",
    )
    row = db.get_batch("b1")
    assert row is not None
    assert row.is_failed
    assert row.error_type == "SchemaDriftError"
    assert row.error_message == "unexpected value: sideways"


def test_mark_missing_batch_raises(db: ManifestDB) -> None:
    with pytest.raises(ManifestError, match="no batch row to update"):
        db.mark_completed(
            batch_id="ghost",
            rows_read=0,
            rows_written=0,
            bronze_path="x",
            finished_at="t",
            duration_ms=0,
        )


def test_is_batch_completed(db: ManifestDB) -> None:
    _insert(db)
    assert db.is_batch_completed("b1") is False
    db.mark_completed(
        batch_id="b1",
        rows_read=1,
        rows_written=1,
        bronze_path="x",
        finished_at="t",
        duration_ms=1,
    )
    assert db.is_batch_completed("b1") is True
    assert db.is_batch_completed("missing") is False


def test_requires_open_connection() -> None:
    db = ManifestDB(":memory:")
    with pytest.raises(ManifestError, match="not open"):
        db.get_batch("x")


def test_transaction_rolls_back_on_error(db: ManifestDB) -> None:
    _insert(db)
    # Force a duplicate to trigger rollback. Prior row must remain untouched.
    with pytest.raises(ManifestError):
        _insert(db)
    row = db.get_batch("b1")
    assert row is not None
    assert row.status == "IN_PROGRESS"


def test_file_backed_db_creates_parent_dir(tmp_path: Path) -> None:
    nested = tmp_path / "sub" / "dir" / "manifest.db"
    with ManifestDB(nested) as db:
        _insert(db)
    assert nested.is_file()
