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


def test_reset_stale_marks_in_progress_as_failed(tmp_path: Path) -> None:
    db_path = tmp_path / "manifest.db"
    # First "process" leaves a stale IN_PROGRESS row.
    with ManifestDB(db_path) as db:
        _insert(db, "stale1")
        _insert(db, "stale2")
        row = db.get_batch("stale1")
        assert row is not None
        assert row.status == "IN_PROGRESS"
    # Second "process" opens the same file and runs reset_stale on open.
    with ManifestDB(db_path) as db:
        stale = db.get_batch("stale1")
        assert stale is not None
        assert stale.is_failed
        assert stale.error_type == "StaleInProgress"
        assert stale.error_message is not None


def test_reset_stale_returns_swept_count(db: ManifestDB) -> None:
    _insert(db, "one")
    _insert(db, "two")
    swept = db.reset_stale()
    assert swept == 2


def test_mark_completed_clears_prior_error_fields(db: ManifestDB) -> None:
    _insert(db)
    db.mark_failed(
        batch_id="b1",
        finished_at="2026-04-22T12:00:01Z",
        duration_ms=100,
        error_type="IngestError",
        error_message="first try failed",
    )
    # Manually re-arm to IN_PROGRESS so we can retry. (In production the
    # workflow is: reset_stale -> drop-and-reinsert or a dedicated retry path.
    # Here we exercise the low-level invariant that COMPLETED clears errors.)
    assert db._conn is not None
    db._conn.execute(
        "UPDATE batches SET status = 'IN_PROGRESS' WHERE batch_id = ?;",
        ("b1",),
    )
    db.mark_completed(
        batch_id="b1",
        rows_read=10,
        rows_written=10,
        bronze_path="data/bronze/batch_id=b1/part-0.parquet",
        finished_at="2026-04-22T12:00:05Z",
        duration_ms=5000,
    )
    row = db.get_batch("b1")
    assert row is not None
    assert row.is_completed
    assert row.error_type is None
    assert row.error_message is None


def test_update_with_invalid_status_raises(db: ManifestDB) -> None:
    _insert(db)
    with pytest.raises(ManifestError, match="invalid status"):
        db._update_status(
            batch_id="b1",
            status="BOGUS",
            finished_at="t",
            duration_ms=0,
        )
