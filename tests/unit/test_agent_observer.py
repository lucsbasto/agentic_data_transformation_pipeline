"""Coverage for the F4 agent observer (F4.9)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from pipeline.agent.observer import (
    DEFAULT_STALE_AFTER_S,
    discover_source_batches,
    is_pending,
    scan,
)
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS
from pipeline.state.manifest import BatchRow, ManifestDB

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def db() -> Iterator[ManifestDB]:
    manifest = ManifestDB(":memory:").open()
    try:
        yield manifest
    finally:
        manifest.close()


def _write_source_parquet(path: Path, *, marker: str = "x") -> None:
    """Tiny one-row source parquet — content varies by ``marker`` so
    each call yields a different content hash and therefore a
    different ``batch_id``."""
    rows = {col: [marker] for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"]
    rows["direction"] = ["inbound"]
    rows["message_type"] = ["text"]
    rows["status"] = ["delivered"]
    df = pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _insert_batch(
    db: ManifestDB,
    *,
    batch_id: str,
    status: str,
    started_at: str,
    finished_at: str | None = None,
    error_type: str | None = None,
) -> None:
    """Drop a row directly into ``batches`` so tests can pin the
    status without going through the F1 ingest CLI."""
    conn = db._require_conn()
    conn.execute(
        "INSERT INTO batches (batch_id, source_path, source_hash, source_mtime, "
        "status, started_at, finished_at, error_type) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
        (batch_id, "fake/source", "deadbeef", 0, status, started_at, finished_at, error_type),
    )


# ---------------------------------------------------------------------------
# discover_source_batches.
# ---------------------------------------------------------------------------


def test_discover_source_batches_returns_empty_when_dir_missing(tmp_path: Path) -> None:
    assert discover_source_batches(tmp_path / "missing") == []


def test_discover_source_batches_returns_empty_when_dir_empty(tmp_path: Path) -> None:
    (tmp_path / "raw").mkdir()
    assert discover_source_batches(tmp_path / "raw") == []


def test_discover_source_batches_lists_each_parquet_with_identity(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    a = raw / "a.parquet"
    b = raw / "b.parquet"
    _write_source_parquet(a, marker="aaa")
    _write_source_parquet(b, marker="bbb")
    pairs = discover_source_batches(raw)
    ids = {batch_id for batch_id, _ in pairs}
    assert ids == {compute_batch_identity(a).batch_id, compute_batch_identity(b).batch_id}


def test_discover_source_batches_sorted_by_batch_id(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    for marker in ["zzz", "aaa", "mmm"]:
        _write_source_parquet(raw / f"{marker}.parquet", marker=marker)
    pairs = discover_source_batches(raw)
    ids = [batch_id for batch_id, _ in pairs]
    assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# is_pending.
# ---------------------------------------------------------------------------


_NOW = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


def _row(*, status: str, started_iso: str = "2026-04-25T12:00:00+00:00") -> BatchRow:
    return BatchRow(
        batch_id="bid01",
        source_path="src",
        source_hash="hash",
        source_mtime=0,
        status=status,
        rows_read=None,
        rows_written=None,
        bronze_path=None,
        started_at=started_iso,
        finished_at=None,
        duration_ms=None,
        error_type=None,
        error_message=None,
    )


def test_is_pending_treats_missing_row_as_pending() -> None:
    assert is_pending(None, now=_NOW, stale_after=timedelta(hours=1)) is True


def test_is_pending_treats_failed_as_pending() -> None:
    assert (
        is_pending(_row(status="FAILED"), now=_NOW, stale_after=timedelta(hours=1)) is True
    )


def test_is_pending_treats_completed_as_not_pending() -> None:
    assert (
        is_pending(_row(status="COMPLETED"), now=_NOW, stale_after=timedelta(hours=1))
        is False
    )


_ONE_HOUR = timedelta(hours=1)


def test_is_pending_treats_fresh_in_progress_as_not_pending() -> None:
    """Healthy active processing — leave alone."""
    started = (_NOW - timedelta(minutes=5)).isoformat()
    row = _row(status="IN_PROGRESS", started_iso=started)
    assert is_pending(row, now=_NOW, stale_after=_ONE_HOUR) is False


def test_is_pending_treats_stale_in_progress_as_pending() -> None:
    """Crash debris — re-schedule."""
    started = (_NOW - timedelta(hours=2)).isoformat()
    row = _row(status="IN_PROGRESS", started_iso=started)
    assert is_pending(row, now=_NOW, stale_after=_ONE_HOUR) is True


def test_is_pending_treats_unparseable_started_at_as_stale() -> None:
    """Defensive: corrupt timestamp must surface to humans, not
    wedge the loop forever."""
    row = _row(status="IN_PROGRESS", started_iso="not-a-timestamp")
    assert is_pending(row, now=_NOW, stale_after=_ONE_HOUR) is True


def test_is_pending_treats_naive_started_at_as_utc() -> None:
    """Backward-compat: pre-tz-aware rows in the manifest must still
    be classifiable. Default to UTC."""
    started = (_NOW - timedelta(minutes=5)).replace(tzinfo=None).isoformat()
    row = _row(status="IN_PROGRESS", started_iso=started)
    assert is_pending(row, now=_NOW, stale_after=_ONE_HOUR) is False


# ---------------------------------------------------------------------------
# scan end-to-end.
# ---------------------------------------------------------------------------


def test_scan_returns_empty_when_source_dir_missing(db: ManifestDB, tmp_path: Path) -> None:
    assert scan(db, tmp_path / "missing") == []


def test_scan_includes_batches_missing_from_manifest(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    _write_source_parquet(raw / "a.parquet", marker="a")
    pending = scan(db, raw)
    assert len(pending) == 1


def test_scan_skips_completed_batches(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    src = raw / "a.parquet"
    _write_source_parquet(src, marker="a")
    batch_id = compute_batch_identity(src).batch_id
    _insert_batch(db, batch_id=batch_id, status="COMPLETED", started_at="2026-04-25T11:00:00+00:00")
    assert scan(db, raw) == []


def test_scan_includes_failed_batches(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    src = raw / "a.parquet"
    _write_source_parquet(src, marker="a")
    batch_id = compute_batch_identity(src).batch_id
    _insert_batch(
        db,
        batch_id=batch_id,
        status="FAILED",
        started_at="2026-04-25T11:00:00+00:00",
        error_type="ConnectionError",
    )
    assert scan(db, raw) == [batch_id]


def test_scan_skips_fresh_in_progress_batches(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    src = raw / "a.parquet"
    _write_source_parquet(src, marker="a")
    batch_id = compute_batch_identity(src).batch_id
    _insert_batch(
        db,
        batch_id=batch_id,
        status="IN_PROGRESS",
        started_at="2026-04-25T11:55:00+00:00",
    )
    assert scan(db, raw, now=_NOW) == []


def test_scan_includes_stale_in_progress_batches(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    src = raw / "a.parquet"
    _write_source_parquet(src, marker="a")
    batch_id = compute_batch_identity(src).batch_id
    _insert_batch(
        db,
        batch_id=batch_id,
        status="IN_PROGRESS",
        started_at="2026-04-25T09:00:00+00:00",  # 3h ago
    )
    assert scan(db, raw, now=_NOW) == [batch_id]


def test_scan_returns_sorted_pending_ids(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    for marker in ["zzz", "aaa", "mmm"]:
        _write_source_parquet(raw / f"{marker}.parquet", marker=marker)
    pending = scan(db, raw)
    assert pending == sorted(pending)


def test_default_stale_after_matches_design() -> None:
    """1h default per design §4 — pin so a reduction surfaces in CI."""
    assert DEFAULT_STALE_AFTER_S == 3600.0
