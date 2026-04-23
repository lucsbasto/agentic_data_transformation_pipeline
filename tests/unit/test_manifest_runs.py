"""Tests for ``pipeline.state.manifest`` run-level helpers.

These cover the ``runs`` table extension done for F2 (reusing the F1
table rather than creating a new one): ``output_path`` / ``rows_deduped``
columns, layer-parameterized CRUD helpers, cascade from parent
``batches`` rows, and the stale-IN_PROGRESS sweep at startup.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from pipeline.errors import ManifestError
from pipeline.schemas.manifest import (
    BATCHES_DDL,
    LLM_CACHE_DDL,
    RUN_LAYER_GOLD,
    RUN_LAYER_SILVER,
)
from pipeline.state.manifest import ManifestDB, RunRow


@pytest.fixture
def db() -> Iterator[ManifestDB]:
    with ManifestDB(":memory:") as manifest:
        yield manifest


def _insert_batch(db: ManifestDB, batch_id: str = "b1") -> None:
    db.insert_batch(
        batch_id=batch_id,
        source_path="/tmp/src.parquet",
        source_hash="deadbeef",
        source_mtime=1_700_000_000,
        started_at="2026-04-22T12:00:00Z",
    )


def _start_silver_run(
    db: ManifestDB,
    *,
    run_id: str = "r1",
    batch_id: str = "b1",
    started_at: str = "2026-04-22T12:00:01Z",
) -> None:
    db.insert_run(
        run_id=run_id,
        batch_id=batch_id,
        layer=RUN_LAYER_SILVER,
        started_at=started_at,
    )


# ---------------------------------------------------------------------------
# Migration — ALTER TABLE idempotency on a legacy F1 database.
# ---------------------------------------------------------------------------


def test_migration_adds_missing_columns_to_existing_f1_db(tmp_path: Path) -> None:
    """Simulate an F1-era DB (no ``output_path`` / ``rows_deduped``) and
    confirm ``ensure_schema`` adds the missing columns idempotently.
    """
    db_path = tmp_path / "legacy.db"

    # Build the legacy ``runs`` shape by hand — exactly what F1 shipped
    # before F2. No ``output_path``, no ``rows_deduped``, no duration_ms.
    conn = sqlite3.connect(db_path)
    conn.execute(BATCHES_DDL)
    conn.execute(
        """
        CREATE TABLE runs (
            run_id        TEXT PRIMARY KEY,
            batch_id      TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
            layer         TEXT NOT NULL CHECK (layer IN ('bronze','silver','gold')),
            status        TEXT NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED','FAILED')),
            started_at    TEXT NOT NULL,
            finished_at   TEXT,
            rows_in       INTEGER,
            rows_out      INTEGER,
            error_type    TEXT,
            error_message TEXT
        );
        """
    )
    conn.execute(LLM_CACHE_DDL)
    conn.commit()
    legacy_cols = {row[1] for row in conn.execute("PRAGMA table_info(runs);")}
    conn.close()
    assert "output_path" not in legacy_cols
    assert "rows_deduped" not in legacy_cols
    assert "duration_ms" not in legacy_cols

    # Open with the F2 ManifestDB → migration runs.
    with ManifestDB(db_path) as db:
        conn2 = db._require_conn()  # type: ignore[attr-defined]
        cols = {row[1] for row in conn2.execute("PRAGMA table_info(runs);")}
        assert "output_path" in cols
        assert "rows_deduped" in cols
        assert "duration_ms" in cols

    # Re-opening must be a no-op (the migration guard prevents double-add).
    with ManifestDB(db_path):
        pass


# ---------------------------------------------------------------------------
# Insert + read + happy path
# ---------------------------------------------------------------------------


def test_insert_run_and_read_back(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db)
    row = db.get_run("r1")
    assert row is not None
    assert isinstance(row, RunRow)
    assert row.run_id == "r1"
    assert row.batch_id == "b1"
    assert row.layer == RUN_LAYER_SILVER
    assert row.status == "IN_PROGRESS"
    assert row.is_completed is False
    assert row.is_failed is False
    assert row.output_path is None
    assert row.rows_deduped is None


def test_insert_run_rejects_invalid_layer(db: ManifestDB) -> None:
    _insert_batch(db)
    with pytest.raises(ManifestError, match="invalid run layer"):
        db.insert_run(
            run_id="r1",
            batch_id="b1",
            layer="platinum",
            started_at="2026-04-22T12:00:01Z",
        )


def test_insert_run_rejects_missing_parent_batch(db: ManifestDB) -> None:
    # Silver run referencing a batch that was never inserted → FK error.
    with pytest.raises(ManifestError, match="failed to insert run"):
        _start_silver_run(db, batch_id="does-not-exist")


def test_insert_duplicate_run_id_raises(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db)
    with pytest.raises(ManifestError, match="failed to insert run"):
        _start_silver_run(db)


def test_mark_run_completed_records_silver_metrics(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db)
    db.mark_run_completed(
        run_id="r1",
        finished_at="2026-04-22T12:00:03Z",
        duration_ms=2100,
        rows_in=200,
        rows_out=150,
        rows_deduped=50,
        output_path="data/silver/batch_id=b1",
    )
    row = db.get_run("r1")
    assert row is not None
    assert row.is_completed is True
    assert row.duration_ms == 2100
    assert row.rows_in == 200
    assert row.rows_out == 150
    assert row.rows_deduped == 50
    assert row.output_path == "data/silver/batch_id=b1"
    # Completing clears any prior error fields.
    assert row.error_type is None
    assert row.error_message is None


def test_mark_run_failed_keeps_error_fields(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db)
    db.mark_run_failed(
        run_id="r1",
        finished_at="2026-04-22T12:00:02Z",
        duration_ms=900,
        error_type="ValueError",
        error_message="bad cast",
    )
    row = db.get_run("r1")
    assert row is not None
    assert row.is_failed is True
    assert row.error_type == "ValueError"
    assert row.error_message == "bad cast"


def test_mark_run_completed_requires_existing_run(db: ManifestDB) -> None:
    with pytest.raises(ManifestError, match="no run row to update"):
        db.mark_run_completed(
            run_id="ghost",
            finished_at="2026-04-22T12:00:03Z",
            duration_ms=100,
        )


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def test_is_run_completed_matches_status(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db)
    assert db.is_run_completed(batch_id="b1", layer=RUN_LAYER_SILVER) is False
    db.mark_run_completed(
        run_id="r1",
        finished_at="2026-04-22T12:00:03Z",
        duration_ms=500,
        rows_out=10,
    )
    assert db.is_run_completed(batch_id="b1", layer=RUN_LAYER_SILVER) is True
    # A different layer stays False — layer is part of the identity.
    assert db.is_run_completed(batch_id="b1", layer=RUN_LAYER_GOLD) is False


def test_get_latest_run_orders_by_started_at(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db, run_id="r1", started_at="2026-04-22T12:00:01Z")
    _start_silver_run(db, run_id="r2", started_at="2026-04-22T12:00:05Z")
    _start_silver_run(db, run_id="r3", started_at="2026-04-22T12:00:03Z")
    latest = db.get_latest_run(batch_id="b1", layer=RUN_LAYER_SILVER)
    assert latest is not None
    assert latest.run_id == "r2"


def test_get_latest_run_returns_none_when_empty(db: ManifestDB) -> None:
    _insert_batch(db)
    assert db.get_latest_run(batch_id="b1", layer=RUN_LAYER_SILVER) is None


# ---------------------------------------------------------------------------
# Delete + stale sweep
# ---------------------------------------------------------------------------


def test_delete_runs_for_removes_only_target_layer(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db, run_id="r_silver")
    db.insert_run(
        run_id="r_gold",
        batch_id="b1",
        layer=RUN_LAYER_GOLD,
        started_at="2026-04-22T12:00:02Z",
    )
    deleted = db.delete_runs_for(batch_id="b1", layer=RUN_LAYER_SILVER)
    assert deleted == 1
    assert db.get_run("r_silver") is None
    assert db.get_run("r_gold") is not None


def test_reset_stale_runs_sweeps_in_progress(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db)
    swept = db.reset_stale_runs(now_iso="2026-04-22T23:59:59Z")
    assert swept == 1
    row = db.get_run("r1")
    assert row is not None
    assert row.is_failed is True
    assert row.error_type == "StaleInProgress"
    assert row.finished_at == "2026-04-22T23:59:59Z"


def test_reset_stale_runs_layer_filter(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db, run_id="r_silver")
    db.insert_run(
        run_id="r_gold",
        batch_id="b1",
        layer=RUN_LAYER_GOLD,
        started_at="2026-04-22T12:00:02Z",
    )
    swept = db.reset_stale_runs(layer=RUN_LAYER_SILVER)
    assert swept == 1
    assert db.get_run("r_silver").is_failed is True  # type: ignore[union-attr]
    assert db.get_run("r_gold").status == "IN_PROGRESS"  # type: ignore[union-attr]


def test_reset_stale_runs_rejects_invalid_layer(db: ManifestDB) -> None:
    with pytest.raises(ManifestError, match="invalid run layer"):
        db.reset_stale_runs(layer="platinum")


# ---------------------------------------------------------------------------
# Cascade from the parent ``batches`` row.
# ---------------------------------------------------------------------------


def test_runs_cascade_when_parent_batch_deleted(db: ManifestDB) -> None:
    _insert_batch(db)
    _start_silver_run(db)
    assert db.delete_batch("b1") is True
    # Run row disappears because of ON DELETE CASCADE on the FK.
    assert db.get_run("r1") is None
