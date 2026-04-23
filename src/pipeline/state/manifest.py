"""SQLite-backed manifest for batch + run tracking.

The class is deliberately thin: it opens a connection, applies the DDL,
and exposes a minimum surface for the Bronze ingest (F1). Silver and Gold
runs plus the agent loop will reuse the same file in later features.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pipeline.errors import ManifestError
from pipeline.schemas.manifest import (
    ALL_DDL,
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_FAILED,
    BATCH_STATUS_IN_PROGRESS,
    BATCH_STATUSES,
)

STALE_ERROR_TYPE: str = "StaleInProgress"
"""``error_type`` used when crash recovery marks an orphan batch FAILED."""

STALE_ERROR_MESSAGE: str = (
    "previous process exited without completing this batch"
)


@dataclass(frozen=True, slots=True)
class BatchRow:
    """Typed projection of one row from the ``batches`` table."""

    batch_id: str
    source_path: str
    source_hash: str
    source_mtime: int
    status: str
    rows_read: int | None
    rows_written: int | None
    bronze_path: str | None
    started_at: str
    finished_at: str | None
    duration_ms: int | None
    error_type: str | None
    error_message: str | None

    @property
    def is_completed(self) -> bool:
        return self.status == BATCH_STATUS_COMPLETED

    @property
    def is_failed(self) -> bool:
        return self.status == BATCH_STATUS_FAILED


class ManifestDB:
    """Wrapper around a SQLite manifest file.

    The connection is opened on ``__enter__`` and closed on ``__exit__``;
    use the class as a context manager in entrypoints. Direct instantiation
    is also supported for tests (call :meth:`close` when done).
    """

    def __init__(self, db_path: Path | str) -> None:
        self._db_path: Path | str = db_path if db_path == ":memory:" else Path(db_path)
        self._conn: sqlite3.Connection | None = None

    # ------------------------------------------------------------------ lifecycle

    def open(self) -> ManifestDB:
        """Open (or reopen) the connection and ensure the schema."""
        if self._conn is not None:
            return self
        if isinstance(self._db_path, Path):
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            target: str = str(self._db_path)
        else:
            target = self._db_path
        conn = sqlite3.connect(target, isolation_level=None)  # autocommit off via BEGIN
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        if target != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL;")
        self._conn = conn
        self.ensure_schema()
        self.reset_stale()
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> ManifestDB:
        return self.open()

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ schema

    def ensure_schema(self) -> None:
        with self._transaction() as cur:
            for stmt in ALL_DDL:
                cur.execute(stmt)

    def reset_stale(self, *, now_iso: str | None = None) -> int:
        """Mark orphaned ``IN_PROGRESS`` batches as ``FAILED``.

        A new process starting up means any ``IN_PROGRESS`` row is from a
        previous process that crashed. Without this sweep, re-running the
        same batch hits the primary-key constraint on ``batches`` and
        surfaces as a cryptic ``IntegrityError`` instead of a clean retry.

        Returns the number of rows swept. ``now_iso`` is injectable so tests
        can assert a deterministic timestamp.
        """
        finished_at = now_iso if now_iso is not None else _utcnow_iso()
        with self._transaction() as cur:
            cur.execute(
                "UPDATE batches SET status = ?, finished_at = ?, "
                "error_type = ?, error_message = ? "
                "WHERE status = ?;",
                (
                    BATCH_STATUS_FAILED,
                    finished_at,
                    STALE_ERROR_TYPE,
                    STALE_ERROR_MESSAGE,
                    BATCH_STATUS_IN_PROGRESS,
                ),
            )
            return cur.rowcount

    # ------------------------------------------------------------------ writes

    def insert_batch(
        self,
        *,
        batch_id: str,
        source_path: str,
        source_hash: str,
        source_mtime: int,
        started_at: str,
    ) -> None:
        """Create a new batch row in ``IN_PROGRESS`` state."""
        with self._transaction() as cur:
            try:
                cur.execute(
                    "INSERT INTO batches (batch_id, source_path, source_hash, "
                    "source_mtime, status, started_at) "
                    "VALUES (?, ?, ?, ?, ?, ?);",
                    (
                        batch_id,
                        source_path,
                        source_hash,
                        source_mtime,
                        BATCH_STATUS_IN_PROGRESS,
                        started_at,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ManifestError(
                    f"batch_id already exists: {batch_id!r}"
                ) from exc

    def mark_completed(
        self,
        *,
        batch_id: str,
        rows_read: int,
        rows_written: int,
        bronze_path: str,
        finished_at: str,
        duration_ms: int,
    ) -> None:
        """Mark a batch COMPLETED and clear any error fields from prior attempts."""
        self._update_status(
            batch_id=batch_id,
            status=BATCH_STATUS_COMPLETED,
            finished_at=finished_at,
            duration_ms=duration_ms,
            rows_read=rows_read,
            rows_written=rows_written,
            bronze_path=bronze_path,
            error_type=None,
            error_message=None,
            clear_error_fields=True,
        )

    def mark_failed(
        self,
        *,
        batch_id: str,
        finished_at: str,
        duration_ms: int,
        error_type: str,
        error_message: str,
    ) -> None:
        self._update_status(
            batch_id=batch_id,
            status=BATCH_STATUS_FAILED,
            finished_at=finished_at,
            duration_ms=duration_ms,
            error_type=error_type,
            error_message=error_message,
            clear_error_fields=False,
        )

    # ------------------------------------------------------------------ reads

    def get_batch(self, batch_id: str) -> BatchRow | None:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT batch_id, source_path, source_hash, source_mtime, status, "
            "rows_read, rows_written, bronze_path, started_at, finished_at, "
            "duration_ms, error_type, error_message "
            "FROM batches WHERE batch_id = ?;",
            (batch_id,),
        ).fetchone()
        if row is None:
            return None
        return BatchRow(**dict(row))

    def is_batch_completed(self, batch_id: str) -> bool:
        batch = self.get_batch(batch_id)
        return batch is not None and batch.is_completed

    # ------------------------------------------------------------------ internals

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise ManifestError(
                "ManifestDB connection is not open; use as a context manager "
                "or call .open() first."
            )
        return self._conn

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Cursor]:
        conn = self._require_conn()
        cur = conn.cursor()
        cur.execute("BEGIN;")
        try:
            yield cur
        except Exception:
            cur.execute("ROLLBACK;")
            raise
        else:
            cur.execute("COMMIT;")
        finally:
            cur.close()

    def _update_status(
        self,
        *,
        batch_id: str,
        status: str,
        finished_at: str,
        duration_ms: int,
        rows_read: int | None = None,
        rows_written: int | None = None,
        bronze_path: str | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        clear_error_fields: bool = False,
    ) -> None:
        if status not in BATCH_STATUSES:
            raise ManifestError(
                f"invalid status {status!r}; expected one of {BATCH_STATUSES}"
            )
        # Error columns:
        #   clear_error_fields=True  (used by mark_completed) -> always set to NULL
        #     so a prior FAILED attempt does not leak errors into a COMPLETED row.
        #   clear_error_fields=False (used by mark_failed)    -> assign the values.
        # Lineage columns (rows_read/written, bronze_path) stay COALESCE so that a
        # later partial update never erases progress recorded earlier in the run.
        if clear_error_fields:
            error_clause = "error_type = NULL, error_message = NULL"
            error_params: tuple[object, ...] = ()
        else:
            error_clause = "error_type = ?, error_message = ?"
            error_params = (error_type, error_message)

        sql = (
            "UPDATE batches SET status = ?, finished_at = ?, duration_ms = ?, "
            "rows_read = COALESCE(?, rows_read), "
            "rows_written = COALESCE(?, rows_written), "
            "bronze_path = COALESCE(?, bronze_path), "
            f"{error_clause} "
            "WHERE batch_id = ?;"
        )
        params: tuple[object, ...] = (
            status,
            finished_at,
            duration_ms,
            rows_read,
            rows_written,
            bronze_path,
            *error_params,
            batch_id,
        )

        with self._transaction() as cur:
            try:
                cur.execute(sql, params)
            except sqlite3.IntegrityError as exc:
                raise ManifestError(
                    f"integrity error updating batch {batch_id!r}: {exc}"
                ) from exc
            if cur.rowcount == 0:
                raise ManifestError(f"no batch row to update: {batch_id!r}")


def _utcnow_iso() -> str:
    """Return the current UTC timestamp as an ISO-8601 string (seconds resolution)."""
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
