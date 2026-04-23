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
from pathlib import Path

from pipeline.errors import ManifestError
from pipeline.schemas.manifest import (
    ALL_DDL,
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_FAILED,
    BATCH_STATUS_IN_PROGRESS,
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
        conn.execute("PRAGMA journal_mode = WAL;") if target != ":memory:" else None
        self._conn = conn
        self.ensure_schema()
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
        conn = self._require_conn()
        with self._transaction() as cur:
            for stmt in ALL_DDL:
                cur.execute(stmt)
        del conn  # silence unused-warning without suppressing in linters

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
        self._update_status(
            batch_id=batch_id,
            status=BATCH_STATUS_COMPLETED,
            finished_at=finished_at,
            duration_ms=duration_ms,
            rows_read=rows_read,
            rows_written=rows_written,
            bronze_path=bronze_path,
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
        return BatchRow(**{k: row[k] for k in row.keys()})

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
    ) -> None:
        with self._transaction() as cur:
            cur.execute(
                "UPDATE batches SET status = ?, finished_at = ?, duration_ms = ?, "
                "rows_read = COALESCE(?, rows_read), "
                "rows_written = COALESCE(?, rows_written), "
                "bronze_path = COALESCE(?, bronze_path), "
                "error_type = COALESCE(?, error_type), "
                "error_message = COALESCE(?, error_message) "
                "WHERE batch_id = ?;",
                (
                    status,
                    finished_at,
                    duration_ms,
                    rows_read,
                    rows_written,
                    bronze_path,
                    error_type,
                    error_message,
                    batch_id,
                ),
            )
            if cur.rowcount == 0:
                raise ManifestError(f"no batch row to update: {batch_id!r}")
