"""SQLite-backed manifest for batch + run tracking.

The class is deliberately thin: it opens a connection, applies the DDL,
and exposes a minimum surface for the Bronze ingest (F1). Silver and Gold
runs plus the agent loop will reuse the same file in later features.
"""

from __future__ import annotations

# LEARN: the stdlib imports below. Worth knowing what each one gives us:
#   - ``sqlite3``       — Python's built-in SQL engine. Embedded, no
#                         server; perfect for a single-operator pipeline.
#   - ``Iterator``      — static type for "yields values one at a time";
#                         annotates the ``_transaction`` generator below.
#   - ``contextmanager`` — decorator that turns a ``yield``-using function
#                         into something you can ``with``-block over.
#                         Everything BEFORE ``yield`` runs on enter, and
#                         AFTER ``yield`` runs on exit (including on
#                         exceptions). Classic acquire/release pattern.
#   - ``dataclass``     — auto-generates ``__init__`` / ``__repr__`` /
#                         ``__eq__`` from type-annotated attributes. With
#                         ``frozen=True`` + ``slots=True`` we get
#                         immutable, memory-light value objects.
#   - ``datetime`` + ``UTC`` — build timezone-aware timestamps.
#   - ``pathlib.Path`` — object-oriented filesystem paths.
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
    RUN_LAYERS,
    RUN_STATUS_COMPLETED,
    RUN_STATUS_FAILED,
    RUN_STATUS_IN_PROGRESS,
    RUN_STATUSES,
    RUNS_MIGRATIONS,
)

STALE_ERROR_TYPE: str = "StaleInProgress"
"""``error_type`` used when crash recovery marks an orphan batch FAILED."""

STALE_ERROR_MESSAGE: str = (
    "previous process exited without completing this batch"
)


# LEARN: ``@dataclass(frozen=True, slots=True)`` = a value object. The
# attributes declared below become immutable once the instance is built.
# The type hints drive the auto-generated ``__init__`` — ``BatchRow(...)``
# only accepts the keyword names listed here.
@dataclass(frozen=True, slots=True)
class BatchRow:
    """Typed projection of one row from the ``batches`` table."""

    batch_id: str
    source_path: str
    source_hash: str
    source_mtime: int
    status: str
    # LEARN: ``int | None`` means "either int or None". The Python 3.10+
    # pipe syntax replaces ``Optional[int]``.
    rows_read: int | None
    rows_written: int | None
    bronze_path: str | None
    started_at: str
    finished_at: str | None
    duration_ms: int | None
    error_type: str | None
    error_message: str | None

    # LEARN: ``@property`` turns a method into a read-only attribute:
    # ``row.is_completed`` not ``row.is_completed()``. Great for tiny
    # derived booleans that keep call sites clean.
    @property
    def is_completed(self) -> bool:
        return self.status == BATCH_STATUS_COMPLETED

    @property
    def is_failed(self) -> bool:
        return self.status == BATCH_STATUS_FAILED


# LEARN: ``RunRow`` mirrors the ``runs`` table introduced for F1 and
# grown for F2. One row per attempt at a layer transform (Bronze ingest,
# Silver build, Gold build). Silver populates ``rows_deduped`` and
# ``output_path``; Bronze leaves them null (its output path lives on
# ``batches.bronze_path``).
@dataclass(frozen=True, slots=True)
class RunRow:
    """Typed projection of one row from the ``runs`` table."""

    run_id: str
    batch_id: str
    layer: str
    status: str
    started_at: str
    finished_at: str | None
    duration_ms: int | None
    rows_in: int | None
    rows_out: int | None
    rows_deduped: int | None
    rows_rejected: int | None
    output_path: str | None
    error_type: str | None
    error_message: str | None

    @property
    def is_completed(self) -> bool:
        return self.status == RUN_STATUS_COMPLETED

    @property
    def is_failed(self) -> bool:
        return self.status == RUN_STATUS_FAILED


class ManifestDB:
    """Wrapper around a SQLite manifest file.

    The connection is opened on ``__enter__`` and closed on ``__exit__``;
    use the class as a context manager in entrypoints. Direct instantiation
    is also supported for tests (call :meth:`close` when done).
    """

    def __init__(self, db_path: Path | str) -> None:
        # LEARN: the class stores ``db_path`` as a Path (for real files)
        # or a string (for the in-memory ``:memory:`` case). We check
        # with ``==`` rather than ``isinstance`` because ``":memory:"``
        # is a magic SQLite path, not a real filesystem entry.
        self._db_path: Path | str = db_path if db_path == ":memory:" else Path(db_path)
        # LEARN: the connection starts ``None`` and is created in
        # ``open()``. Keeping it Optional lets ``close()`` / ``reopen``
        # work cleanly without an "is it open?" boolean.
        self._conn: sqlite3.Connection | None = None

    # ------------------------------------------------------------------ lifecycle

    def open(self) -> ManifestDB:
        """Open (or reopen) the connection and ensure the schema."""
        # LEARN: ``is not None`` checks identity, not truthiness. An empty
        # list is falsy but ``[] is None`` is False — always use ``is
        # None`` / ``is not None`` when checking for None.
        if self._conn is not None:
            return self
        if isinstance(self._db_path, Path):
            # LEARN: ``.parent`` is the directory that contains the file.
            # ``mkdir(parents=True, exist_ok=True)`` = "create any missing
            # parent dirs; don't error if the directory already exists".
            # The classic ``mkdir -p`` behaviour.
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            target: str = str(self._db_path)
        else:
            target = self._db_path
        # LEARN: ``isolation_level=None`` turns OFF sqlite3's auto-commit
        # wrapping. We issue BEGIN / COMMIT / ROLLBACK manually in
        # ``_transaction`` below, which gives us precise control.
        conn = sqlite3.connect(target, isolation_level=None)  # autocommit off via BEGIN
        # LEARN: ``sqlite3.Row`` makes fetched rows addressable by
        # column name (``row["batch_id"]``) in addition to index
        # (``row[0]``). We convert to dataclasses downstream.
        conn.row_factory = sqlite3.Row
        # LEARN: SQLite's foreign-key enforcement is OFF by default for
        # backward compatibility. We turn it on so ``ON DELETE CASCADE``
        # from ``schemas/manifest.py`` actually fires.
        conn.execute("PRAGMA foreign_keys = ON;")
        # LEARN: ``busy_timeout`` makes SQLite wait up to 5 s when
        # another connection holds a lock, instead of failing with
        # SQLITE_BUSY immediately. Critical under concurrent writers.
        conn.execute("PRAGMA busy_timeout = 5000;")
        if target != ":memory:":
            # LEARN: WAL = "Write-Ahead Logging". A journaling mode that
            # lets one writer and many readers operate without blocking
            # each other. Does not work on ``:memory:`` databases.
            conn.execute("PRAGMA journal_mode = WAL;")
        self._conn = conn
        self.ensure_schema()
        # LEARN: on every open, sweep leftover IN_PROGRESS rows from a
        # previous crashed process to FAILED. Without this, a Ctrl-C at
        # the wrong moment would block future runs from reusing that
        # batch_id (primary-key collision).
        self.reset_stale()
        # LEARN: the same idea applies one level deeper: ``runs`` rows
        # stuck in IN_PROGRESS belong to a crashed Silver/Gold run. We
        # flip them to FAILED so the retry path in
        # ``cli/silver.py`` -> ``delete_runs_for`` does not surprise
        # operators with a dangling "half-done" row.
        self.reset_stale_runs()
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # LEARN: ``__enter__`` / ``__exit__`` are the *context-manager
    # protocol*. They let callers write ``with ManifestDB(path) as db:``
    # and Python guarantees ``__exit__`` (which closes the connection)
    # runs even if the ``with`` body raises.
    def __enter__(self) -> ManifestDB:
        return self.open()

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ schema

    def ensure_schema(self) -> None:
        with self._transaction() as cur:
            # LEARN: ``ALL_DDL`` is the tuple of DDL strings from
            # ``schemas/manifest.py``. Each ``IF NOT EXISTS`` makes the
            # call idempotent — safe to run on every open.
            for stmt in ALL_DDL:
                cur.execute(stmt)
            # LEARN: SQLite has no ``ALTER TABLE ... ADD COLUMN IF NOT
            # EXISTS``, so we diff by hand. ``PRAGMA table_info(runs)``
            # returns one row per existing column (name at index 1). We
            # build a set of current names, then run only the ``ADD
            # COLUMN`` statements whose target column is missing. Safe
            # on fresh DBs (CREATE TABLE already has the columns → no
            # migration fires) and on F1 DBs (CREATE is no-op, ALTER
            # fires for every missing column).
            existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(runs);")}
            for column_name, alter_sql in RUNS_MIGRATIONS:
                if column_name not in existing_cols:
                    cur.execute(alter_sql)

    def reset_stale(self, *, now_iso: str | None = None) -> int:
        """Mark orphaned ``IN_PROGRESS`` batches as ``FAILED``.

        A new process starting up means any ``IN_PROGRESS`` row is from a
        previous process that crashed. Without this sweep, re-running the
        same batch hits the primary-key constraint on ``batches`` and
        surfaces as a cryptic ``IntegrityError`` instead of a clean retry.

        Returns the number of rows swept. ``now_iso`` is injectable so tests
        can assert a deterministic timestamp.
        """
        # LEARN: the ``*,`` in the signature forces ``now_iso`` to be
        # passed as a keyword argument. Prevents ``reset_stale("...")``
        # callers from accidentally binding a positional value.
        finished_at = now_iso if now_iso is not None else _utcnow_iso()
        with self._transaction() as cur:
            # LEARN: ``?`` placeholders are SQLite's SQL-injection guard.
            # NEVER interpolate user data into SQL with f-strings — the
            # tuple argument binds values safely.
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
            # LEARN: ``cursor.rowcount`` reports how many rows the last
            # statement affected. Handy for "swept N stale batches" logs.
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
                # LEARN: we translate the low-level sqlite3 error into
                # our domain error (ManifestError). Callers handle ONE
                # hierarchy (PipelineError) and never need to know
                # sqlite3 exists. The ``from exc`` preserves the
                # original traceback for debugging.
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
        # LEARN: ``execute(...).fetchone()`` runs the query and pulls
        # one row. Returns ``None`` if the row does not exist.
        row = conn.execute(
            "SELECT batch_id, source_path, source_hash, source_mtime, status, "
            "rows_read, rows_written, bronze_path, started_at, finished_at, "
            "duration_ms, error_type, error_message "
            "FROM batches WHERE batch_id = ?;",
            (batch_id,),
        ).fetchone()
        if row is None:
            return None
        # LEARN: ``BatchRow(**dict(row))`` is the "explode a dict into
        # keyword arguments" idiom. ``dict(row)`` converts the
        # ``sqlite3.Row`` to a real dict; ``**`` unpacks it so each key
        # becomes a keyword argument to ``BatchRow``.
        return BatchRow(**dict(row))

    def is_batch_completed(self, batch_id: str) -> bool:
        batch = self.get_batch(batch_id)
        # LEARN: short-circuit boolean — if ``batch is None``, the
        # second expression is never evaluated (avoids AttributeError).
        return batch is not None and batch.is_completed

    def delete_batch(self, batch_id: str) -> bool:
        """Remove a batch row unconditionally. Returns True if a row was deleted.

        This exists for the retry-after-FAILED path: ``reset_stale`` only
        sweeps ``IN_PROGRESS`` rows, so a batch that already has a FAILED
        row would collide on the primary key when a retry inserts again.
        Callers who *know* they want to restart that specific batch call
        this helper explicitly; automatic retry is not implied.
        """
        with self._transaction() as cur:
            cur.execute("DELETE FROM batches WHERE batch_id = ?;", (batch_id,))
            return cur.rowcount > 0

    # ------------------------------------------------------------------ runs

    def reset_stale_runs(
        self,
        *,
        now_iso: str | None = None,
        layer: str | None = None,
    ) -> int:
        """Mark orphaned ``IN_PROGRESS`` runs as ``FAILED``.

        Mirror of :meth:`reset_stale` but for the ``runs`` table. Called
        from :meth:`open` on every startup. ``layer`` narrows the sweep
        to one layer when set; ``None`` sweeps all layers.
        """
        if layer is not None:
            self._require_layer(layer)
        finished_at = now_iso if now_iso is not None else _utcnow_iso()
        with self._transaction() as cur:
            if layer is None:
                cur.execute(
                    "UPDATE runs SET status = ?, finished_at = ?, "
                    "error_type = ?, error_message = ? "
                    "WHERE status = ?;",
                    (
                        RUN_STATUS_FAILED,
                        finished_at,
                        STALE_ERROR_TYPE,
                        STALE_ERROR_MESSAGE,
                        RUN_STATUS_IN_PROGRESS,
                    ),
                )
            else:
                cur.execute(
                    "UPDATE runs SET status = ?, finished_at = ?, "
                    "error_type = ?, error_message = ? "
                    "WHERE status = ? AND layer = ?;",
                    (
                        RUN_STATUS_FAILED,
                        finished_at,
                        STALE_ERROR_TYPE,
                        STALE_ERROR_MESSAGE,
                        RUN_STATUS_IN_PROGRESS,
                        layer,
                    ),
                )
            return cur.rowcount

    def insert_run(
        self,
        *,
        run_id: str,
        batch_id: str,
        layer: str,
        started_at: str,
    ) -> None:
        """Create a new run row in ``IN_PROGRESS`` state.

        ``run_id`` is caller-generated (typically the same short UUID
        that shows up in structlog events) — this lets the CLI log lines
        and the DB row share a single correlation id.
        """
        self._require_layer(layer)
        with self._transaction() as cur:
            try:
                cur.execute(
                    "INSERT INTO runs (run_id, batch_id, layer, status, started_at) "
                    "VALUES (?, ?, ?, ?, ?);",
                    (run_id, batch_id, layer, RUN_STATUS_IN_PROGRESS, started_at),
                )
            except sqlite3.IntegrityError as exc:
                # LEARN: a run_id collision OR a missing batch_id (FK
                # violation) both land here. We translate the raw
                # sqlite3 error into our domain error so callers handle
                # ONE hierarchy (PipelineError).
                raise ManifestError(
                    f"failed to insert run {run_id!r} for batch {batch_id!r}: {exc}"
                ) from exc

    def mark_run_completed(
        self,
        *,
        run_id: str,
        finished_at: str,
        duration_ms: int,
        rows_in: int | None = None,
        rows_out: int | None = None,
        rows_deduped: int | None = None,
        rows_rejected: int | None = None,
        output_path: str | None = None,
    ) -> None:
        """Flip a run to COMPLETED and record its metrics."""
        self._update_run_status(
            run_id=run_id,
            status=RUN_STATUS_COMPLETED,
            finished_at=finished_at,
            duration_ms=duration_ms,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_deduped=rows_deduped,
            rows_rejected=rows_rejected,
            output_path=output_path,
            error_type=None,
            error_message=None,
            clear_error_fields=True,
        )

    def mark_run_failed(
        self,
        *,
        run_id: str,
        finished_at: str,
        duration_ms: int,
        error_type: str,
        error_message: str,
    ) -> None:
        """Flip a run to FAILED and record the error cause."""
        self._update_run_status(
            run_id=run_id,
            status=RUN_STATUS_FAILED,
            finished_at=finished_at,
            duration_ms=duration_ms,
            error_type=error_type,
            error_message=error_message,
            clear_error_fields=False,
        )

    def get_run(self, run_id: str) -> RunRow | None:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT run_id, batch_id, layer, status, started_at, finished_at, "
            "duration_ms, rows_in, rows_out, rows_deduped, rows_rejected, "
            "output_path, error_type, error_message "
            "FROM runs WHERE run_id = ?;",
            (run_id,),
        ).fetchone()
        return None if row is None else RunRow(**dict(row))

    def get_latest_run(self, *, batch_id: str, layer: str) -> RunRow | None:
        """Return the most recently started run for ``(batch_id, layer)``.

        "Most recent" is by ``started_at`` DESC; ties break on ``rowid``
        so the answer is deterministic even when two runs share an ISO
        timestamp (seconds resolution).
        """
        self._require_layer(layer)
        conn = self._require_conn()
        row = conn.execute(
            "SELECT run_id, batch_id, layer, status, started_at, finished_at, "
            "duration_ms, rows_in, rows_out, rows_deduped, rows_rejected, "
            "output_path, error_type, error_message "
            "FROM runs WHERE batch_id = ? AND layer = ? "
            "ORDER BY started_at DESC, rowid DESC LIMIT 1;",
            (batch_id, layer),
        ).fetchone()
        return None if row is None else RunRow(**dict(row))

    def is_run_completed(self, *, batch_id: str, layer: str) -> bool:
        """True when ``(batch_id, layer)`` has at least one COMPLETED run."""
        self._require_layer(layer)
        conn = self._require_conn()
        row = conn.execute(
            "SELECT 1 FROM runs WHERE batch_id = ? AND layer = ? AND status = ? "
            "LIMIT 1;",
            (batch_id, layer, RUN_STATUS_COMPLETED),
        ).fetchone()
        return row is not None

    def delete_runs_for(self, *, batch_id: str, layer: str) -> int:
        """Delete every run row for ``(batch_id, layer)``. Returns the deleted count.

        Used by the retry path in Silver (and later Gold): stale
        ``IN_PROGRESS`` / ``FAILED`` rows are wiped before a new
        ``insert_run`` so the operator can simply re-run the CLI.
        """
        self._require_layer(layer)
        with self._transaction() as cur:
            cur.execute(
                "DELETE FROM runs WHERE batch_id = ? AND layer = ?;",
                (batch_id, layer),
            )
            return cur.rowcount

    # ------------------------------------------------------------------ internals

    def _require_conn(self) -> sqlite3.Connection:
        # LEARN: a "guard" method — call this before any read/write to
        # make the "DB is not open" mistake a loud error instead of an
        # obscure AttributeError down the line.
        if self._conn is None:
            raise ManifestError(
                "ManifestDB connection is not open; use as a context manager "
                "or call .open() first."
            )
        return self._conn

    # LEARN: ``@contextmanager`` + a generator function = a reusable
    # ``with`` block. Python enters the ``try`` before ``yield``,
    # returns the yielded value to the caller, then resumes AFTER
    # ``yield`` on exit (whether the caller raised or not).
    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Cursor]:
        conn = self._require_conn()
        cur = conn.cursor()
        cur.execute("BEGIN;")
        try:
            # LEARN: the value after ``yield`` becomes what the caller
            # receives via ``with ... as cur:``.
            yield cur
        except Exception:
            # LEARN: if the ``with`` body raised, roll the transaction
            # back so partial writes disappear. The bare ``raise``
            # re-raises the original exception with its traceback.
            cur.execute("ROLLBACK;")
            raise
        else:
            # LEARN: ``else`` on a try/except runs only when no
            # exception fired. We commit only on the happy path.
            cur.execute("COMMIT;")
        finally:
            # LEARN: ``finally`` always runs — closes the cursor no
            # matter what, preventing resource leaks.
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
            # LEARN: ``tuple[object, ...]`` types a variable-length tuple
            # of any types. Empty tuple ``()`` is valid.
            error_params: tuple[object, ...] = ()
        else:
            error_clause = "error_type = ?, error_message = ?"
            error_params = (error_type, error_message)

        # LEARN: Python implicitly concatenates adjacent string literals
        # inside parentheses (no ``+`` needed). ``f"..." "literal"``
        # works too. Keeps multi-line SQL readable.
        sql = (
            "UPDATE batches SET status = ?, finished_at = ?, duration_ms = ?, "
            "rows_read = COALESCE(?, rows_read), "
            "rows_written = COALESCE(?, rows_written), "
            "bronze_path = COALESCE(?, bronze_path), "
            f"{error_clause} "
            "WHERE batch_id = ?;"
        )
        # LEARN: ``*error_params`` unpacks the tuple into this position.
        # The ``params`` tuple is the ordered sequence of values bound
        # to the ``?`` placeholders in ``sql``.
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

    # ---- run-level helpers (mirror the batch-level ones above) --------

    @staticmethod
    def _require_layer(layer: str) -> None:
        """Validate ``layer`` at the Python boundary instead of relying
        only on the SQL CHECK constraint. Gives callers a clean
        ``ManifestError`` before we round-trip to SQLite.
        """
        if layer not in RUN_LAYERS:
            raise ManifestError(
                f"invalid run layer {layer!r}; expected one of {RUN_LAYERS}"
            )

    def _update_run_status(
        self,
        *,
        run_id: str,
        status: str,
        finished_at: str,
        duration_ms: int,
        rows_in: int | None = None,
        rows_out: int | None = None,
        rows_deduped: int | None = None,
        rows_rejected: int | None = None,
        output_path: str | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        clear_error_fields: bool = False,
    ) -> None:
        if status not in RUN_STATUSES:
            raise ManifestError(
                f"invalid run status {status!r}; expected one of {RUN_STATUSES}"
            )
        if clear_error_fields:
            error_clause = "error_type = NULL, error_message = NULL"
            error_params: tuple[object, ...] = ()
        else:
            error_clause = "error_type = ?, error_message = ?"
            error_params = (error_type, error_message)

        # LEARN: COALESCE keeps values recorded by a prior partial
        # update. Useful for Silver where ``mark_run_completed`` is the
        # single call that records rows_* and output_path — but the
        # pattern keeps the door open for future multi-phase updates
        # (e.g., a "dedup done, mask pending" partial update in F5).
        sql = (
            "UPDATE runs SET status = ?, finished_at = ?, duration_ms = ?, "
            "rows_in = COALESCE(?, rows_in), "
            "rows_out = COALESCE(?, rows_out), "
            "rows_deduped = COALESCE(?, rows_deduped), "
            "rows_rejected = COALESCE(?, rows_rejected), "
            "output_path = COALESCE(?, output_path), "
            f"{error_clause} "
            "WHERE run_id = ?;"
        )
        params: tuple[object, ...] = (
            status,
            finished_at,
            duration_ms,
            rows_in,
            rows_out,
            rows_deduped,
            rows_rejected,
            output_path,
            *error_params,
            run_id,
        )
        with self._transaction() as cur:
            try:
                cur.execute(sql, params)
            except sqlite3.IntegrityError as exc:
                raise ManifestError(
                    f"integrity error updating run {run_id!r}: {exc}"
                ) from exc
            if cur.rowcount == 0:
                raise ManifestError(f"no run row to update: {run_id!r}")


def _utcnow_iso() -> str:
    """Return the current UTC timestamp as an ISO-8601 string (seconds resolution)."""
    # LEARN: ``datetime.now(tz=UTC)`` returns a timezone-aware ``datetime``.
    # ``strftime`` formats it. ``Z`` at the end is the ISO-8601 marker
    # for UTC ("Zulu time") — preferred over ``+00:00`` in logs.
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
