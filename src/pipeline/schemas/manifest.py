"""SQLite schema for the state manifest and the LLM cache.

All tables share one SQLite file (default ``state/manifest.db``) per
ADR-003. The DDL is idempotent — ``ensure_schema`` in
``pipeline.state.manifest`` executes the full list on every startup.
"""

from __future__ import annotations

from typing import Final

# LEARN: DDL = "Data Definition Language" — the subset of SQL that
# creates / alters tables. We store each statement as a Python ``str``
# constant and execute it at startup. Benefits for a beginner:
#   - the schema lives next to the code that queries it;
#   - the same string is reused by tests that spin up a fresh DB;
#   - ``IF NOT EXISTS`` makes the statement idempotent — running it on
#     an already-created DB is a safe no-op.
# Triple-quoted strings (``""" ... """``) let us embed multi-line SQL
# without string concatenation clutter.
BATCHES_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id            TEXT PRIMARY KEY,
    source_path         TEXT NOT NULL,
    source_hash         TEXT NOT NULL,
    source_mtime        INTEGER NOT NULL,
    -- CHECK constraints are per-row validators SQLite runs before INSERT
    -- or UPDATE. A typo like status='FAIED' fails at the DB layer; we
    -- never have to remember to validate it in Python too.
    status              TEXT NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED','FAILED')),
    rows_read           INTEGER,
    rows_written        INTEGER,
    bronze_path         TEXT,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    duration_ms         INTEGER,
    error_type          TEXT,
    error_message       TEXT
);
"""

# LEARN: indexes speed up queries that filter or sort on the indexed
# columns. Without ``idx_batches_status`` a query like ``WHERE status =
# 'FAILED'`` would scan every row. Keep indexes with their table DDL so
# schema changes stay in one place.
BATCHES_INDEXES: Final[tuple[str, ...]] = (
    "CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);",
    "CREATE INDEX IF NOT EXISTS idx_batches_started ON batches(started_at);",
)

# LEARN: a block of string constants that mirror the CHECK list in the
# RUNS_DDL below. Duplication is intentional: Python code compares
# against these names, SQL enforces them at the DB. Both agree.
RUN_STATUS_IN_PROGRESS: Final[str] = "IN_PROGRESS"
RUN_STATUS_COMPLETED: Final[str] = "COMPLETED"
RUN_STATUS_FAILED: Final[str] = "FAILED"

RUN_STATUSES: Final[tuple[str, ...]] = (
    RUN_STATUS_IN_PROGRESS,
    RUN_STATUS_COMPLETED,
    RUN_STATUS_FAILED,
)

RUNS_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS runs (
    run_id              TEXT PRIMARY KEY,
    -- FOREIGN KEY: ``runs.batch_id`` must point at an existing row in
    -- ``batches``. ON DELETE CASCADE means deleting the parent batch
    -- also deletes its runs — so Bronze's retry path (delete_batch)
    -- never leaves dangling run rows behind.
    batch_id            TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
    layer               TEXT NOT NULL CHECK (layer IN ('bronze','silver','gold')),
    status              TEXT NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED','FAILED')),
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    rows_in             INTEGER,
    rows_out            INTEGER,
    error_type          TEXT,
    error_message       TEXT
);
"""

RUNS_INDEXES: Final[tuple[str, ...]] = (
    "CREATE INDEX IF NOT EXISTS idx_runs_batch_id ON runs(batch_id);",
    "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);",
    "CREATE INDEX IF NOT EXISTS idx_runs_layer ON runs(layer);",
)

# LEARN: the LLM cache table. Every successful call to the LLM gets
# stored here keyed by a hash of the request (see ``llm/cache.py``). A
# later identical request becomes a DB lookup instead of a $$$ API call.
# Notice this table lives in the SAME sqlite file as ``batches`` / ``runs``
# — ADR-003 says "one SQLite file for all local state". One connection,
# one backup, one schema.
LLM_CACHE_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS llm_cache (
    cache_key       TEXT PRIMARY KEY,
    model           TEXT NOT NULL,
    response_text   TEXT NOT NULL,
    input_tokens    INTEGER NOT NULL,
    output_tokens   INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);
"""

# LEARN: ``*BATCHES_INDEXES`` inside a tuple literal UNPACKS the inner
# tuple into this position. Equivalent to writing each index statement
# again by hand, but stays in sync automatically when we add a new
# index up above. Python calls this "iterable unpacking".
ALL_DDL: Final[tuple[str, ...]] = (
    BATCHES_DDL,
    *BATCHES_INDEXES,
    RUNS_DDL,
    *RUNS_INDEXES,
    LLM_CACHE_DDL,
)

BATCH_STATUS_IN_PROGRESS: Final[str] = "IN_PROGRESS"
BATCH_STATUS_COMPLETED: Final[str] = "COMPLETED"
BATCH_STATUS_FAILED: Final[str] = "FAILED"

BATCH_STATUSES: Final[tuple[str, ...]] = (
    BATCH_STATUS_IN_PROGRESS,
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_FAILED,
)
