"""SQLite schema for the state manifest and the LLM cache.

All tables share one SQLite file (default ``state/manifest.db``) per
ADR-003. The DDL is idempotent — ``ensure_schema`` in
``pipeline.state.manifest`` executes the full list on every startup.
"""

from __future__ import annotations

from typing import Final

BATCHES_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id            TEXT PRIMARY KEY,
    source_path         TEXT NOT NULL,
    source_hash         TEXT NOT NULL,
    source_mtime        INTEGER NOT NULL,
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

BATCHES_INDEXES: Final[tuple[str, ...]] = (
    "CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);",
    "CREATE INDEX IF NOT EXISTS idx_batches_started ON batches(started_at);",
)

RUNS_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS runs (
    run_id              TEXT PRIMARY KEY,
    batch_id            TEXT NOT NULL REFERENCES batches(batch_id),
    layer               TEXT NOT NULL CHECK (layer IN ('bronze','silver','gold')),
    status              TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    rows_in             INTEGER,
    rows_out            INTEGER,
    error_type          TEXT,
    error_message       TEXT
);
"""

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

ALL_DDL: Final[tuple[str, ...]] = (
    BATCHES_DDL,
    *BATCHES_INDEXES,
    RUNS_DDL,
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
