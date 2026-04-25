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

# LEARN: ``runs`` is the per-layer execution log. One row per attempt
# (``run_id`` is caller-generated, typically the same short UUID that
# shows up in ``structlog`` events). Silver and Gold reuse this table —
# the ``layer`` column is the only thing that differs between a Bronze
# ingest, a Silver transform, and a Gold build.
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
    duration_ms         INTEGER,
    rows_in             INTEGER,
    rows_out            INTEGER,
    -- F2: Silver reports how many duplicate (conversation_id, message_id)
    -- rows were collapsed. Gold leaves this ``NULL`` because Gold's
    -- aggregation semantics are different.
    rows_deduped        INTEGER,
    -- F2: Silver also routes rows with null dedup keys (or future
    -- contract violations) to a sibling ``rejected/`` parquet — this
    -- column records how many landed there. Null for Bronze/Gold runs.
    rows_rejected       INTEGER,
    -- Written path of the layer's parquet output. Silver: Silver partition
    -- root. Gold: Gold table dir. Bronze could also populate this in
    -- retrospect, but F1 stores its path on ``batches.bronze_path`` so
    -- ``output_path`` for a bronze run is typically left ``NULL``.
    output_path         TEXT,
    error_type          TEXT,
    error_message       TEXT
);
"""

RUNS_INDEXES: Final[tuple[str, ...]] = (
    "CREATE INDEX IF NOT EXISTS idx_runs_batch_id ON runs(batch_id);",
    "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);",
    "CREATE INDEX IF NOT EXISTS idx_runs_layer ON runs(layer);",
    # LEARN: composite index — speeds up the common Silver/Gold lookup
    # "does this batch have a COMPLETED run for this layer?" which
    # filters on ``(batch_id, layer, status)``.
    "CREATE INDEX IF NOT EXISTS idx_runs_batch_layer ON runs(batch_id, layer);",
)

# LEARN: SQLite has no ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS``. The
# migration path is: read ``PRAGMA table_info(runs)`` (returns one row per
# column), diff against the list below, execute each missing ALTER. This
# tuple is the AUTHORITATIVE set of columns that exist in the current
# ``RUNS_DDL`` but were NOT in the F1 version of the DDL — a fresh DB
# gets them via CREATE TABLE; an existing F1 DB gets them via ALTER.
# Format: (column_name, full ``ADD COLUMN`` statement).
RUNS_MIGRATIONS: Final[tuple[tuple[str, str], ...]] = (
    ("duration_ms", "ALTER TABLE runs ADD COLUMN duration_ms INTEGER;"),
    ("rows_deduped", "ALTER TABLE runs ADD COLUMN rows_deduped INTEGER;"),
    ("output_path", "ALTER TABLE runs ADD COLUMN output_path TEXT;"),
    ("rows_rejected", "ALTER TABLE runs ADD COLUMN rows_rejected INTEGER;"),
)

# LEARN: ``layer`` values that pass the CHECK constraint. Kept here as
# Python constants so code comparisons stay type-checked rather than
# relying on stringly-typed literals scattered around the codebase.
RUN_LAYER_BRONZE: Final[str] = "bronze"
RUN_LAYER_SILVER: Final[str] = "silver"
RUN_LAYER_GOLD: Final[str] = "gold"

RUN_LAYERS: Final[tuple[str, ...]] = (
    RUN_LAYER_BRONZE,
    RUN_LAYER_SILVER,
    RUN_LAYER_GOLD,
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

# LEARN: F4 — the agent loop persists its own per-iteration row in
# ``agent_runs`` plus one row per failure attempt in ``agent_failures``.
# Both tables live in the same SQLite file as ``batches`` / ``runs``
# (ADR-003). Status / error_class strings mirror the ``ErrorKind`` and
# ``RunStatus`` StrEnums in ``pipeline.agent.types`` — the CHECK
# constraint enforces them at write time.
AGENT_RUN_STATUS_IN_PROGRESS: Final[str] = "IN_PROGRESS"
AGENT_RUN_STATUS_COMPLETED: Final[str] = "COMPLETED"
AGENT_RUN_STATUS_INTERRUPTED: Final[str] = "INTERRUPTED"
AGENT_RUN_STATUS_FAILED: Final[str] = "FAILED"

AGENT_RUN_STATUSES: Final[tuple[str, ...]] = (
    AGENT_RUN_STATUS_IN_PROGRESS,
    AGENT_RUN_STATUS_COMPLETED,
    AGENT_RUN_STATUS_INTERRUPTED,
    AGENT_RUN_STATUS_FAILED,
)

AGENT_ERROR_KIND_SCHEMA_DRIFT: Final[str] = "schema_drift"
AGENT_ERROR_KIND_REGEX_BREAK: Final[str] = "regex_break"
AGENT_ERROR_KIND_PARTITION_MISSING: Final[str] = "partition_missing"
AGENT_ERROR_KIND_OUT_OF_RANGE: Final[str] = "out_of_range"
AGENT_ERROR_KIND_UNKNOWN: Final[str] = "unknown"

AGENT_ERROR_KINDS: Final[tuple[str, ...]] = (
    AGENT_ERROR_KIND_SCHEMA_DRIFT,
    AGENT_ERROR_KIND_REGEX_BREAK,
    AGENT_ERROR_KIND_PARTITION_MISSING,
    AGENT_ERROR_KIND_OUT_OF_RANGE,
    AGENT_ERROR_KIND_UNKNOWN,
)

AGENT_RUNS_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS agent_runs (
    agent_run_id        TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    -- NULL while ``IN_PROGRESS``; set on terminal status update.
    ended_at            TEXT,
    status              TEXT NOT NULL CHECK (
        status IN ('IN_PROGRESS', 'COMPLETED', 'INTERRUPTED', 'FAILED')
    ),
    -- Counters refreshed at ``end_agent_run`` time so the row reflects
    -- the loop's final tally without an extra aggregate query.
    batches_processed   INTEGER NOT NULL DEFAULT 0,
    failures_recovered  INTEGER NOT NULL DEFAULT 0,
    escalations         INTEGER NOT NULL DEFAULT 0
);
"""

AGENT_RUNS_INDEXES: Final[tuple[str, ...]] = (
    "CREATE INDEX IF NOT EXISTS idx_agent_runs_status ON agent_runs(status);",
)

AGENT_FAILURES_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS agent_failures (
    failure_id          TEXT PRIMARY KEY,
    -- ON DELETE CASCADE so dropping an agent_run wipes its failure
    -- trail too — keeps the DB tidy in tests / dev resets.
    agent_run_id        TEXT NOT NULL REFERENCES agent_runs(agent_run_id) ON DELETE CASCADE,
    batch_id            TEXT NOT NULL,
    layer               TEXT NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
    error_class         TEXT NOT NULL CHECK (
        error_class IN (
            'schema_drift',
            'regex_break',
            'partition_missing',
            'out_of_range',
            'unknown'
        )
    ),
    -- ``attempts`` = ordinal of THIS attempt within the
    -- (batch_id, layer, error_class) triple. Executor increments it
    -- before the row is written.
    attempts            INTEGER NOT NULL,
    last_fix_kind       TEXT,
    -- 0/1 instead of TEXT to keep the JOIN on ``escalations`` cheap.
    escalated           INTEGER NOT NULL DEFAULT 0 CHECK (escalated IN (0, 1)),
    -- Sanitized + truncated by the writer (≤512 chars per spec §4.2).
    last_error_msg      TEXT,
    ts                  TEXT NOT NULL
);
"""

AGENT_FAILURES_INDEXES: Final[tuple[str, ...]] = (
    "CREATE INDEX IF NOT EXISTS idx_agent_failures_run ON agent_failures(agent_run_id);",
    # LEARN: composite index supports the executor's hot lookup
    # ``count_attempts(batch_id, layer, error_class)`` — without it
    # SQLite would full-scan agent_failures on every retry decision.
    "CREATE INDEX IF NOT EXISTS idx_agent_failures_kind "
    "ON agent_failures(batch_id, layer, error_class);",
)

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
    AGENT_RUNS_DDL,
    *AGENT_RUNS_INDEXES,
    AGENT_FAILURES_DDL,
    *AGENT_FAILURES_INDEXES,
)

BATCH_STATUS_IN_PROGRESS: Final[str] = "IN_PROGRESS"
BATCH_STATUS_COMPLETED: Final[str] = "COMPLETED"
BATCH_STATUS_FAILED: Final[str] = "FAILED"

BATCH_STATUSES: Final[tuple[str, ...]] = (
    BATCH_STATUS_IN_PROGRESS,
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_FAILED,
)
