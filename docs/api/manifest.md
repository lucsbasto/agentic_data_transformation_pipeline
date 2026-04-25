# ManifestDB API Reference

## Overview

`ManifestDB` is the public API for the runtime state manifest — a single-file SQLite database at `state/manifest.db` (configurable). It tracks batch executions, layer transforms, agent loop iterations, and LLM cache entries across the entire pipeline.

The manifest persists:
- **Batches**: Ingest attempts with source file metadata and completion status.
- **Runs**: Per-layer execution logs (Bronze, Silver, Gold) with row counts and transform metrics.
- **Agent runs**: Agent loop iterations (F4+) with recovery and escalation tallies.
- **Agent failures**: Failure records per `(batch_id, layer, error_class)` triple with retry budget tracking.
- **LLM cache**: Keyed response cache to avoid redundant API calls.

All tables coexist in one SQLite file per ADR-003, enabling atomic multi-table transactions and a single backup/recovery mechanism.

## Storage Location

Default: `state/manifest.db` (relative to the pipeline root, or absolute if passed to `ManifestDB(path)`).

Uses Write-Ahead Logging (WAL) for concurrent read/write. Connection pool manages 5-second busy timeout.

## Schema

### batches

Tracks data ingestion attempts.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `batch_id` | TEXT | No | Primary key. Caller-generated (typically a hash or UUID). |
| `source_path` | TEXT | No | Path to the source parquet file. |
| `source_hash` | TEXT | No | Content hash (e.g., MD5 or SHA256) for change detection. |
| `source_mtime` | INTEGER | No | Source file modification time (UNIX seconds). |
| `status` | TEXT | No | CHECK(`IN_PROGRESS` \| `COMPLETED` \| `FAILED`). |
| `rows_read` | INTEGER | Yes | Rows fetched from source during ingest. |
| `rows_written` | INTEGER | Yes | Rows written to Bronze output. |
| `bronze_path` | TEXT | Yes | Path to the written Bronze parquet file. |
| `started_at` | TEXT | No | ISO-8601 timestamp (UTC, seconds resolution). |
| `finished_at` | TEXT | Yes | ISO-8601 timestamp when status became terminal. |
| `duration_ms` | INTEGER | Yes | Elapsed milliseconds (finished_at - started_at). |
| `error_type` | TEXT | Yes | Error classification (e.g., `ValidationError`, `StaleInProgress`). |
| `error_message` | TEXT | Yes | Human-readable error details. |

**Primary Key**: `batch_id`

**Indexes**:
- `idx_batches_status` on `status` — speeds up "FAILED batches" or "IN_PROGRESS cleanup" queries.
- `idx_batches_started` on `started_at` — supports time-range queries for observability.

**Crash Recovery**: On every `open()`, any `IN_PROGRESS` batch is swept to `FAILED` with `error_type='StaleInProgress'` (previous process crashed without completing).

### runs

Per-layer execution log. One row per attempt (Bronze ingest, Silver transform, Gold build).

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `run_id` | TEXT | No | Primary key. Caller-generated UUID, matches structlog run IDs. |
| `batch_id` | TEXT | No | Foreign key → `batches.batch_id` with `ON DELETE CASCADE`. |
| `layer` | TEXT | No | CHECK(`bronze` \| `silver` \| `gold`). |
| `status` | TEXT | No | CHECK(`IN_PROGRESS` \| `COMPLETED` \| `FAILED`). |
| `started_at` | TEXT | No | ISO-8601 timestamp (UTC, seconds resolution). |
| `finished_at` | TEXT | Yes | ISO-8601 timestamp when status became terminal. |
| `duration_ms` | INTEGER | Yes | Elapsed milliseconds. |
| `rows_in` | INTEGER | Yes | Input row count (Bronze: rows from source; Silver/Gold: from prior layer). |
| `rows_out` | INTEGER | Yes | Output row count written to parquet. |
| `rows_deduped` | INTEGER | Yes | (Silver only) Duplicate rows collapsed. Null for Bronze/Gold. |
| `rows_rejected` | INTEGER | Yes | (Silver only) Rows routed to rejected/ directory. Null for Bronze/Gold. |
| `output_path` | TEXT | Yes | Path to layer output parquet (Bronze null; see `batches.bronze_path`). |
| `error_type` | TEXT | Yes | Error classification. |
| `error_message` | TEXT | Yes | Error details (max 512 chars per spec). |

**Primary Key**: `run_id`

**Indexes**:
- `idx_runs_batch_id` on `batch_id`.
- `idx_runs_status` on `status`.
- `idx_runs_layer` on `layer`.
- `idx_runs_batch_layer` on `(batch_id, layer)` — composite, supports is_run_completed() lookup.

**Crash Recovery**: On every `open()`, any `IN_PROGRESS` run is swept to `FAILED` with `error_type='StaleInProgress'`.

### llm_cache

LLM response cache keyed by request hash.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `cache_key` | TEXT | No | Primary key. Hash of request (algorithm defined in `llm/cache.py`). |
| `model` | TEXT | No | Model name (e.g., `gpt-4`, `claude-opus`). |
| `response_text` | TEXT | No | Full response body. |
| `input_tokens` | INTEGER | No | Prompt tokens used. |
| `output_tokens` | INTEGER | No | Completion tokens used. |
| `created_at` | TEXT | No | ISO-8601 timestamp when response was cached. |

**Primary Key**: `cache_key`

No public methods in `ManifestDB` expose LLM cache operations (handled by `pipeline.llm.cache` module).

### agent_runs

F4+ agent loop iterations.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `agent_run_id` | TEXT | No | Primary key. UUID-4, generated by `start_agent_run()`. |
| `started_at` | TEXT | No | ISO-8601 timestamp when loop began. |
| `ended_at` | TEXT | Yes | ISO-8601 timestamp when loop entered terminal status. |
| `status` | TEXT | No | CHECK(`IN_PROGRESS` \| `COMPLETED` \| `INTERRUPTED` \| `FAILED`). |
| `batches_processed` | INTEGER | No | Default 0. Updated on `end_agent_run()`. |
| `failures_recovered` | INTEGER | No | Default 0. Count of escalated failures that were recovered. |
| `escalations` | INTEGER | No | Default 0. Count of failures that hit retry budget. |

**Primary Key**: `agent_run_id`

**Indexes**:
- `idx_agent_runs_status` on `status`.

### agent_failures

Failure log for executor retry tracking.

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `failure_id` | TEXT | No | Primary key. UUID-4, generated by `record_agent_failure()`. |
| `agent_run_id` | TEXT | No | Foreign key → `agent_runs.agent_run_id` with `ON DELETE CASCADE`. |
| `batch_id` | TEXT | No | Batch experiencing the error. |
| `layer` | TEXT | No | CHECK(`bronze` \| `silver` \| `gold`). |
| `error_class` | TEXT | No | CHECK(`schema_drift` \| `regex_break` \| `partition_missing` \| `out_of_range` \| `unknown`). |
| `attempts` | INTEGER | No | Ordinal attempt number within the `(batch_id, layer, error_class)` triple. |
| `last_fix_kind` | TEXT | Yes | Fix strategy applied (e.g., `backfill`, `mask`, `skip_batch`). |
| `escalated` | INTEGER | No | CHECK(`0` \| `1`). Set to 1 by `mark_agent_failure_escalated()`. |
| `last_error_msg` | TEXT | Yes | Truncated error text (max 512 chars per spec §4.2). |
| `ts` | TEXT | No | ISO-8601 timestamp when failure was recorded. |

**Primary Key**: `failure_id`

**Indexes**:
- `idx_agent_failures_run` on `agent_run_id`.
- `idx_agent_failures_kind` on `(batch_id, layer, error_class)` — composite, supports executor retry lookups.

## Public Methods

### Lifecycle

#### `__init__(db_path: Path | str) -> None`

Create a manifest instance. Does not open the connection.

**Parameters:**
- `db_path`: Path to SQLite file, or `:memory:` for in-memory (tests only).

**Idempotent**: Yes. Safe to instantiate multiple times with the same path.

#### `open() -> ManifestDB`

Open (or reopen) the connection, ensure schema, and sweep stale rows.

**Returns:** `self`

**Side effects**:
- Creates parent directories if needed.
- Executes all DDL statements (idempotent via `IF NOT EXISTS`).
- Applies column migrations (F1→F2 additions).
- Calls `reset_stale()` and `reset_stale_runs()` to mark crashed batches/runs as `FAILED`.
- Enables WAL (except for in-memory databases).

**Idempotent**: Yes. Calling `open()` on an already-open connection returns immediately.

**Raises**: `ManifestError` if directory creation fails or schema setup encounters a database error.

#### `close() -> None`

Close the connection without removing the file.

**Idempotent**: Yes. Calling on a closed or unopened connection is a no-op.

#### Context Manager Protocol

```python
with ManifestDB("state/manifest.db") as db:
    db.insert_batch(...)  # connection is open
    # __exit__ calls close() automatically
```

**Idempotent**: Yes. Multiple context managers for the same path are safe (separate connections).

---

### Batches

#### `insert_batch(*, batch_id: str, source_path: str, source_hash: str, source_mtime: int, started_at: str) -> None`

Create a new batch row in `IN_PROGRESS` state.

**Parameters:**
- `batch_id`: Unique batch identifier (collision raises `ManifestError`).
- `source_path`: Path to source parquet file.
- `source_hash`: Content hash for change detection.
- `source_mtime`: UNIX timestamp (seconds) of source file modification.
- `started_at`: ISO-8601 timestamp when ingest began (or call `_utcnow_iso()`).

**Idempotent**: No. Re-inserting the same `batch_id` raises `ManifestError`.

**Raises**: `ManifestError` if `batch_id` already exists.

#### `mark_completed(*, batch_id: str, rows_read: int, rows_written: int, bronze_path: str, finished_at: str, duration_ms: int) -> None`

Mark a batch `COMPLETED` and record ingest metrics. Clears any `error_type`/`error_message` from prior attempts.

**Parameters:**
- `batch_id`: Batch to complete.
- `rows_read`: Rows fetched from source.
- `rows_written`: Rows written to Bronze.
- `bronze_path`: Path to output parquet.
- `finished_at`: ISO-8601 timestamp when ingest completed.
- `duration_ms`: Elapsed milliseconds.

**Idempotent**: No. Calling twice with the same `batch_id` succeeds both times (idempotent at the row level — same values written twice).

**Raises**: `ManifestError` if `batch_id` does not exist.

#### `mark_failed(*, batch_id: str, finished_at: str, duration_ms: int, error_type: str, error_message: str) -> None`

Mark a batch `FAILED` and record the error cause. Leaves `rows_read`/`rows_written`/`bronze_path` untouched (from prior partial progress).

**Parameters:**
- `batch_id`: Batch to fail.
- `finished_at`: ISO-8601 timestamp when failure occurred.
- `duration_ms`: Elapsed milliseconds.
- `error_type`: Error classification (e.g., `ValidationError`, `FileNotFoundError`).
- `error_message`: Human-readable details.

**Idempotent**: No. Calling twice overwrites `error_type`/`error_message`.

**Raises**: `ManifestError` if `batch_id` does not exist.

#### `get_batch(batch_id: str) -> BatchRow | None`

Fetch one batch row by ID.

**Returns:** `BatchRow` (immutable dataclass) or `None` if not found.

**Idempotent**: Yes (read-only).

#### `is_batch_completed(batch_id: str) -> bool`

Check if a batch exists and has status `COMPLETED`.

**Returns:** `True` only if `batch_id` exists and `status == 'COMPLETED'`; otherwise `False`.

**Idempotent**: Yes (read-only).

#### `delete_batch(batch_id: str) -> bool`

Remove a batch row unconditionally. Cascades to delete all associated `runs` rows.

**Returns:** `True` if a row was deleted; `False` if `batch_id` did not exist.

**Use case**: Retry-after-FAILED path. `reset_stale()` only sweeps `IN_PROGRESS` rows, so a previously `FAILED` batch blocks re-insert. Callers explicitly delete before retrying.

**Idempotent**: No. The second call returns `False` (no row to delete).

---

### Runs

#### `reset_stale_runs(*, now_iso: str | None = None, layer: str | None = None) -> int`

Mark orphaned `IN_PROGRESS` runs as `FAILED` (crash recovery).

**Parameters:**
- `now_iso`: ISO-8601 timestamp for `finished_at`. Default: current UTC time.
- `layer`: Optional. If set (one of `bronze`, `silver`, `gold`), only sweep that layer. If `None`, sweep all layers.

**Returns:** Number of rows swept to `FAILED`.

**Side effect**: Sets `error_type='StaleInProgress'` and `error_message='previous process exited without completing this batch'`.

**Called automatically**: On every `open()`.

**Idempotent**: Yes. Re-calling with the same parameters updates already-swept rows with the same values (idempotent at the row level).

#### `insert_run(*, run_id: str, batch_id: str, layer: str, started_at: str) -> None`

Create a new run row in `IN_PROGRESS` state.

**Parameters:**
- `run_id`: Unique run identifier (caller-generated, typically matches structlog run ID). Collision raises `ManifestError`.
- `batch_id`: Parent batch (must exist; FK violation raises `ManifestError`).
- `layer`: One of `bronze`, `silver`, `gold`.
- `started_at`: ISO-8601 timestamp when layer execution began.

**Idempotent**: No. Re-inserting the same `run_id` raises `ManifestError`.

**Raises**:
- `ManifestError` if `run_id` already exists.
- `ManifestError` if `batch_id` does not exist (FK violation).
- `ManifestError` if `layer` is not in `('bronze', 'silver', 'gold')`.

#### `mark_run_completed(*, run_id: str, finished_at: str, duration_ms: int, rows_in: int | None = None, rows_out: int | None = None, rows_deduped: int | None = None, rows_rejected: int | None = None, output_path: str | None = None) -> None`

Flip a run to `COMPLETED` and record transform metrics.

**Parameters:**
- `run_id`: Run to complete.
- `finished_at`: ISO-8601 timestamp when layer completed.
- `duration_ms`: Elapsed milliseconds.
- `rows_in`: Input row count (optional, preserved on re-call via `COALESCE`).
- `rows_out`: Output row count.
- `rows_deduped`: (Silver only) Deduplicated rows.
- `rows_rejected`: (Silver only) Rejected rows.
- `output_path`: Path to layer output parquet.

**Idempotent**: No. Calling twice overwrites metrics. However, `COALESCE` in the UPDATE preserves values from partial prior updates (if re-called with `NULL` for a metric, the prior value is retained).

**Raises**: `ManifestError` if `run_id` does not exist.

#### `mark_run_failed(*, run_id: str, finished_at: str, duration_ms: int, error_type: str, error_message: str) -> None`

Flip a run to `FAILED` and record the error cause. Leaves `rows_*` and `output_path` untouched.

**Parameters:**
- `run_id`: Run to fail.
- `finished_at`: ISO-8601 timestamp when failure occurred.
- `duration_ms`: Elapsed milliseconds.
- `error_type`: Error classification.
- `error_message`: Human-readable details.

**Idempotent**: No. Calling twice overwrites `error_type`/`error_message`.

**Raises**: `ManifestError` if `run_id` does not exist.

#### `get_run(run_id: str) -> RunRow | None`

Fetch one run row by ID.

**Returns:** `RunRow` (immutable dataclass) or `None` if not found.

**Idempotent**: Yes (read-only).

#### `get_latest_run(*, batch_id: str, layer: str) -> RunRow | None`

Return the most recently started run for `(batch_id, layer)`.

**Parameters:**
- `batch_id`: Batch ID.
- `layer`: One of `bronze`, `silver`, `gold`.

**Returns:** `RunRow` with the highest `started_at`, or `None` if no run exists for that pair.

**Ordering**: By `started_at DESC`, ties broken by `rowid DESC` (deterministic even with second-resolution timestamps).

**Idempotent**: Yes (read-only).

**Raises**: `ManifestError` if `layer` is invalid.

#### `is_run_completed(*, batch_id: str, layer: str) -> bool`

Check if `(batch_id, layer)` has at least one `COMPLETED` run.

**Returns:** `True` if at least one run with status `COMPLETED` exists for the pair; otherwise `False`.

**Idempotent**: Yes (read-only).

**Raises**: `ManifestError` if `layer` is invalid.

#### `delete_runs_for(*, batch_id: str, layer: str) -> int`

Delete every run row for `(batch_id, layer)`. Returns the deleted count.

**Use case**: Retry-after-FAILED path in Silver/Gold. Stale `IN_PROGRESS`/`FAILED` rows are wiped before a new `insert_run()` so the operator can simply re-run the CLI.

**Returns:** Number of rows deleted.

**Idempotent**: No. The second call returns 0 (no rows to delete).

**Raises**: `ManifestError` if `layer` is invalid.

---

### Agent Runs (F4+)

#### `start_agent_run(*, now_iso: str | None = None) -> str`

Open a new `agent_runs` row in `IN_PROGRESS` state.

**Parameters:**
- `now_iso`: ISO-8601 timestamp for `started_at`. Default: current UTC time.

**Returns:** Freshly-minted `agent_run_id` (UUID-4 hex string).

**Side effects**: Counters (`batches_processed`, `failures_recovered`, `escalations`) start at zero; `ended_at` is `NULL`.

**Idempotent**: No. Each call creates a new row with a unique ID.

#### `end_agent_run(agent_run_id: str, *, status: str, batches_processed: int = 0, failures_recovered: int = 0, escalations: int = 0, now_iso: str | None = None) -> None`

Stamp the terminal `status` and final tallies on an `agent_runs` row.

**Parameters:**
- `agent_run_id`: ID returned by `start_agent_run()`.
- `status`: One of `COMPLETED`, `INTERRUPTED`, `FAILED` (not `IN_PROGRESS`).
- `batches_processed`: Final count of batches processed.
- `failures_recovered`: Final count of recovered failures.
- `escalations`: Final count of escalated failures.
- `now_iso`: ISO-8601 timestamp for `ended_at`. Default: current UTC time.

**Idempotent**: No. Calling twice with the same `agent_run_id` overwrites tallies.

**Raises**:
- `ManifestError` if `status` is not one of the valid AGENT_RUN_STATUSES.
- `ManifestError` if `status == 'IN_PROGRESS'` (must be a terminal status).
- `ManifestError` if `agent_run_id` does not exist.

---

### Agent Failures

#### `record_agent_failure(*, agent_run_id: str, batch_id: str, layer: str, error_class: str, attempts: int, last_error_msg: str, now_iso: str | None = None) -> str`

Append one `agent_failures` row and return its `failure_id`.

**Parameters:**
- `agent_run_id`: Parent agent run ID.
- `batch_id`: Batch experiencing the error.
- `layer`: One of `bronze`, `silver`, `gold`.
- `error_class`: One of `schema_drift`, `regex_break`, `partition_missing`, `out_of_range`, `unknown`.
- `attempts`: Ordinal attempt number (>=1). The executor increments this before recording.
- `last_error_msg`: Raw exception text. Truncated to 512 chars before insert (per spec §4.2).
- `now_iso`: ISO-8601 timestamp for `ts`. Default: current UTC time.

**Returns:** Freshly-minted `failure_id` (UUID-4 hex string).

**Idempotent**: No. Each call creates a new row.

**Raises**:
- `ManifestError` if `layer` is invalid.
- `ManifestError` if `error_class` is not one of the valid AGENT_ERROR_KINDS.
- `ManifestError` if `attempts < 1`.
- `ManifestError` if `agent_run_id` does not exist (FK violation).

#### `record_agent_fix(failure_id: str, *, fix_kind: str) -> None`

Stamp the fix strategy applied after a failure. The row stays `escalated=0`.

**Parameters:**
- `failure_id`: ID returned by `record_agent_failure()`.
- `fix_kind`: Fix strategy (e.g., `backfill`, `mask`, `skip_batch`). No enum — any string accepted.

**Idempotent**: No. Calling twice overwrites `last_fix_kind`.

**Raises**: `ManifestError` if `failure_id` does not exist.

#### `mark_agent_failure_escalated(failure_id: str) -> None`

Flip `escalated=1` on a failure row.

**Parameters:**
- `failure_id`: ID returned by `record_agent_failure()`.

**Idempotent**: Yes. Re-calling on an already-escalated row is a no-op UPDATE (rowcount may be 0, but the method does not error).

**Raises**: `ManifestError` if `failure_id` does not exist on the first call.

#### `count_agent_attempts(*, batch_id: str, layer: str, error_class: str) -> int`

Return how many failure rows exist for the `(batch_id, layer, error_class)` triple across all `agent_run` rows.

**Parameters:**
- `batch_id`: Batch ID.
- `layer`: One of `bronze`, `silver`, `gold`.
- `error_class`: One of the valid AGENT_ERROR_KINDS.

**Returns:** Count (0 if no failures recorded for this triple).

**Use case**: Executor checks this count against the retry budget to decide whether to escalate (F4 design §6).

**Idempotent**: Yes (read-only).

**Raises**:
- `ManifestError` if `layer` is invalid.
- `ManifestError` if `error_class` is not valid.

#### `latest_agent_failure_id(*, batch_id: str, layer: str, error_class: str) -> str | None`

Return the `failure_id` of the most recent `agent_failures` row for the `(batch_id, layer, error_class)` triple, or `None` when no row exists.

**Parameters:**
- `batch_id`: Batch ID.
- `layer`: One of `bronze`, `silver`, `gold`.
- `error_class`: One of the valid AGENT_ERROR_KINDS.

**Returns:** `failure_id` string or `None`.

**Ordering**: By `attempts DESC` (the ordinal written) instead of `ts` (second resolution; deterministic even on fast test runs).

**Use case**: Executor needs this to flip `escalated=1` on the exact row that triggered the escalation after the retry budget is exhausted (F4 design §6).

**Idempotent**: Yes (read-only).

**Raises**:
- `ManifestError` if `layer` is invalid.
- `ManifestError` if `error_class` is not valid.

---

### Schema & Migrations

#### `ensure_schema() -> None`

Create all tables and apply column migrations if needed.

**Side effects**:
- Executes all DDL statements from `pipeline.schemas.manifest.ALL_DDL` (idempotent via `IF NOT EXISTS`).
- For F1→F2 upgrades: checks for missing columns in `runs` table and runs `ALTER TABLE ADD COLUMN` statements as needed.

**Called automatically**: By `open()`.

**Idempotent**: Yes. Re-calling on an already-initialized DB is a no-op (or applies missing migrations only).

#### `reset_stale(*, now_iso: str | None = None) -> int`

Mark orphaned `IN_PROGRESS` batches as `FAILED` (crash recovery).

**Parameters:**
- `now_iso`: ISO-8601 timestamp for `finished_at`. Default: current UTC time.

**Returns:** Number of batches swept to `FAILED`.

**Side effect**: Sets `error_type='StaleInProgress'` and `error_message='previous process exited without completing this batch'`.

**Called automatically**: By `open()` on every startup.

**Context**: A new process starting up means any `IN_PROGRESS` row is from a previous process that crashed. Without this sweep, re-running the same batch hits the primary-key constraint and surfaces as a cryptic `IntegrityError` instead of a clean retry.

**Idempotent**: Yes. Re-calling with the same parameters updates already-swept rows with the same values.

---

## Idempotency Table

| Method | Idempotent | Semantics on Repeat Call |
|--------|-----------|--------------------------|
| `open()` | Yes | Returns immediately if already open. |
| `close()` | Yes | No-op on closed/unopened connection. |
| `ensure_schema()` | Yes | CREATE TABLE IF NOT EXISTS; migrates missing columns only. |
| `reset_stale(now_iso)` | Yes | Updates already-swept rows to same values. Same `now_iso` yields identical row values. |
| `insert_batch(...)` | No | Re-insert with same ID raises `ManifestError`. |
| `mark_completed(...)` | Semi | Succeeds both times; row contains same values both times. Not a transaction guard — use application logic. |
| `mark_failed(...)` | No | Overwrites `error_type`/`error_message`; subsequent calls change row state. |
| `get_batch(...)` | Yes | Read-only; same return each time. |
| `is_batch_completed(...)` | Yes | Read-only; same return each time. |
| `delete_batch(...)` | No | Second call returns False; first returns True. |
| `reset_stale_runs(layer, now_iso)` | Yes | Updates already-swept runs to same values. |
| `insert_run(...)` | No | Re-insert with same ID raises `ManifestError`. |
| `mark_run_completed(...)` | Semi | Succeeds both times; metrics overwritten with same values. `COALESCE` preserves null metrics. |
| `mark_run_failed(...)` | No | Overwrites `error_type`/`error_message`; subsequent calls change row state. |
| `get_run(...)` | Yes | Read-only. |
| `get_latest_run(...)` | Yes | Read-only. |
| `is_run_completed(...)` | Yes | Read-only. |
| `delete_runs_for(...)` | No | Second call returns 0; first returns deleted count. |
| `start_agent_run(...)` | No | Each call creates a new row with unique ID. |
| `end_agent_run(agent_run_id, status, ...)` | No | Overwrites tallies and status; multiple calls change row state. |
| `record_agent_failure(...)` | No | Each call creates a new row with unique ID. |
| `record_agent_fix(failure_id, ...)` | No | Overwrites `last_fix_kind`; multiple calls change row state. |
| `mark_agent_failure_escalated(...)` | Yes | UPDATE to `escalated=1` is idempotent (rowcount may be 0 if already escalated, but no error). |
| `count_agent_attempts(...)` | Yes | Read-only. |
| `latest_agent_failure_id(...)` | Yes | Read-only. |

**Notes:**
- **Yes**: Safe to call multiple times; same input yields same observable effect.
- **No**: Subsequent calls change row state or raise errors.
- **Semi**: First call succeeds and changes state; second call succeeds but overwrites the same state (idempotent at the row level, not at the transaction level).

---

## Error Handling

All methods that operate on the database raise `ManifestError` (from `pipeline.errors`) in the following cases:

- **Connection not open**: Call without `open()` or after `close()`.
- **Constraint violation**: Primary key collision on insert, foreign key violation, status/layer validation.
- **Row not found**: Update called on non-existent ID.
- **Invalid enum**: Status, layer, or error_class not in valid set.

**Example:**
```python
from pipeline.state.manifest import ManifestDB
from pipeline.errors import ManifestError

try:
    with ManifestDB("state/manifest.db") as db:
        db.insert_batch(batch_id="b1", ...)
except ManifestError as e:
    print(f"Manifest error: {e}")
```

---

## Transaction Model

All write operations (`insert_*`, `mark_*`, `record_*`, `delete_*`, `reset_*`) run within an explicit `BEGIN`/`COMMIT`/`ROLLBACK` transaction managed by the `_transaction()` context manager. On exception, the transaction is rolled back automatically; on success, it is committed.

Read operations (`get_*`, `is_*`, `count_*`, `latest_*`) do not require transactions and use the open connection directly.

---

## Cross-References

- **Error types**: See `docs/api/errors.md` for domain error classifications and recovery strategies.
- **Agent loop**: See `docs/agent-flow.md` for F4 executor design and retry semantics.
- **Schema details**: See `src/pipeline/schemas/manifest.py` for DDL, migrations, and constants.

---

## Usage Example

```python
from pathlib import Path
from pipeline.state.manifest import ManifestDB, _utcnow_iso

# Batch workflow
with ManifestDB("state/manifest.db") as db:
    # Start a batch ingest
    now = _utcnow_iso()
    db.insert_batch(
        batch_id="data_2025-01-15",
        source_path="input/2025-01-15.parquet",
        source_hash="abc123",
        source_mtime=1705334400,
        started_at=now,
    )

    # Create a Bronze run
    run_id = "run_bronze_001"
    db.insert_run(
        run_id=run_id,
        batch_id="data_2025-01-15",
        layer="bronze",
        started_at=now,
    )

    # ... ingest ...

    # Complete the run
    finished_at = _utcnow_iso()
    db.mark_run_completed(
        run_id=run_id,
        finished_at=finished_at,
        duration_ms=5000,
        rows_in=1000,
        rows_out=995,
        output_path="output/bronze/data_2025-01-15.parquet",
    )

    # Complete the batch
    db.mark_completed(
        batch_id="data_2025-01-15",
        rows_read=1000,
        rows_written=995,
        bronze_path="output/bronze/data_2025-01-15.parquet",
        finished_at=finished_at,
        duration_ms=5000,
    )

# Agent loop workflow (F4+)
with ManifestDB("state/manifest.db") as db:
    agent_run_id = db.start_agent_run()
    
    try:
        # ... agent processing ...
        failures_recovered = 2
        db.end_agent_run(
            agent_run_id,
            status="COMPLETED",
            batches_processed=10,
            failures_recovered=failures_recovered,
            escalations=0,
        )
    except Exception as e:
        db.end_agent_run(agent_run_id, status="FAILED")
```
