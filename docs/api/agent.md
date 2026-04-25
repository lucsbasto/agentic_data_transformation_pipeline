# API Reference: pipeline.agent

## Module Overview

The `pipeline.agent` module implements the self-healing agentic loop (F4) that drives the Bronze → Silver → Gold pipeline end-to-end. The loop detects batches needing work, sequences execution across layers with a per-batch-per-layer retry budget, classifies failures with a two-stage deterministic-then-LLM approach, and escalates to observability when the budget exhausts. For the full architecture and data flow, see [docs/agent-flow.md](../agent-flow.md).

**Public surface:** All symbols listed below are re-exported from `pipeline.agent.__init__` for direct import.

---

## Entrypoints

### `run_once()`

```python
def run_once(
    *,
    manifest: ManifestDB,
    source_root: Path,
    runners_for: RunnersFor,
    classify: Classifier,
    build_fix: FixBuilder,
    escalate: Escalator,
    lock: AgentLock | None = None,
    retry_budget: int = DEFAULT_RETRY_BUDGET,
    event_logger: AgentEventLogger | None = None,
) -> AgentResult
```

**Purpose:** Drive one complete agent iteration end-to-end.

**Parameters:**
- `manifest` — Open `ManifestDB` instance; executor reads/writes `agent_runs`, `agent_failures`, `agent_fixes` rows.
- `source_root` — Directory containing raw parquet files; observer scans for pending batches.
- `runners_for` — Callable `(batch_id: str) -> {Layer: LayerRunner}`; loop invokes once per batch to materialize layer runners.
- `classify` — Callable `(exc, layer, batch_id) -> ErrorKind`; typically curried diagnoser with LLM client and budget.
- `build_fix` — Callable `(exc, kind, layer, batch_id) -> Fix | None`; deterministic fix builder or None when no fix module registered.
- `escalate` — Callable `(exc, kind, layer, batch_id) -> None`; invoked when retry budget exhausted or kind is `UNKNOWN`.
- `lock` — Optional `AgentLock`; if None, a fresh lock is acquired (released on return). Acquired before `start_agent_run()`.
- `retry_budget` — Per-`(batch_id, layer, error_class)` retry cap (default 3; see spec D1).
- `event_logger` — Optional `AgentEventLogger` for instrumentation; if None, a no-op logger is used.

**Returns:** `AgentResult` with terminal `RunStatus` and tallies (batches processed, failures recovered, escalations).

**Side Effects:**
- Acquires lock (if not supplied) for the duration of the call.
- Opens one `agent_runs` row with status `IN_PROGRESS`; closes it with final status and tallies.
- For each batch: writes `agent_failures` rows for every retry, and optionally `agent_fixes` rows if a fix is applied.
- Appends structured events to the agent event log (via `event_logger`).

---

### `run_forever()`

```python
def run_forever(
    *,
    manifest: ManifestDB,
    source_root: Path,
    runners_for: RunnersFor,
    classify: Classifier,
    build_fix: FixBuilder,
    escalate: Escalator,
    interval: float = DEFAULT_LOOP_INTERVAL_S,
    max_iters: int | None = None,
    stop_event: threading.Event | None = None,
    lock: AgentLock | None = None,
    retry_budget: int = DEFAULT_RETRY_BUDGET,
    event_logger: AgentEventLogger | None = None,
) -> list[AgentResult]
```

**Purpose:** Run `run_once()` repeatedly, sleeping between iterations.

**Parameters:** Same as `run_once()`, plus:
- `interval` — Sleep duration in seconds between iterations (default 60.0; see spec D5).
- `max_iters` — Cap on iteration count; if None (default), runs indefinitely.
- `stop_event` — Optional `threading.Event`; when set, wakes the loop instantly instead of waiting out the remaining interval (spec O3).

**Returns:** List of `AgentResult` for every completed iteration (crashed iterations are excluded).

**Side Effects:** Same as `run_once()` per iteration. Sleep is implemented via `Event.wait()` so SIGINT/SIGTERM handlers can set `stop_event` for graceful shutdown.

---

## Types

### `ErrorKind` (Enum)

```python
class ErrorKind(StrEnum):
    SCHEMA_DRIFT = "schema_drift"
    REGEX_BREAK = "regex_break"
    PARTITION_MISSING = "partition_missing"
    OUT_OF_RANGE = "out_of_range"
    UNKNOWN = "unknown"
```

**Failure taxonomy:** The first four are auto-correctable by a deterministic `Fix` module. `UNKNOWN` bypasses the retry budget and escalates immediately (no retries spent).

- `SCHEMA_DRIFT` — Bronze schema changed (new/missing column, type mismatch, malformed parquet).
- `REGEX_BREAK` — Silver regex failed to match a known format.
- `PARTITION_MISSING` — Expected file or partition absent from disk.
- `OUT_OF_RANGE` — Numeric value outside declared bounds.
- `UNKNOWN` — No pattern matched or LLM budget exhausted; escalate without retry.

---

### `Layer` (Enum)

```python
class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
```

Pipeline layer the agent currently drives. Execution order is canonical: Bronze → Silver → Gold.

---

### `RunStatus` (Enum)

```python
class RunStatus(StrEnum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    INTERRUPTED = "INTERRUPTED"
    FAILED = "FAILED"
```

Lifecycle states for an `agent_runs` row (spec §4.1). `IN_PROGRESS` is the live state; the other three are terminal.

---

### `AgentResult` (Dataclass)

```python
@dataclass(frozen=True)
class AgentResult:
    agent_run_id: str
    batches_processed: int
    failures_recovered: int
    escalations: int
    status: RunStatus
```

Return value of `run_once()`. Status is the terminal run status written to `agent_runs.status`.

---

### `Fix` (Dataclass)

```python
@dataclass(frozen=True)
class Fix:
    kind: str
    description: str
    apply: Callable[[], None]
    requires_llm: bool = False
```

Deterministic recovery action emitted by the diagnoser. The `apply()` callable must be idempotent — the executor re-runs the layer entrypoint after applying it, and a partial second application must not corrupt the manifest or data.

---

### `FailureRecord` (Dataclass)

```python
@dataclass(frozen=True)
class FailureRecord:
    batch_id: str
    layer: Layer
    error_class: ErrorKind
    attempts: int
    last_fix_kind: str | None
    last_error_msg: str
    escalated: bool
```

One row of `manifest.agent_failures` (spec §4.2). `last_error_msg` is sanitized and capped to 512 chars by the caller.

---

## Components

### `Executor` Class

```python
class Executor:
    def __init__(
        self,
        *,
        manifest: ManifestDB,
        agent_run_id: str,
        classify: Classifier,
        build_fix: FixBuilder,
        escalate: Escalator,
        retry_budget: int = DEFAULT_RETRY_BUDGET,
    ) -> None: ...

    def run_with_recovery(
        self,
        *,
        layer: Layer,
        batch_id: str,
        fn: Callable[[], None],
    ) -> RecoveryResult: ...
```

**Role:** Retry-budget-aware runner for one layer entrypoint (spec §6). Invokes `fn()`, catches exceptions, classifies them, records failures, applies fixes, and retries — all bounded by a per-`(batch_id, layer, error_class)` budget.

**Invariants:**
- Budget tracking is per-`(batch_id, layer, error_class)`, not global. One layer's failures do not consume another layer's retries.
- `fn()` is assumed idempotent; re-execution after a fix must be safe.
- `UNKNOWN` errors escalate immediately without consuming retries.

---

### `Planner` Module

```python
def plan(
    batch_id: str,
    *,
    manifest: ManifestDB,
    runners: Mapping[Layer, LayerRunner],
) -> list[tuple[Layer, LayerRunner]]
```

**Role:** Determine which layers in `runners` still need work for a given `batch_id`, in canonical Bronze → Silver → Gold order (spec §5).

**Invariants:**
- Pure function: no side effects, no logging beyond debug level.
- Returns only the subset of `runners` that have not completed for this batch.
- Missing runners for pending layers are silently skipped.

---

### `Observer` Module

```python
def scan(
    manifest: ManifestDB,
    source_root: Path,
    *,
    now: datetime | None = None,
    stale_after_s: float = DEFAULT_STALE_AFTER_S,
) -> list[str]
```

**Role:** Detect pending batches by comparing the source directory against the manifest (spec §4).

**Parameters:**
- `manifest` — Reads `batches` table only; `agent_runs` and `agent_failures` are owned by executor.
- `source_root` — Directory containing raw parquet files; one parquet = one prospective batch.
- `now` — Wall-clock for deterministic tests (default: `datetime.now(tz=UTC)`).
- `stale_after_s` — Window after which an `IN_PROGRESS` row is treated as crash debris and re-scheduled (default 1h; spec §4).

**Returns:** List of pending `batch_id` values, sorted lexicographically. Pending includes missing, terminal-failed, or stale-in-progress rows.

---

### `Diagnoser` Module

```python
def classify(
    exc: BaseException,
    *,
    layer: Layer,
    batch_id: str,
    llm_client: _LLMClientProto | None = None,
    budget: _DiagnoseBudget | None = None,
) -> ErrorKind
```

**Role:** Two-stage failure classifier (spec §7):

1. **Stage 1 (deterministic):** Pattern-match on exception type and message. Covers ~90% of expected failures without LLM.
2. **Stage 2 (LLM fallback):** When no pattern fires, query the LLM with error context. Any parse error collapses to `UNKNOWN`.

**Parameters:**
- `llm_client` — Structural type matching `LLMClient.cached_call(system, user, max_tokens, temperature) -> object` with a `.text` attribute.
- `budget` — Per-`run_once` cap on LLM calls (default 10; spec F4-RF-10). `None` budget disables LLM fallback.

**Returns:** One of five `ErrorKind` values. Deterministic matches are logged at `info`; LLM fallbacks at `info` or `warning` (budget exhausted).

---

### `AgentLock` Class

```python
class AgentLock:
    def __init__(
        self,
        path: Path | str = _DEFAULT_LOCK_PATH,
        stale_after_s: float = _DEFAULT_STALE_AFTER_S,
    ) -> None: ...

    def acquire(self) -> None: ...
    def release(self) -> None: ...

    @property
    def held(self) -> bool: ...

    def __enter__(self) -> AgentLock: ...
    def __exit__(self, exc_type, exc, tb) -> None: ...
```

**Role:** PID-based filesystem lock to ensure only one agent iteration runs at a time.

**Invariants:**
- Use as a context manager (`with AgentLock()`) so `release()` runs on exit, even on uncaught exceptions.
- Direct `acquire()`/`release()` is also supported for cross-function handoff.
- Stale takeover: if the lock file is older than `stale_after_s` (default 1h; design §10), the lock is assumed abandoned and forcibly acquired.
- The lock is stored at `state/agent.lock` by default.

---

### `Escalator` Module

```python
def make_escalator(
    *,
    log_path: Path = DEFAULT_LOG_PATH,
    manifest: ManifestDB | None = None,
    now: Callable[[], datetime] | None = None,
) -> Callable[[BaseException, ErrorKind, Layer, str], None]
```

**Role:** Factory that returns a curried escalator matching the `Escalator` signature (spec §14).

```python
def escalate(
    *,
    exc: BaseException,
    kind: ErrorKind,
    layer: Layer,
    batch_id: str,
    log_path: Path = DEFAULT_LOG_PATH,
    manifest: ManifestDB | None = None,
    now: datetime | None = None,
) -> dict[str, str]
```

**Role:** Run the full escalation: build payload, append JSON event to logs, emit structured warning, optionally flip the run to `FAILED`.

**Returns:** Escalation payload (dict) so callers can inspect emitted content.

**Side Effects:**
- Appends one JSON event to `logs/agent.jsonl`.
- Logs a structured warning with batch_id, layer, error_class, and suggested fix.
- If `manifest` is supplied, marks the latest `agent_runs` row for this batch as `FAILED`.

---

## Invariants

- **Per-batch isolation:** One batch's escalation or failure does not stop other batches. The executor processes each batch independently; the planner re-schedules batches that fail to complete.
- **Retry budget per error class:** Budget is per `(batch_id, layer, error_class)`. A batch that hits the budget for `regex_break` in Silver does not affect Gold or other error classes.
- **Stale-takeover lock:** An `AgentLock` file older than `AGENT_LOCK_STALE_AFTER_S` (default 1h) is forcibly acquired, assuming the previous holder crashed.
- **All events appended:** Every iteration logs to `logs/agent.jsonl` in JSON format (one event per line). No events are overwritten.
- **Idempotent fixes:** Layer entrypoints and `Fix.apply()` are both idempotent; re-running after a partial failure is safe.

---

## Cross-References

- **[docs/agent-flow.md](../agent-flow.md)** — Full architecture, data flow diagram, and design rationale.
- **[docs/api/errors.md](../api/errors.md)** — Error types and exception hierarchy.
- **F4 Specification** — Design document referenced throughout (F4-RF-05, spec §3, etc.).
