# Code Review Report — F4 Agent Core (loop + auto-correção)

**Files Reviewed:** 18 (10 in `src/pipeline/agent/`, 1 CLI, 1 errors, 2 manifest/schemas, 1 silver/regex, 1 script, 2 docs)
**Test State:** 866 tests passing.
**Recommendation:** COMMENT — no CRITICAL issues; two HIGH should land as a follow-up commit but do not block the milestone.

## Summary

The F4 agent core is well-structured, with clean separation of concerns across observer / planner / executor / diagnoser / escalator modules. The dependency-injection approach makes every module independently testable and the retry-budget loop is logically sound. The escalator's JSON event format matches the spec's seven required keys. All four fix modules are present with idempotent `apply()` semantics and atomic writes. The `_logging.py` injectable `Clock` correctly supports NFR-06 deterministic replay.

Two HIGH-severity issues exist: (1) `run_once` catches `BaseException` but re-raises, which means `KeyboardInterrupt` during the batch loop sets `status=FAILED` instead of `INTERRUPTED`, violating F4-RF-09; (2) the executor accesses the manifest's private `_require_conn()` method, breaking encapsulation. No CRITICAL issues found.

## High

### [HIGH-1] `KeyboardInterrupt` / `SIGINT` sets `FAILED` instead of `INTERRUPTED` — violates F4-RF-09

**File:** `src/pipeline/agent/loop.py:131-133`

**Issue:** The `except BaseException` block unconditionally sets `status = RunStatus.FAILED` and re-raises. `KeyboardInterrupt` is a `BaseException`, so a `SIGINT` mid-iteration produces `agent_runs.status = 'FAILED'` rather than the spec-required `'INTERRUPTED'`. The `INTERRUPTED` variant of `RunStatus` is defined but never assigned anywhere. No signal handler wires `stop_event.set()` for `run_forever`.

**Fix:** Split the `except` into two branches:

```python
except KeyboardInterrupt:
    status = RunStatus.INTERRUPTED
    raise
except BaseException:
    status = RunStatus.FAILED
    raise
```

And wire a `signal.signal(SIGINT, ...)` handler in `run_forever` (or in the CLI command) that calls `stop_event.set()` and sets status to `INTERRUPTED`.

### [HIGH-2] Executor accesses private `ManifestDB._require_conn()` — leaky abstraction

**File:** `src/pipeline/agent/executor.py:222`

**Issue:** `_latest_failure_id` reaches into `self._manifest._require_conn()` to execute a raw SQL query. This bypasses the manifest's public API, couples the executor to SQLite implementation details, and would silently break if the manifest ever moved to a different backend.

**Fix:** Add a public method to `ManifestDB`:

```python
def latest_agent_failure_id(
    self, *, batch_id: str, layer: str, error_class: str
) -> str | None:
```

that encapsulates the `ORDER BY attempts DESC LIMIT 1` query, then call it from the executor.

## Medium

### [MEDIUM-1] CLI imports private `_DiagnoseBudget` from diagnoser

**File:** `src/pipeline/cli/agent.py:32`

**Issue:** `from pipeline.agent.diagnoser import _DiagnoseBudget` — leading underscore marks this internal. Coupling to a non-contracted name.

**Fix:** Either rename to `DiagnoseBudget` (drop underscore) and add to `__init__.py` exports, or expose a factory `make_classifier(budget_cap=10) -> Classifier`.

### [MEDIUM-2] Retry budget is per-invocation, not cross-run — spec ambiguity

**File:** `src/pipeline/agent/executor.py:113-117`

**Issue:** Budget counter starts at zero each `run_with_recovery` call. `count_agent_attempts()` exists at `manifest.py:724` but is never called. Same `(batch_id, layer)` failing once per `run_once` invocation retries indefinitely across successive `run_once` calls.

**Fix:** Document whether budget is per-invocation (current) or cumulative. If cumulative, query `count_agent_attempts(...)` at the start of `run_with_recovery` and offset the local counter.

### [MEDIUM-3] `AgentLock.acquire()` has a TOCTOU window

**File:** `src/pipeline/agent/lock.py:70-81`

**Issue:** Two processes calling `acquire()` concurrently could both read `recorded = None`, both decide the lock is free, and both proceed to `_write_self_pid()`. Last writer wins but both believe they hold the lock. ADR-008 acknowledges this as acceptable for "single-operator tool".

**Fix:** Acceptable given the documented constraint. If multi-process safety is ever needed, switch to `open(path, 'x')` (exclusive create) or `fcntl.flock()`. Add a comment noting the assumption.

### [MEDIUM-4] Escalator `_set_run_failed` passes `duration_ms=0` — loses timing data

**File:** `src/pipeline/agent/escalator.py:117-118`

**Issue:** Hardcoded `duration_ms=0`. Misleading — the layer did consume wall time across retries.

**Fix:** Propagate real elapsed time from executor into escalator, or use `None`/`-1` as sentinel meaning "not measured".

### [MEDIUM-5] Non-deterministic `agent_run_id` from `uuid.uuid4()` breaks strict NFR-06 replay

**File:** `src/pipeline/state/manifest.py:591`

**Issue:** Random UUID makes byte-identical replay impossible for `agent_runs` rows and any log lines carrying `agent_run_id`. Clock injection solves timestamps but not IDs.

**Fix:** Accept an optional `agent_run_id` parameter (default `uuid.uuid4().hex`) so tests can inject a deterministic value.

## Low

### [LOW-1] `run_forever` does not survive a transient `run_once` failure

**File:** `src/pipeline/agent/loop.py:195-206`

**Issue:** If `run_once` raises, exception propagates out. One transient failure aborts the whole `run_forever`.

**Fix:** Wrap `run_once` in try/except inside `run_forever` that logs, appends a `FAILED` result, and continues.

### [LOW-2] `_DiagnoseBudget` is mutable — lacks thread safety annotation

**File:** `src/pipeline/agent/diagnoser.py:56-76`

**Issue:** Plain `int` field for `used`, incremented by `consume()`. Not `frozen`, not thread-safe, not documented as single-threaded-only.

**Fix:** Add docstring note: "Not thread-safe; budget is scoped to one `run_once` invocation on a single thread."

### [LOW-3] `discover_source_batches` sorts twice

**File:** `src/pipeline/agent/observer.py:46-49`

**Issue:** `sorted(source_root.glob("*.parquet"))` produces sorted paths, then `pairs.sort(key=lambda pair: pair[0])` re-sorts by `batch_id`. First sort is redundant.

**Fix:** Drop the `sorted()` wrapper on the glob call.

### [LOW-4] `inject_out_of_range` uses fragile dict mutation for template row

**File:** `scripts/inject_fault.py` (`inject_out_of_range`)

**Issue:** `template = df.row(0, named=True)` returns dict. Mutate then `pl.DataFrame([template], schema=df.schema)`. Brittle.

**Fix:** Use Polars expressions: `df.head(1).with_columns(pl.lit("valor_pago_atual_brl=-999").alias("message_body"))`.

## Positive Observations

- **Clean dependency injection everywhere.** Executor, loop, diagnoser, escalator, event logger all accept injected collaborators. The `Protocol`-based `_LLMClientProto` in diagnoser.py is particularly elegant.
- **Atomic write pattern consistently applied.** `schema_drift.py`, `regex_break.py`, `_logging.py` all use temp-then-rename.
- **Two-stage diagnoser is well-designed.** Deterministic match first (free, fast), LLM fallback second (budgeted). Prompt-version embedding rotates LLM cache keys automatically.
- **Per-batch isolation (F4-RF-08) correctly implemented.** `break` on escalation exits per-layer loop but `continue` keeps per-batch loop going.
- **Spec compliance is strong.** All 12 functional requirements have corresponding code paths. `agent_failures` / `agent_runs` DDL matches spec §4 exactly.
- **Escalator suggested-fix table covers all five kinds.** No `KeyError` possible.
- **Observer staleness detection is robust.** Malformed timestamps treated as stale (conservative); timezone-naive values get UTC pinned.

## Focus Area Verdicts

### 1. Executor retry/recovery loop logic correctness
Sound. Edge cases handled: `UNKNOWN` short-circuits, `None` fix escalates, fix failure logs but re-enters. Concern: per-invocation budget vs cross-run (MEDIUM-2).

### 2. SOLID / clean-code / coupling
Excellent SRP across modules. Only encapsulation breach is HIGH-2 (`_require_conn`). CLI's import of `_DiagnoseBudget` (MEDIUM-1) is a minor DIP concern. No God Objects, no feature envy, no shotgun surgery.

### 3. Concurrency / race conditions
`AgentLock` has a theoretical TOCTOU window (MEDIUM-3), acceptable per ADR-008. SQLite manifest writes serialized via `BEGIN/COMMIT`. No thread-safety issues in current architecture.

### 4. NFR-06 byte-stable replay
Injectable `Clock` covers timestamps. `agent_run_id` uses random UUID (MEDIUM-5). Sorted observer output ensures batch order stability. Replay achievable for tests that inject both clock and ID.

### 5. Per-batch isolation (F4-RF-08)
Correctly implemented. `batch_had_failure` flag + `continue` pattern is clean.

### 6. Retry-budget cap (F4-RF-04..F4-RF-06)
Per-invocation budget works correctly within one `run_once`. Cross-run budget is ambiguous (MEDIUM-2). `mark_agent_failure_escalated` flips the right row.

### 7. Test gaps not yet covered
- No test for `KeyboardInterrupt` setting `INTERRUPTED` (HIGH-1).
- No integration test for `run_forever` with `stop_event` signal handler.
- No test for concurrent `AgentLock.acquire()` from two processes.
- No test for `run_forever` resilience to transient `run_once` failure (LOW-1).
- Fix modules have unit tests but no end-to-end wiring test (blocked by `_empty_runners` — known scope cut).
