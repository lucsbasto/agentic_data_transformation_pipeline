# F4 Critic Review

**Verdict:** ACCEPT-WITH-RESERVATIONS

The F4 agent loop is a well-structured, thoroughly tested implementation that faithfully delivers the observe → diagnose → act → verify cycle described in the spec and design. Module boundaries are clean, injection is done via simple `Callable` type aliases rather than heavyweight Protocol classes, and the 864-test suite covers the critical paths. Three genuine issues warrant reservations: an unimplemented spec requirement (F4-RF-09 SIGINT/INTERRUPTED), a budget-counting deviation from design, and a private API access in the executor.

## Architect Perspective

**Module layout vs design §1.** The design specifies 10 modules under `src/pipeline/agent/`. The actual implementation has 9 files plus a `fixes/` directory. Key deviation: **`state.py` does not exist**. The design says `state.py — agent_runs / agent_failures CRUD + lockfile` but all CRUD operations were merged into `pipeline.state.manifest.ManifestDB` instead. This is a reasonable consolidation (avoids a thin wrapper) but contradicts the documented layout.

`lock.py` was supposed to be part of `state.py` per the design but was correctly extracted as a standalone module (matching the design's own later §10). The `diagnoser.py` merges what the design calls separate "classify" and "build_fix" responsibilities — the design's pseudocode in §6 shows `diagnoser.classify()` and `diagnoser.build_fix()`, and the implementation matches this grouping even though the executor receives them as separate callables.

**Protocol-style injection quality.** Clean. `Classifier`, `FixBuilder`, and `Escalator` are defined as simple `Callable` type aliases in `executor.py:40-51`, not Protocol classes. Lighter-weight and appropriate. The `_LLMClientProto` in `diagnoser.py:79` correctly uses `Protocol` since it needs structural subtyping for the `cached_call` method.

**MAJOR — Private API access in executor** (`executor.py:222`):
```python
conn = self._manifest._require_conn()
```
The `_latest_failure_id` method directly accesses the manifest's private `_require_conn()` to run a raw SQL query. Breaks the encapsulation boundary. Fix: add `ManifestDB.latest_agent_failure_id(batch_id, layer, error_class) -> str | None` and call it from the executor.

## Skeptical Reviewer — Top 5 Untested Failure Modes

1. **SIGINT mid-layer execution.** No signal handler registered. If process killed during `fn()`, `run_once` catches `BaseException` and sets `FAILED` (not `INTERRUPTED`). `try/finally` releases lock and writes agent_run row, but spec requires `INTERRUPTED`. Docs honestly acknowledge this gap.

2. **Retry budget semantic mismatch.** Executor's `while attempts < self._retry_budget` counts ALL failures for a `(batch_id, layer)` invocation regardless of error class. Design says budget counts per `(batch_id, layer, error_class)` triple — each error class should get its own 3-attempt budget. `count_agent_attempts` exists to support design's semantics but is never called.

3. **Concurrent lock acquisition race window.** Between `_read_pid()` and `_write_self_pid()` in `lock.py:acquire()` (lines 70-73), TOCTOU window exists. Two processes checking simultaneously could both see `recorded is None` and both write their PID. Design accepts this as "single-operator tool" but no test exercises the race.

4. **`run_forever` does not catch per-iteration exceptions.** Looking at `loop.py:195-206`, `run_once` is called directly. If it raises (lock failure, manifest corruption), the entire `run_forever` loop crashes. No per-iteration error boundary. Production daemon = transient failure on one iteration kills the long-running agent.

5. **Escalator and AgentEventLogger both write to `logs/agent.jsonl` independently.** Both `escalator.py` (`write_event` using its own `DEFAULT_LOG_PATH`) and `_logging.py` (`AgentEventLogger.event`) append. Currently both default to the same path, but no single source of truth enforces this — a future change giving them different defaults would split events.

## Future Maintainer — `docs/agent-flow.md` Cold Read

**Accurate and helpful.** Sections 0-7 are well-written, file-referenced, and match the code. Observability cheat sheet (§10) and demo instructions (§11) are useful. Open follow-ups (§12) are honest.

**Issues a new engineer would hit:**

1. **§8 says `INTERRUPTED` is "reserved for signal-driven shutdown wiring that is not yet implemented"** — honest but a new engineer would wonder if this is a bug or deferred work. Doc should explicitly cross-reference F4-RF-09 and say it is deferred.

2. **Design §1 module layout lists `state.py`** but it does not exist. A new engineer following the design doc to find CRUD methods would look for `state.py` and be confused.

3. **§4 says "retry budget (default 3 per (batch_id, layer, error_class))"** but the actual implementation counts per `(batch_id, layer)`. A maintainer debugging budget exhaustion would be misled.

4. **No prominent warning that `_empty_runners` means the agent processes zero layers on first run.** §0 + §6 mention it but a new engineer running `pipeline agent run-once` for the first time would see the loop process zero layers and wonder if the agent is broken.

5. **§11 (demo) references `scripts/inject_fault.py` but does not mention fixture requirements.** A new engineer cloning the repo would need to know where to get test parquet files.

## Spec Compliance Auditor

| Requirement | Status | Evidence |
|---|---|---|
| **F4-RF-01** `run_once` reads Bronze, identifies pending batch_ids, dispatches bronze→silver→gold | **PASS** | `loop.py:run_once` calls `scan()` then iterates `plan()` per batch with `LAYER_ORDER = (BRONZE, SILVER, GOLD)` |
| **F4-RF-02** `run_forever(interval, max_iters)` loops with sleep | **PASS** | `loop.py:run_forever` uses `threading.Event.wait(interval)`; `max_iters` cap works |
| **F4-RF-03** Planner skips COMPLETED layers (idempotency) | **PASS** | `planner.py:is_layer_completed` checks `runs` table; `plan()` filters |
| **F4-RF-04** Executor wraps with retry budget, classifies, applies fix | **PARTIAL** | Core mechanism works. Budget counts per `(batch_id, layer)` not per triple as spec implies via design §6. Functionally correct but semantically narrower. |
| **F4-RF-05** ErrorKind enum has all 5 values; UNKNOWN escalates immediately | **PASS** | `types.py:ErrorKind` has all 5; `executor.py:141` short-circuits on UNKNOWN |
| **F4-RF-06** Each attempt records `agent_failures` row; budget exhaustion sets `escalated=1` + `FAILED` | **PASS** | `executor.py:125-132` records failures; `_escalate_and_mark` flips escalated; escalator's `_set_run_failed` marks runs.status |
| **F4-RF-07** Escalation log JSON has required keys | **PASS** | `escalator.py:build_payload` includes `event`, `batch_id`, `layer`, `error_class`, `last_error_msg`, `suggested_fix` |
| **F4-RF-08** Per-batch independence | **PASS** | `loop.py:150-155` breaks inner loop on escalation, continues outer batch loop |
| **F4-RF-09** SIGINT/SIGTERM sets INTERRUPTED, recoverable | **FAIL** | `RunStatus.INTERRUPTED` is defined but never set. `run_once` catches `BaseException` and sets `FAILED`. No signal handler registered. Docs acknowledge as "not yet implemented." |
| **F4-RF-10** LLM diagnose budget-capped at 10/run_once | **PASS** | `diagnoser.py:_DiagnoseBudget` caps at 10; `llm_client.cached_call` used |
| **F4-RF-11** Idempotency: re-running on unchanged Bronze is no-op | **PASS** | Observer skips COMPLETED batches; planner skips COMPLETED layers; no new failures created |
| **F4-RF-12** Demo `inject_fault.py` with 4 kinds | **PASS** | `scripts/inject_fault.py` supports all 4 kinds per task F4.17 |
| **NFR-01** run_once with no delta completes in ≤1s | **PASS** | Only manifest query; no heavy computation on empty delta path |
| **NFR-02** ≤10 LLM calls per run_once | **PASS** | `_DiagnoseBudget(cap=10)` enforced in diagnoser |
| **NFR-03** ≥90% coverage on agent/** | **PARTIAL** | 50+ test files touching agent code; coverage not measured here |
| **NFR-04** No print() in production code | **PASS** | grep confirms zero `print()` calls in `src/pipeline/agent/` |
| **NFR-05** No new dependencies | **PASS** | Only uses Polars, structlog, anthropic, pytest (already in pyproject.toml) |
| **NFR-06** Determinism with same fixture + seed | **PASS** | Injectable `Clock` in `_logging.py`, injectable `now` in observer/escalator, seeded random in property tests |

**Known scope cuts (acknowledged, not scored as failures):**
- CLI placeholders (`_empty_runners`, `_default_build_fix`) pending runner wiring.
- `had_quarantine` flag on `runs` deferred.
- F2 PII regex hookup deferred.
- Smoke F4.20 deferred until runner wiring lands.

## Critical Findings

### F4-RF-09: SIGINT/SIGTERM → INTERRUPTED is unimplemented

- **Evidence:** `RunStatus.INTERRUPTED` defined at `types.py:48` but grep across all agent source shows it is never assigned. `loop.py:131-132` catches `BaseException` and sets `status = RunStatus.FAILED`. No `signal.signal()` handler exists anywhere.
- **Confidence:** HIGH
- **Why:** Spec explicitly requires graceful interruption with `INTERRUPTED` status. SIGINT during `run_once` produces `FAILED` instead.
- **Fix:** Register SIGINT/SIGTERM handlers that set `status = RunStatus.INTERRUPTED` and call `stop_event.set()` for `run_forever`.
- **Realist Check:** Downgraded to MAJOR. `try/finally` releases lock and writes agent_run row, so no state lost. `FAILED` triggers safe idempotent retry. Documented as known follow-up. Worst case is wasted re-run, not data corruption.

## Major Findings

1. **Retry budget counts per `(batch_id, layer)` not per `(batch_id, layer, error_class)` as design §6.**
   - Evidence: `executor.py:117` `while attempts < self._retry_budget` uses single counter. `count_agent_attempts(batch_id, layer, error_class)` exists at `manifest.py:724` but is never called.
   - Why: If Silver fails once with `schema_drift` then twice with `regex_break`, budget exhausted after 3 total attempts. Design intends each error class to get 3 retries independently.
   - Fix: Either change executor to query `count_agent_attempts` per error_class OR update design to reflect per-layer semantics. Latter is simpler and arguably safer (prevents unbounded retries).

2. **Executor accesses private manifest API** (`executor.py:222`) — already detailed in Architect section.

3. **Design module layout lists `state.py` that does not exist.**
   - Evidence: Design §1 lists `state.py — agent_runs / agent_failures CRUD + lockfile`. File does not exist; CRUD lives in `pipeline.state.manifest`.
   - Fix: Update design §1 to remove `state.py` and note CRUD is in `ManifestDB`.

## Minor Findings

1. `run_forever` has no per-iteration error boundary. Single bad iteration kills the daemon. Wrap each iteration in try/except.

2. Escalator and AgentEventLogger both hardcode `Path("logs/agent.jsonl")` independently. Single constant import would be safer.

3. `docs/agent-flow.md` §4 says budget is per triple matching design, but code disagrees. Docs need updating after budget semantics resolve.

4. CLI's `_default_build_fix` returns `None` for every kind — `agent run-once` is effectively non-self-healing until runner wiring lands. Acknowledged as placeholder.

## Top 5 Highest-Leverage Follow-ups

1. **Implement SIGINT/SIGTERM handler.** Closes F4-RF-09. Register handlers in `run_once`/CLI that set `status = RunStatus.INTERRUPTED` and `stop_event.set()` for `run_forever`.

2. **Add `ManifestDB.latest_agent_failure_id()`.** Eliminate `_require_conn()` private access in `executor.py:222`.

3. **Resolve budget semantics.** Decide per-triple (design) or per-layer (code), update losing document, add test exercising chosen semantics with mixed error classes.

4. **Add per-iteration error boundary in `run_forever`.** Wrap `run_once()` in try/except, append `FAILED` AgentResult, log exception, continue.

5. **Update design §1 module layout.** Remove phantom `state.py`, note CRUD lives in `ManifestDB`. Update `docs/agent-flow.md` §4 budget description.
