# Agent flow — observe → diagnose → act → verify, step by step

Audience: engineers picking up the F4 self-healing agent for the first time. The PRD (§7-§8) specifies *what* the agent must do; this doc walks through *how* the code actually does it, file by file, from `python -m pipeline agent run-once` to the JSON event that lands in `logs/agent.jsonl`.

Every section ends with the exact file/function you can jump to if you want to see the implementation. Keep this doc in sync when the sequence changes — reviewers rely on it to sanity-check PRs.

---

## 0. Entry point: `python -m pipeline agent run-once`

File: `src/pipeline/__main__.py` → `cli.add_command(agent)`

Click dispatches `agent` to the group in `pipeline.cli.agent`, which exposes two subcommands:

| Subcommand | Purpose |
| --- | --- |
| `agent run-once` | Single iteration. Exit 0 on `COMPLETED`, 1 otherwise. |
| `agent run-forever` | Repeat `run-once` every `--interval` seconds (default 60). |

Shared flags + matching env vars:

| Flag | Env var | Default |
| --- | --- | --- |
| `--source-root` | — | `data/raw` |
| `--manifest-path` | — | `state/manifest.db` |
| `--retry-budget` | `AGENT_RETRY_BUDGET` | `3` |
| `--diagnose-budget` | `AGENT_DIAGNOSE_BUDGET` | `10` |
| `--lock-path` | `AGENT_LOCK_PATH` | `state/agent.lock` |
| `--interval` (run-forever) | `AGENT_LOOP_INTERVAL` | `60` |
| `--max-iters` (run-forever) | — | unlimited |

The CLI builds the four collaborator callables (`runners_for`, `classify`, `build_fix`, `escalate`) and passes them into the loop module. **Note:** the production runner factory is intentionally `_empty_runners` in this commit — see §6 for what that means in practice.

**File:** `src/pipeline/cli/agent.py`.

---

## 1. Lock acquisition

File: `src/pipeline/agent/lock.py` — `AgentLock`.

Before any work, the loop calls `AgentLock.acquire()`:

- missing file → write own PID, done.
- corrupt / empty file → overwrite (treated as stale).
- recorded PID == own PID → reentrant; no-op.
- recorded PID is alive (`os.kill(pid, 0)`) AND mtime is fresh → raise `AgentBusyError`.
- recorded PID is dead OR mtime is older than `stale_after_s` (default 1h) → overwrite (stale takeover).

Stale takeover via mtime catches the PID-rotation edge case where the OS reused the recorded PID for an unrelated process.

`release()` only unlinks when the recorded PID matches `os.getpid()`. The loop wraps `acquire/release` in a `try/finally` so even a `KeyboardInterrupt` mid-iteration leaves the lockfile cleaned up. SIGINT/SIGTERM signal handlers are not yet wired — the next iteration's `AgentLock` constructor will detect any leftover lock as stale within the hour.

---

## 2. `agent_runs` row + observer scan

File: `src/pipeline/state/manifest.py` — `start_agent_run`.

A fresh UUID is allocated, an `IN_PROGRESS` row lands in `agent_runs`, and the loop logs `loop_started` (event names live in `pipeline.agent._logging`).

File: `src/pipeline/agent/observer.py` — `scan(manifest, source_root)`.

The observer walks `source_root/*.parquet`, computes each parquet's deterministic `batch_id` via `pipeline.ingest.batch.compute_batch_identity`, and consults the `batches` table. A batch is **pending** iff:

- it has no `batches` row yet, OR
- its row is `FAILED`, OR
- its row is `IN_PROGRESS` AND `started_at < now() - stale_after_s` (default 1h, configurable; corrupt timestamps are treated as stale so a bad row surfaces to humans rather than wedging the loop forever).

Output is sorted lexicographically so the planner sees batches in a predictable order across runs. The loop emits one `loop_iteration` event with `pending_count`.

---

## 3. Per-batch planning

File: `src/pipeline/agent/planner.py` — `plan(batch_id, *, manifest, runners)`.

`LAYER_ORDER = (Layer.BRONZE, Layer.SILVER, Layer.GOLD)` is pinned. The planner returns the subset of `runners` that the manifest does NOT yet show as `COMPLETED`, in canonical order. Skipped layers are logged at `debug` level. The planner is pure — no side effects, no runner invocation — so the executor can wrap each step in retry / fix logic without the planner observing the results.

The loop emits `batch_started` once per batch.

---

## 4. Layer execution with retry budget

File: `src/pipeline/agent/executor.py` — `Executor.run_with_recovery`.

For each `(layer, runner)` pair the executor runs:

1. invoke `runner()` — log `layer_completed`, return `Outcome.COMPLETED`.
2. on raise: classify the exception via the injected classifier (next §), write one row in `agent_failures` with the running attempt count, log `failure_detected`, then either:
    - `UNKNOWN`           → escalate immediately, return `ESCALATED`.
    - no fix registered   → escalate immediately, return `ESCALATED`.
    - fix succeeds        → `record_agent_fix` stamps the kind, log `fix_applied`, loop and retry `runner()`.
    - fix fails           → log a warning, do NOT stamp the row, loop and retry anyway (the fix may have had partial effect).
3. budget exhausted (default 3 per `(batch_id, layer, error_class)`) → mark the latest failure row `escalated=1` (ordered by `attempts DESC` because `ts` has second resolution and ties on a fast test run) and call the escalator once.

When a layer escalates the loop breaks out of the per-layer iteration but keeps processing sibling batches (F4-RF-08 isolation). When the layer completes the loop emits `layer_completed` with the attempt count.

---

## 5. Classification

File: `src/pipeline/agent/diagnoser.py` — `classify(exc, *, layer, batch_id, llm_client=None, budget=None)`.

Stage 1 (deterministic, no LLM cost):

| Exception type | `ErrorKind` |
| --- | --- |
| `pl.SchemaError` / `SchemaFieldNotFoundError` / `ColumnNotFoundError` | `SCHEMA_DRIFT` |
| `SilverRegexMissError` | `REGEX_BREAK` |
| `SilverOutOfRangeError` | `OUT_OF_RANGE` |
| `FileNotFoundError` | `PARTITION_MISSING` |

Stage 2 (LLM fallback, only when stage 1 misses):

A small `error_ctx` dict (`layer`, `batch_id`, `exc_type`, first 1024 chars of `exc_message`) is wrapped in an `<error_ctx untrusted="true">` block and handed to `LLMClient.cached_call` with a system prompt carrying `PROMPT_VERSION_DIAGNOSE=v1`. The reply must parse as `{"kind": "..."}`; anything malformed, off-list, or wrongly shaped collapses to `UNKNOWN`. A per-`run_once` `_DiagnoseBudget` (default 10) caps stage 2 calls so a runaway loop cannot drain the LLM quota.

The CLI's default classifier uses Stage 1 only (`llm_client=None`) to keep the CLI dependency surface narrow.

---

## 6. Fix dispatch

The executor calls `build_fix(exc, kind, layer, batch_id) -> Fix | None`. The four fix modules under `src/pipeline/agent/fixes/` each export a `build_fix(...)` factory:

- `schema_drift.py` — `repair_bronze_partition`: drop extras, fill missing nulls, cast `strict=False`, atomic temp-then-rename. Idempotent.
- `regex_break.py` — `regenerate_regex` via LLM, validate against caller-supplied baseline, persist to `state/regex_overrides.json` keyed by `(batch_id, pattern_name)`. Silver code can read it via `pipeline.silver.regex.load_override`.
- `partition_missing.py` — `recreate_partition` reuses the F1 ingest pipeline (`scan_source` → `transform_to_bronze` → `collect_bronze` → `write_bronze`). Refuses if the source hash produces a different `batch_id` than requested.
- `out_of_range.py` — `acknowledge_quarantine` checks that `silver_root/batch_id=<id>/rejected/part-0.parquet` has ≥1 row.

**Status of CLI dispatch:** the CLI's default `_default_build_fix` returns `None` for every kind in the current commit — the per-kind dispatcher that wires the four `build_fix` factories together is a follow-up task. Until that lands, every classified failure escalates immediately. The fix modules themselves are exercised end-to-end by their own unit tests in `tests/unit/test_fix_*.py`.

---

## 7. Escalation

File: `src/pipeline/agent/escalator.py` — `escalate(...)`.

When the executor decides to escalate it:

1. Calls `mark_agent_failure_escalated(failure_id)` (executor side).
2. Calls the injected escalator (CLI default = `make_escalator(log_path=DEFAULT_LOG_PATH, manifest=...)`), which:
    - builds a canonical event payload (`event="escalation"`, `batch_id`, `layer`, `error_class`, `last_error_msg` (≤512 chars), `suggested_fix`, `ts`),
    - appends one JSON line to `logs/agent.jsonl` (mkdir parent on first use),
    - emits a `structlog.warning` to stdout,
    - flips the latest `runs` row for `(batch_id, layer)` to `FAILED` (no-op if no `runs` row exists or it's already FAILED — preserves the original `error_type`).

The `SUGGESTED_FIX` table maps every `ErrorKind` to a one-line operator hint (design §9). A unit test asserts the table covers every enum value.

---

## 8. End-of-iteration bookkeeping

The `try/finally` block in `loop.run_once`:

- writes the terminal `RunStatus` to `agent_runs.status` (`COMPLETED` on clean exit, `FAILED` on uncaught exception; `INTERRUPTED` is reserved for signal-driven shutdown wiring that is not yet implemented),
- updates the `batches_processed`, `failures_recovered`, `escalations` counters on the `agent_runs` row,
- emits `loop_stopped` with the same fields,
- releases the lock.

The function returns an `AgentResult` dataclass. The CLI prints `json.dumps(asdict(result), default=str)` and exits with the right code.

---

## 9. `run_forever` — long-lived mode

File: `src/pipeline/agent/loop.py` — `run_forever(*, interval, max_iters=None, stop_event=None)`.

Iterates `run_once`, sleeping `interval` seconds between iterations via `threading.Event.wait(interval)`. A SIGINT/SIGTERM handler can call `stop_event.set()` and short-circuit the wait instead of waiting out the remaining interval (design §17 O3). `max_iters=N` runs the loop body N times then returns; the last iteration does NOT sleep (so `max_iters=1` returns immediately even with `interval=60`).

Returns `list[AgentResult]` — one entry per iteration that completed.

---

## 10. Observability cheat sheet

Canonical events (defined in `pipeline.agent._logging`):

| Event | Carried fields (typical) |
| --- | --- |
| `loop_started` | `agent_run_id`, `interval` (when applicable) |
| `loop_iteration` | `pending_count` |
| `batch_started` | `batch_id` |
| `layer_started` | `batch_id`, `layer` |
| `layer_completed` | `batch_id`, `layer`, `attempts` |
| `failure_detected` | `batch_id`, `layer`, `error_class`, `attempt` |
| `fix_applied` | `batch_id`, `layer`, `fix_kind` |
| `escalation` | `batch_id`, `layer`, `error_class`, `last_error_msg`, `suggested_fix` |
| `loop_stopped` | `agent_run_id`, `status`, counter tally |

Every event timestamp is computed from an injectable `Clock` so deterministic-replay tests can pin `ts` and assert byte-identical log output (NFR-06).

---

## 11. Demo: synthetic fault injection

`scripts/inject_fault.py --kind {schema_drift,regex_break,partition_missing,out_of_range} --target <path>` mutates a target Bronze parquet / source parquet / partition dir so the next pipeline pass produces the matching failure. Helpers are importable for unit tests:

- `inject_schema_drift(parquet_path)` — adds an `injected_col`.
- `inject_regex_break(parquet_path)` — replaces the first `message_body` with `❌ R$ 1.500,00`.
- `inject_partition_missing(target)` — `shutil.rmtree` (or `unlink` for a single file).
- `inject_out_of_range(parquet_path)` — appends a row with `valor_pago_atual_brl=-999`.

A full `inject_fault → agent run-once → recovery` E2E demo is blocked on the runner-wiring follow-up (§6 status note); the injection helpers themselves are covered by 12 integration tests.

---

## 12. Open follow-ups

- **Runner wiring** — `_empty_runners` and `_default_build_fix` placeholders in `pipeline.cli.agent` need replacing with adapters that invoke `pipeline.bronze.ingest.run` / `pipeline.silver.transform.run` / `pipeline.gold.transform.run` and dispatch the four `build_fix` factories.
- **Smoke run on the 153k fixture** (F4.20) — needs the wiring above before it can produce meaningful timing.
- **Signal handler integration** — SIGINT/SIGTERM should call `lock.release()` AND `stop_event.set()` cleanly. Currently only the `try/finally` cleanup fires.
- **`had_quarantine` flag on `runs`** — design §8.4 calls for it; `out_of_range` fix currently records the acknowledgment in `agent_failures.last_fix_kind` only.
- **F2 PII regex hookup** — `pipeline.silver.regex.load_override` is callable but `pipeline.silver.extract` does not yet consult it before falling back to its compiled defaults.
