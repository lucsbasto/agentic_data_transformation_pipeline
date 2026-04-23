---
name: agentic-loop
description: Agent loop patterns for the self-healing pipeline. Covers observe→diagnose→act→verify cycle, retry budget, deterministic escalation, and loop safety. Use when building or reviewing any autonomous/agentic component. Trigger keywords: agent, loop, retry, escalate, watch, auto-correct, self-heal, scheduler, diagnose.
---

# Agentic loop

Binding patterns for the pipeline agent (PRD §F4, ADR-004, ADR-006).

## 1. Loop anatomy

Single-threaded loop. Phases, in order, every tick:

1. **Observe** — read manifest, detect new batches, detect failed runs.
2. **Decide** — pick next action: `ingest`, `transform:<layer>`, `retry:<run_id>`, `noop`.
3. **Act** — execute deterministic step; LLM called only inside `diagnose`.
4. **Verify** — check postconditions (row counts, schema, manifest row).
5. **Sleep** — `PIPELINE_LOOP_SLEEP_SECONDS` before next tick.

Each phase logs start + end with `run_id`. No phase may silently skip.

## 2. Retry budget (hard limit)

- Per logical task: `PIPELINE_RETRY_BUDGET` (default 3).
- Counter persisted in manifest. Survives process restart.
- On budget exhaustion → **escalate**, don't retry silently.

## 3. Escalation ladder (deterministic)

Ordered, no LLM voting on ordering:

1. **Retry with same inputs** (attempts 1..budget).
2. **Retry with fallback LLM** (`qwen3-coder-plus`) if failure class is LLM-related.
3. **Diagnose** — single LLM call with error + last 20 log lines + schema context. Returns structured `{root_cause, suggested_fix, confidence}`.
4. **Auto-fix** — apply fix only if `confidence >= 0.8` AND fix is in allowlist of safe ops (e.g., regex adjustment, schema coercion, retry with smaller batch). Never edit code automatically.
5. **Human escalation** — write to manifest + structured log with full context. Loop continues on other tasks.

Each transition is logged with reason. No skipping rungs.

## 3.1 Allowlist of auto-fixes

Only these mutations are allowed without human approval:

- Retry with reduced batch size.
- Retry with relaxed schema (nullable cast) if source added null.
- Regex swap from a prompt-time versioned alternate list.
- Skip a single poison row and continue (recorded in manifest as `skipped_rows`).

Anything else → human queue.

## 4. Diagnose LLM call

- Single call per failure. Never loops on itself.
- Input: error type, error message, last 20 structured log lines, expected vs actual schema.
- Output: Pydantic model `DiagnosisReport`.
- If parse fails → auto-fail and escalate to human. Do not retry diagnose.

## 5. Idempotency

- Every Act is idempotent keyed by `(run_id, step)`. Re-running the same step produces same output.
- Bronze writes use `batch_id` partition (overwrite safe).
- Silver/Gold writes are full-layer rewrites per run (simpler than CDC for this scope).

## 6. State

- All agent state in `state/manifest.db` (ADR-003). Rows: `batches`, `runs`, `retries`, `diagnoses`, `escalations`.
- No in-memory state between ticks except config. Everything survives process crash.

## 7. Loop safety rails

- Max ticks per hour cap (configurable). Prevents runaway cost.
- Max LLM calls per run cap. Prevents agent self-loop via retries chaining.
- Circuit breaker: N consecutive failures on same task → pause that task, continue others.
- `KeyboardInterrupt` → flush manifest, log `agent.stopped reason=signal`, exit clean.

## 8. Observability per tick

Emit these events (structlog):
- `agent.tick.start` with `tick_id`, `pending_tasks`.
- `agent.decide` with chosen action.
- `agent.act.start` / `agent.act.end` with duration.
- `agent.verify.pass` / `agent.verify.fail`.
- `agent.escalate` with rung + reason.
- `agent.tick.end` with tick duration + next wake time.

## 9. Testing the agent

- Unit-test Decide phase with fabricated manifest rows.
- Inject synthetic failures (corrupt parquet, schema drift, LLM timeout) → assert correct escalation rung reached.
- Snapshot the log stream for a scripted scenario → golden file.
- Never test with real sleep; inject `sleep` as dependency.

## 10. Demo scenario (M4)

Reproducible script: corrupt one row in source parquet → run `watch` → agent detects, diagnoses, skips row, logs escalation, continues. Stored as `demos/auto_correct_demo.sh`.

## 11. Anti-patterns (do not)

- No LLM in Decide phase. Deciding what to do next is deterministic.
- No nested loops. One phase → one action.
- No retry without incrementing budget counter.
- No "retry forever" branches.
- No fix that modifies source data or code. Fixes = retries with adjusted params only.
- No shared mutable state across ticks.
