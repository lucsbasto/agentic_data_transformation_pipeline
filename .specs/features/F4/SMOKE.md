# F4 smoke — `pipeline agent run-once` end-to-end

## Round 1 — wiring smoke (2026-04-25)

**Goal:** prove WIRING-1 + WIRING-2 produce a working CLI without a single line of code change between unit-test green and a real `python -m pipeline agent run-once` invocation.

**Setup:** existing repo state — `data/raw/conversations_bronze.parquet` (153k rows) already ingested; `data/bronze/batch_id=d287cbb50cc3/` already populated; `state/manifest.db` records the batch as `COMPLETED`. No env var override.

**Command:**
```
$ python -m pipeline agent run-once
```

**Output (timestamps clipped):**
```
loop_started     agent_run_id=27cda33fb91146e4aa63fda532762388
loop_iteration   pending_count=0
loop_stopped     agent_run_id=27cda33fb91146e4aa63fda532762388
                 batches_processed=0 escalations=0
                 failures_recovered=0 status=COMPLETED
{"agent_run_id":"27cda33fb91146e4aa63fda532762388",
 "batches_processed":0, "failures_recovered":0,
 "escalations":0, "status":"COMPLETED"}
```

**Wall time:** sub-second (NFR-01 ≤ 1s for empty delta — verified).

**What this proves:**
- `Settings.load()` works at agent CLI invocation time.
- `ManifestDB` opens, writes the `agent_runs` row, and closes cleanly (one `IN_PROGRESS` row → `COMPLETED` row).
- `AgentLock` acquires `state/agent.lock` and releases on exit (file absent post-run).
- `observer.scan` walks `data/raw/`, computes batch identity, consults `batches`, and correctly returns `[]` because the only batch is already COMPLETED.
- `loop_started` / `loop_iteration` / `loop_stopped` events all reach `logs/agent.jsonl` with the canonical fields.

**What this does NOT prove yet:**
- The full Bronze→Silver→Gold path through the agent (no pending batches to drive).
- Self-healing on injected faults (separate round).
- RNF-06 SLA timing on the 153k fixture cold-run (~35min per M2 memory; would consume LLM quota).

## Round 2 — cold fixture smoke (DEFERRED)

Trigger by wiping `state/manifest.db` + `data/bronze/`, then `pipeline agent run-once`. Expected wall time: ~35min (Silver LLM extract is the bottleneck; same as M2). Cost: 5000+ LLM calls (cache cold).

Defer until the operator decides the spend is worth pinning the timing number.

## Round 3 — fault-injection demo (DEFERRED)

Trigger sequence:
```
$ python scripts/inject_fault.py --kind schema_drift \
    --target data/bronze/batch_id=d287cbb50cc3/part-0.parquet
$ sqlite3 state/manifest.db \
    "UPDATE batches SET status='FAILED' WHERE batch_id='d287cbb50cc3';"
$ python -m pipeline agent run-once
```

Expected: observer picks up the now-FAILED batch, planner sees no COMPLETED Silver/Gold runs, executor invokes `_run_silver` which fails on the schema-drifted Bronze parquet, diagnoser classifies `SCHEMA_DRIFT`, fix dispatcher returns `schema_drift.build_fix(...)`, fix repairs the parquet, retry succeeds.

Defer until WIRING-3 (F2 PII regex hookup) is in place — currently the `regex_break` arm of the dispatcher returns `None`, so a regex-related fault would escalate immediately rather than self-heal.
