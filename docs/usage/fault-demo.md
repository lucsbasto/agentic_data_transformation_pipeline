# Fault Injection Demo: Agent Self-Healing

Walkthrough the agent loop's detect → diagnose → fix → retry cycle by injecting synthetic faults into a clean pipeline state. This demo proves the agent can recover from four auto-correctable failure modes without human intervention.

## Goal

Observe a real failure, watch the agent classify and repair it, and track escalation when the retry budget is exhausted. This validates the F4 agent architecture and the four-part recovery loop:

1. **Detect** — observer scans data; executor fails on a layer.
2. **Diagnose** — diagnoser classifies the error into one of four `ErrorKind` values.
3. **Fix** — fix dispatcher returns a repair callable; executor applies it and retries.
4. **Escalate** — if retry budget exhausted, escalator writes an event and marks the run `FAILED`.

## Setup: Clean State

Start with a successful pipeline run to establish a clean baseline:

```bash
# 1. Sync deps and configure API key (see docs/usage/quickstart.md if needed)
uv sync
cp .env.example .env
# Edit .env and set ANTHROPIC_API_KEY to your DashScope key

# 2. Ingest Bronze data
uv run python -m pipeline ingest
```

Note the `batch_id` from the output (12-character hex string). Save it:

```bash
BATCH_ID=<batch_id_from_ingest>
```

This batch is now recorded in `state/manifest.db` with status `COMPLETED`. The agent loop will pick up any `FAILED` or `INTERRUPTED` batches in the next step.

## Fault Scenario 1: Schema Drift

**What it simulates:** Bronze parquet file gains an unexpected column. Downstream Silver layer fails because the schema doesn't match the declared Bronze schema.

**Inject the fault:**

```bash
python scripts/inject_fault.py \
  --kind schema_drift \
  --target data/bronze/batch_id=$BATCH_ID/part-0.parquet
```

Expected output:
```
injected schema_drift into data/bronze/batch_id=<id>/part-0.parquet
```

**Mark the batch as failed** so the observer picks it up:

```bash
sqlite3 state/manifest.db \
  "UPDATE batches SET status='FAILED' WHERE batch_id='$BATCH_ID';"
```

**Run the agent loop:**

```bash
uv run python -m pipeline agent run-once
```

**What to expect:**

1. Observer finds the FAILED batch.
2. Planner targets Silver layer (last incomplete layer).
3. Executor tries `_run_silver`; fails on schema mismatch.
4. Diagnoser classifies the error as `SCHEMA_DRIFT`.
5. Fix dispatcher calls `schema_drift.build_fix(...)`, which removes the injected column.
6. Executor retries Silver; succeeds.
7. Logs show: `event="failure_detected"` then `event="fix_applied"` in `logs/agent.jsonl`.
8. `state/manifest.db` agent_failures row has `error_class='schema_drift'` and `escalated=0` (no escalation).

**Inspect the recovery:**

```bash
# View the event log
tail -20 logs/agent.jsonl | grep -E "failure_detected|fix_applied"

# Verify the batch is now COMPLETED
sqlite3 state/manifest.db \
  "SELECT batch_id, layer, status FROM runs WHERE batch_id='$BATCH_ID' ORDER BY started_at DESC;"
```

## Fault Scenario 2: Regex Break

**What it simulates:** PII regex pattern no longer matches expected message format. Silver extraction fails to identify entities because the regex rejects the message body.

**Inject the fault:**

```bash
python scripts/inject_fault.py \
  --kind regex_break \
  --target data/raw/conversations.parquet
```

**Reset the batch status:**

```bash
sqlite3 state/manifest.db \
  "UPDATE batches SET status='FAILED' WHERE batch_id='$BATCH_ID';"
```

**Run the agent loop:**

```bash
uv run python -m pipeline agent run-once
```

**What to expect:**

1. Observer picks up the FAILED batch.
2. Executor runs Silver; regex fails to match, triggering a classification error.
3. Diagnoser marks it as `REGEX_BREAK`.
4. Fix dispatcher calls `regex_break.build_fix(...)`, which regenerates the regex via LLM and persists it to `state/regex_overrides.json`.
5. Executor retries Silver; succeeds with the new regex.
6. `logs/agent.jsonl` shows `event="failure_detected"` with `error_class='regex_break'` and `event="fix_applied"`.
7. `state/regex_overrides.json` contains the new regex pattern.

**Inspect:**

```bash
# Check the regex override was written
cat state/regex_overrides.json | jq .

# Verify recovery
tail -20 logs/agent.jsonl | grep regex_break
```

## Fault Scenario 3: Partition Missing

**What it simulates:** A Bronze partition directory is deleted or inaccessible. The observer cannot read the batch, and the executor must re-ingest from raw.

**Inject the fault:**

```bash
python scripts/inject_fault.py \
  --kind partition_missing \
  --target data/bronze/batch_id=$BATCH_ID
```

Expected output:
```
injected partition_missing into data/bronze/batch_id=<id>
```

**Reset the batch:**

```bash
sqlite3 state/manifest.db \
  "UPDATE batches SET status='FAILED' WHERE batch_id='$BATCH_ID';"
```

**Run the agent loop:**

```bash
uv run python -m pipeline agent run-once
```

**What to expect:**

1. Observer detects the FAILED batch.
2. Executor tries to read Bronze; file not found.
3. Diagnoser marks it as `PARTITION_MISSING`.
4. Fix dispatcher calls `partition_missing.build_fix(...)`, which triggers a re-ingest from `data/raw/conversations.parquet`.
5. Executor retries; Silver now sees the restored Bronze partition and succeeds.
6. `logs/agent.jsonl` shows `event="failure_detected"` with `error_class='partition_missing'` and `event="fix_applied"`.
7. `data/bronze/batch_id=$BATCH_ID/` is restored.

**Inspect:**

```bash
# Confirm the partition is restored
ls -lh data/bronze/batch_id=$BATCH_ID/

# Check the event log
tail -20 logs/agent.jsonl | grep partition_missing
```

## Fault Scenario 4: Out-of-Range Value

**What it simulates:** A row in Bronze contains a value outside the acceptable range (e.g., negative monetary amount). Silver enrichment succeeds, but Gold classification fails on a range check.

**Inject the fault:**

```bash
python scripts/inject_fault.py \
  --kind out_of_range \
  --target data/raw/conversations.parquet
```

**Reset the batch:**

```bash
sqlite3 state/manifest.db \
  "UPDATE batches SET status='FAILED' WHERE batch_id='$BATCH_ID';"
```

**Run the agent loop:**

```bash
uv run python -m pipeline agent run-once
```

**What to expect:**

1. Observer picks up the FAILED batch.
2. Executor runs Silver (succeeds); runs Gold (fails on range validation).
3. Diagnoser marks it as `OUT_OF_RANGE`.
4. Fix dispatcher calls `out_of_range.build_fix(...)`, which quarantines the offending rows to `data/bronze.rejected/`.
5. Executor retries Gold; succeeds (offending rows excluded).
6. `logs/agent.jsonl` shows `event="failure_detected"` with `error_class='out_of_range'` and `event="fix_applied"`.
7. Quarantined rows appear in `data/bronze.rejected/batch_id=$BATCH_ID/`.

**Inspect:**

```bash
# Check the rejected rows
ls -lh data/bronze.rejected/batch_id=$BATCH_ID/

# Verify recovery
tail -20 logs/agent.jsonl | grep out_of_range

# Query the manifest
sqlite3 state/manifest.db \
  "SELECT batch_id, layer, status FROM runs WHERE batch_id='$BATCH_ID' ORDER BY started_at DESC LIMIT 5;"
```

## Escalation Path: Retry Budget Exhausted

When the executor exhausts its retry budget (default: 3 retries per failure) or encounters an `UNKNOWN` error kind with no deterministic fix, it escalates.

**To trigger escalation, force an unrecoverable failure:**

Modify `src/pipeline/agent/dispatcher.py` to return `None` for a known error kind:

```python
# In dispatcher.suggest_fix(), temporarily make schema_drift return None:
if kind == ErrorKind.SCHEMA_DRIFT:
    return None  # No fix available
```

Then run:

```bash
# Reset the batch
sqlite3 state/manifest.db \
  "UPDATE batches SET status='FAILED' WHERE batch_id='$BATCH_ID';"

# Inject a schema_drift fault
python scripts/inject_fault.py \
  --kind schema_drift \
  --target data/bronze/batch_id=$BATCH_ID/part-0.parquet

# Run the agent
uv run python -m pipeline agent run-once
```

**What to expect:**

1. Observer picks up the FAILED batch.
2. Executor fails; diagnoser classifies `SCHEMA_DRIFT`.
3. Dispatcher returns `None` (no fix available).
4. Executor escalates.
5. Escalator writes a JSON event to `logs/agent.jsonl` with:
   - `event="escalation"`
   - `error_class="schema_drift"`
   - `suggested_fix="Verifique delta de schema em logs/agent.jsonl; ..."`
6. `state/manifest.db` agent_failures row has `escalated=1`.
7. The corresponding `runs` row is marked `FAILED`.

**Inspect escalation:**

```bash
# View the escalation event
tail -5 logs/agent.jsonl | jq .

# Check the agent_failures table
sqlite3 state/manifest.db \
  "SELECT batch_id, layer, error_class, escalated, suggested_fix FROM agent_failures WHERE batch_id='$BATCH_ID';"

# Confirm the runs row is FAILED
sqlite3 state/manifest.db \
  "SELECT batch_id, layer, status FROM runs WHERE batch_id='$BATCH_ID' AND layer='silver';"
```

## Cleanup

**Restore the original state:**

Option 1: Git checkout (resets all injected faults and manifest):
```bash
git checkout data/ state/
```

Option 2: Re-ingest only the batch (keeps manifest history):
```bash
python -m pipeline ingest --batch-id $BATCH_ID
```

Option 3: Clear and start fresh:
```bash
rm -rf data/ state/
uv run python -m pipeline ingest
```

## Observability

The agent loop emits structured logs to three locations:

1. **`logs/agent.jsonl`** — Machine-readable event stream (one JSON object per line):
   - `loop_started`, `loop_iteration`, `loop_stopped` — loop lifecycle
   - `failure_detected` — executor caught an exception
   - `fix_applied` — executor retried and succeeded
   - `escalation` — executor gave up and escalated

2. **`state/manifest.db`** — SQLite database:
   - `agent_runs` — one row per agent invocation with status (`IN_PROGRESS`, `COMPLETED`, `FAILED`)
   - `agent_failures` — one row per detected failure with `error_class`, `escalated`, `suggested_fix`

3. **stdout** — Human-readable summary via `structlog`:
   ```
   agent.loop_started       agent_run_id=... 
   agent.failure_detected   batch_id=... error_class=schema_drift
   agent.fix_applied        batch_id=... layer=silver
   agent.loop_stopped       status=COMPLETED failures_recovered=1
   ```

Use `tail -f logs/agent.jsonl` in one terminal while running the agent in another to watch events in real-time.

## Next Steps

- **Architecture deep-dive**: See [docs/agent-flow.md](../agent-flow.md) for the full detect → diagnose → act → verify loop design.
- **CLI reference**: See [docs/usage/cli.md](../usage/cli.md) for all agent subcommands.
- **Configuration**: See [docs/usage/configuration.md](../usage/configuration.md) for retry budgets and error handling tuning.
