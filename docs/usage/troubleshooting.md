# Troubleshooting Guide

Common failures in the Bronze → Silver → Gold pipeline and agent loop, with root causes and fixes.

---

## Symptom: `AgentBusyError` on `agent run-once`

**Root cause:** Another agent process holds the lock file, or a stale lock remains from a crashed agent.

**Fix:**

The agent uses a PID-based filesystem lock at `state/agent.lock`. When `run-once` starts, it calls `AgentLock.acquire()`:

- **Another agent is running:** The lock is held by a live process with a fresh timestamp (modified within 1 hour, default). Wait for that process to finish, then retry.
- **Stale lock from a crash:** The recorded PID is dead, or the lock file is older than 1 hour. The agent automatically overwrites stale locks on the next `run-once`.
- **Manual cleanup:** If you need to force-clear the lock immediately, remove the file:
  ```bash
  rm state/agent.lock
  ```

See [docs/agent-flow.md § 1. Lock acquisition](../agent-flow.md#1-lock-acquisition) for the full decision tree.

---

## Symptom: `SchemaDriftFixError` — Agent could not auto-repair Bronze partition

**Root cause:** Agent detected a schema mismatch (extra columns, missing columns, or type change) in a Bronze partition and the repair failed.

**Fix:**

1. **Inspect the failure:** Check `logs/agent.jsonl` for the escalation event. It includes the batch_id, layer, and a ≤512-char error message.

2. **Check `agent_failures` table:** Query the manifest SQLite:
   ```sql
   SELECT failure_id, batch_id, layer, error_class, last_error_msg
   FROM agent_failures
   ORDER BY detected_at DESC LIMIT 5;
   ```

3. **Review the affected Bronze partition:** Inspect the parquet file to see which columns or types don't match the expected schema.

4. **Options:**
   - If the schema drift is intentional (e.g., a source added a new column), update the Bronze schema definition in `src/pipeline/bronze/schema.py` and retry.
   - If the source data is malformed, clean the source data and trigger a new ingest via `python -m pipeline bronze --batch-id <id>`.

See [docs/agent-flow.md § 6. Fix dispatch](../agent-flow.md#6-fix-dispatch) for how fixes are selected.

---

## Symptom: `SilverRegexMissError` — Regex pattern stopped matching

**Root cause:** A regex pattern that normally extracts fields from message text no longer matches the message format (format changed, data drift).

**Fix:**

1. **Check the override file:** The agent may have already generated a corrected regex:
   ```bash
   cat state/regex_overrides.json
   ```
   The file is keyed by `[batch_id][pattern_name]` and contains the new regex pattern and its validation hash.

2. **Verify it matches your data:** Test the regex against a sample of the failing messages:
   ```python
   import json
   import re
   with open("state/regex_overrides.json") as f:
       overrides = json.load(f)
   # Look up your batch_id and pattern name
   pattern = overrides["<batch_id>"]["<pattern_name>"]
   re.match(pattern, "sample message")
   ```

3. **Silver code will use the override automatically:** When Silver runs next, it reads `state/regex_overrides.json` via `pipeline.silver.regex.load_override()` before falling back to compiled defaults.

4. **To force regeneration:** If the override is stale, delete it and re-run the agent:
   ```bash
   rm state/regex_overrides.json
   python -m pipeline agent run-once
   ```

See [docs/silver-flow.md § 5f-bis. Extract analytical dimensions (regex lane)](../silver-flow.md#5f-bis-extract-analytical-dimensions-regex-lane) for the regex extraction pipeline.

---

## Symptom: `SilverOutOfRangeError` — Row value outside validation range

**Root cause:** A row contains a numeric field (e.g., `valor_pago_atual_brl`) that falls outside its declared valid range. Rather than silently including bad data, Silver quarantines the row.

**Fix:**

1. **Inspect quarantine partition:** Find rows that were rejected:
   ```bash
   ls data/silver/batch_id=<batch_id>/rejected/
   ```
   Read the parquet to see which rows failed and why:
   ```python
   import polars as pl
   df = pl.read_parquet("data/silver/batch_id=<batch_id>/rejected/part-0.parquet")
   print(df)
   ```

2. **Check the rejection reason:** The `reject_reason` column indicates what violated the contract (e.g., `null_message_id`, `null_conversation_id`, or a numeric out-of-range flag added in future versions).

3. **Fix upstream:** Correct the source data in Bronze and re-ingest via `python -m pipeline bronze --batch-id <id>`.

4. **Acknowledge quarantine:** The agent's `out_of_range` fix module checks that the quarantine partition exists and has ≥1 row. Once fixed, re-run the agent:
   ```bash
   python -m pipeline agent run-once
   ```

See [docs/silver-flow.md § 3b. Quarantine](../silver-flow.md#3b-quarantine--split-valid-from-rejected-before-any-transform) for the full quarantine lane design.

---

## Symptom: DashScope API errors (401, quota exceeded, timeout)

**Root cause:** The LLM client cannot reach DashScope or has insufficient quota/credentials.

**Fix:**

1. **Check your API key and endpoint:**
   - Verify `ANTHROPIC_API_KEY` is set and valid:
     ```bash
     echo $ANTHROPIC_API_KEY
     ```
   - For non-standard DashScope setups, check that the base URL is configured in your environment or `src/pipeline/llm/client.py`.

2. **Check quota:**
   - Log into the DashScope dashboard and verify your account has remaining quota.
   - If quota is exhausted, purchase more or wait for the billing period to reset.

3. **Check network:** Ensure your machine can reach the DashScope endpoint:
   ```bash
   curl -I https://api.dashscope.aliyun.com
   ```

4. **Retry with backoff:** The LLM client has a built-in retry budget. If the error is transient (e.g., a timeout), re-run the agent:
   ```bash
   python -m pipeline agent run-once
   ```

5. **If retries are exhausted:** The agent escalates the failure to `logs/agent.jsonl`. See [docs/agent-flow.md § 5. Classification](../agent-flow.md#5-classification) for how LLM errors are classified and handled.

---

## Symptom: Gold output is empty or missing tables

**Root cause:** Silver upstream failed or produced no output, so Gold has nothing to aggregate.

**Fix:**

1. **Check Silver status:** Query the manifest:
   ```sql
   SELECT run_id, batch_id, status, duration_ms, rows_in, rows_out
   FROM runs
   WHERE layer = 'silver'
   ORDER BY started_at DESC LIMIT 1;
   ```

2. **If Silver status is FAILED:**
   - Check `logs/agent.jsonl` for the failure reason.
   - Check `agent_failures` table for details.
   - Fix the upstream Silver issue (see other symptoms in this guide) and re-run Silver.

3. **If Silver status is COMPLETED but rows_out is 0:**
   - Check that Bronze contains data for the target batch:
     ```bash
     ls data/bronze/batch_id=<id>/
     ```
   - If Bronze is empty, re-ingest via `python -m pipeline bronze --batch-id <id>`.

4. **Gold requires Silver to be COMPLETED:** Gold's preflight checks that the upstream Silver run exists and has status COMPLETED. If you skip Silver, Gold will fail immediately.

See [docs/gold-flow.md § 1. Preflight & idempotency](../gold-flow.md#1-preflight--idempotency) for the full preflight contract.

---

## Symptom: Lock file orphaned after crash

**Root cause:** An agent process crashed (e.g., SIGKILL, out of memory) before it could clean up `state/agent.lock`.

**Fix:**

1. **Automatic recovery:** The lock mechanism has a `stale_after_s` timeout (default 1 hour). If the lock file's modification time is older than 1 hour, the next `agent run-once` will automatically overwrite it.

2. **Manual cleanup (immediate):** If you need to free the lock immediately without waiting:
   ```bash
   rm state/agent.lock
   ```

3. **Verify the old process is gone:** Before removing the lock, confirm that the recorded PID is no longer alive:
   ```bash
   # Get the PID from the lock file
   cat state/agent.lock
   # Check if that PID is running
   ps -p <PID>
   # If not found, it's safe to remove the lock
   rm state/agent.lock
   ```

See [docs/agent-flow.md § 1. Lock acquisition](../agent-flow.md#1-lock-acquisition) for details on stale lock detection.

---

## Symptom: `logs/agent.jsonl` growing unbounded

**Root cause:** No built-in log rotation; the append-only JSON event log grows indefinitely.

**Fix:**

1. **Monitor the file size:**
   ```bash
   du -h logs/agent.jsonl
   ```

2. **Manual rotation:** When the file gets large (e.g., >100MB), archive it and start fresh:
   ```bash
   # Compress the current log
   gzip logs/agent.jsonl
   # Rename with a timestamp
   mv logs/agent.jsonl.gz "logs/agent.jsonl.$(date +%Y%m%d-%H%M%S).gz"
   # Next agent run will create a new agent.jsonl
   ```

3. **Parse and filter:** If you need to extract specific events before archiving:
   ```python
   import json
   with open("logs/agent.jsonl") as f:
       for line in f:
           if line.strip():
               event = json.loads(line)
               if event.get("event") == "escalation":
                   print(event)
   ```

4. **Future work:** Automatic log rotation via a background daemon or cron job is a follow-up task. For now, manage rotation manually.

See [docs/agent-flow.md § 7. Escalation](../agent-flow.md#7-escalation) for the events written to `logs/agent.jsonl`.

---

## Further Reading

- [docs/agent-flow.md](../agent-flow.md) — Full agent loop walkthrough (lock, observe, diagnose, fix, escalate).
- [docs/silver-flow.md](../silver-flow.md) — Silver transform pipeline and quarantine lane.
- [docs/gold-flow.md](../gold-flow.md) — Gold aggregation and persona classification.
- `src/pipeline/errors.py` — Complete exception hierarchy.
- `src/pipeline/agent/diagnoser.py` — Two-stage error classification (deterministic + LLM fallback).
