# Debug Quick-Triage Runbook

Symptom-first lookup for the most common failure modes during a demo or local run. Each entry shows the **error message you will actually see**, the **root cause**, and the **single command** that fixes it.

Designed to be readable in 60 seconds while sweating during a live call.

---

## A. Setup / Environment

### A1. `ModuleNotFoundError: No module named 'pipeline'`
**Cause**: Forgot to install dependencies, or running outside the repo root.
**Fix**:
```bash
cd "/media/lucas/Novo volume/Pipeline de Transformação Agêntica de Dados"
uv sync
```

### A2. `click.UsageError: agent runtime config invalid — set DASHSCOPE_API_KEY`
**Cause**: `.env` is missing or `DASHSCOPE_API_KEY` not exported.
**Fix**:
```bash
# verify the file exists
ls -la .env
# verify the key is non-empty
grep -E '^DASHSCOPE_API_KEY=.+$' .env || echo "MISSING — fix .env"
# verify Python can read it
uv run python -c "from pipeline.config import Settings; print('OK', Settings().DASHSCOPE_API_KEY[:6])"
```
If still missing, copy from `.env.example` and fill the key:
```bash
cp .env.example .env  # only if .env does not yet exist
$EDITOR .env
```

### A3. `pydantic.ValidationError` on Settings load
**Cause**: A required field was renamed or a value has the wrong type.
**Fix**:
```bash
# print the exact missing/invalid field
uv run python -c "from pipeline.config import Settings; Settings()"
```
Look at the offending field name in the traceback, fix `.env`, retry.

### A4. `FileNotFoundError: data/raw/conversations_bronze.parquet`
**Cause**: Source parquet was never downloaded or got deleted.
**Fix**:
```bash
ls -la data/raw/
# if missing, restore from the spec link
# https://drive.google.com/file/d/1rt-bLmt3ccR0_BL424WcAGpUMh14PKiO/view
```

### A5. `OSError: [Errno 28] No space left on device`
**Cause**: Bronze/silver/gold parquets accumulated across runs.
**Fix** (preserves source, wipes derived):
```bash
rm -rf data/bronze data/silver data/gold logs/*.jsonl state/manifest.db
mkdir -p data/bronze data/silver data/gold logs state
```

---

## B. Lock / Concurrency

### B1. `BlockingIOError: [Errno 11] Resource temporarily unavailable` on `AgentLock.acquire`
**Cause**: A previous `pipeline agent` process crashed without releasing the lock, or another process is still running.
**Diagnose**:
```bash
ls -la state/agent.lock 2>/dev/null
ps aux | grep -E 'pipeline (agent|gold|silver|ingest)' | grep -v grep
```
**Fix** (only if no other process is running):
```bash
rm -f state/agent.lock
```

### B2. `sqlite3.OperationalError: database is locked`
**Cause**: Two processes wrote to the manifest at the same time, or a previous run did not close the connection cleanly.
**Diagnose**:
```bash
fuser state/manifest.db 2>/dev/null
```
**Fix**: kill the holder, or wait — SQLite WAL mode usually resolves this in seconds. If persistent:
```bash
# WAL files left behind by an SIGKILL'd process
ls -la state/manifest.db-*
# safe only if no process is holding the DB:
rm -f state/manifest.db-wal state/manifest.db-shm
```

---

## C. Schema drift

### C1. `pipeline.errors.SchemaDriftError: gold.lead_profile schema mismatch: missing column 'sentiment'`
**Cause**: Reading an old (pre-F5) parquet under post-F5 code, or the writer was bypassed.
**Fix**: re-run Gold for the affected batch:
```bash
# find the offending batch
ls data/gold/lead_profile/
# rerun
uv run python -m pipeline gold run-once --batch-id <batch_id>
# or wipe + replay
rm -rf data/gold && uv run python -m pipeline agent run-once
```

### C2. `pipeline.errors.SchemaDriftError: <table> ... unexpected column '<x>'`
**Cause**: Source data added a column the schema does not declare.
**Fix**: open `src/pipeline/schemas/<layer>.py`, decide if the column is wanted; either add it to the dtype dict or update the upstream extractor to drop it. Then re-run.

### C3. Polars `ComputeError: column 'X' is missing from the DataFrame`
**Cause**: A Silver/Gold step expected a column that did not survive a previous step.
**Fix**: trace which step dropped it. Typical suspects:
- `transform.py:_apply_persona_and_intent_score` `.select([...])` (the F5 fix lives here).
- `lead_profile.py` placeholder additions.
Search for `.select(` or `.drop(` near the failing function name.

---

## D. LLM / DashScope

### D1. `LLMCallError: provider returned 401`
**Cause**: API key is invalid or expired.
**Fix**: regenerate at the DashScope console, update `.env`, restart.

### D2. `LLMCallError: provider returned 429`
**Cause**: Rate limit. Expected during heavy backfill.
**Fix**: lower concurrency or wait. The agent already retries:
```bash
# check how many retries happened
grep llm_failed logs/*.jsonl | tail -5
```
If recurring, drop the per-batch budget:
```bash
export PIPELINE_LLM_MAX_CALLS_PER_BATCH=200
uv run python -m pipeline gold run-once
```

### D3. `LLMCallError: timeout`
**Cause**: Provider slow or network stalled.
**Fix**: same as D2 — retry budget absorbs it. If pathological, check connectivity:
```bash
curl -sS https://dashscope-intl.aliyuncs.com/compatible-mode/v1/messages -o /dev/null -w '%{http_code}\n' \
  -H "Authorization: Bearer $DASHSCOPE_API_KEY"
```
Expect `405` (POST-only) or `400`, not `0` or `5xx`.

### D4. `KeyError: 'persona'` after parse_classifier_reply
**Cause**: LLM returned valid JSON without the `persona` key. The parser handles this — should not crash. If you see this, it is a regression in `parse_classifier_reply` itself.
**Fix**: run `pytest tests/unit/test_gold_sentiment.py -k parse` to confirm the parser tests still pass.

### D5. `persona.llm_invalid` warnings flooding logs
**Cause**: Prompt rewrite degraded model compliance with the JSON contract.
**Diagnose**: pull a sample bad reply from the cache:
```bash
uv run python <<'PY'
import sqlite3
con = sqlite3.connect('state/llm_cache.db')
for k, t in con.execute("SELECT key, response_text FROM llm_cache LIMIT 5"):
    print('---', k); print(t)
PY
```
If the model is now wrapping in markdown that the fence-stripper does not catch, expand `_FENCE_RE` in `src/pipeline/gold/persona.py:400`.

---

## E. Manifest / Idempotence

### E1. `pipeline agent run-once` reports `batches_processed: 0` but you expected work
**Cause**: All batches are already COMPLETED.
**Diagnose**:
```bash
uv run python <<'PY'
import sqlite3
con = sqlite3.connect('state/manifest.db')
for row in con.execute("SELECT batch_id, bronze_status, silver_status, gold_status FROM batch_manifest"):
    print(row)
PY
```
**Fix**: to force a re-run, flip statuses or delete the rows:
```bash
uv run python <<'PY'
import sqlite3
con = sqlite3.connect('state/manifest.db')
con.execute("UPDATE batch_manifest SET gold_status='PENDING'")
con.commit()
print('reset gold statuses')
PY
```

### E2. Manifest reports `ESCALATED` rows
**Cause**: Three retries failed for some batch + layer.
**Diagnose**:
```bash
tail -20 logs/agent-escalations.jsonl
```
**Fix**: read the JSON line, identify the cause, fix it, then reset the row:
```bash
uv run python <<'PY'
import sqlite3
con = sqlite3.connect('state/manifest.db')
con.execute("UPDATE batch_manifest SET silver_status='PENDING' WHERE batch_id=?", ("<id>",))
con.commit()
PY
```

---

## F. Tests

### F1. `pytest` reports failures in `test_perf_*.py` or `test_agent_observer.py`
**Cause**: Pre-existing WIP files (perf scenarios + agent observer changes) on this branch. Not F5.
**Fix**: ignore the WIP failures. Run only the gold-related suite to confirm:
```bash
uv run pytest tests/unit/ -k gold --no-cov
```
Should be 292 green.

### F2. `Required test coverage of 80% not reached`
**Cause**: pytest-cov enforces a global threshold. Running a subset trips it.
**Fix**: pass `--no-cov`:
```bash
uv run pytest tests/unit/test_gold_sentiment.py --no-cov
```

### F3. mypy / ruff red on F5 files
**Cause**: A patch broke type or lint rules.
**Diagnose**:
```bash
uv run ruff check src/pipeline/gold/ src/pipeline/schemas/
uv run mypy src/pipeline/gold/ src/pipeline/schemas/
```
**Fix**: apply the suggested change, re-run.

---

## G. Live demo emergencies

### G1. Demo machine cannot reach DashScope
**Fallback**: pivot to unit tests. They use a fake LLM client and prove the wiring.
```bash
uv run pytest tests/unit/test_gold_persona_llm.py tests/unit/test_gold_sentiment.py --no-cov -v | tail -30
```
Talk through the test that mocks the LLM. Show how dependency injection makes this possible.

### G2. Polars OOM on a small machine
**Cause**: Loading the full 153K-row parquet into memory eagerly during a `print(df)`.
**Fix**: never `print(df)` without a slice. Always:
```python
print(df.head(5))
print(df.filter(<predicate>).head(10))
```

### G3. Audience asks a question I have no answer for
**Fix**: don't bluff.
> "I don't have that committed to memory. The relevant file should be `<best guess>`. Want me to open it and reason through it?"

Honest beats wrong every time.

---

## H. Recovery checklist after a botched run

If a run produced bad data and you want a clean slate:

```bash
# 1. stop any running agent
pkill -f 'pipeline agent' 2>/dev/null

# 2. release locks if any
rm -f state/agent.lock

# 3. wipe derived data
rm -rf data/bronze data/silver data/gold

# 4. wipe manifest (keep cache because it costs money)
rm -f state/manifest.db state/manifest.db-wal state/manifest.db-shm

# 5. wipe logs
rm -f logs/*.jsonl

# 6. recreate dirs
mkdir -p data/bronze data/silver data/gold state logs

# 7. confirm clean
ls data/bronze data/silver data/gold

# 8. replay
uv run python -m pipeline agent run-once
```

The LLM cache is preserved — re-runs over the same prompts hit cache and cost nothing.

---

## I. Where to look for clues

| Clue | File |
|---|---|
| Which step failed | last line of `logs/agent-events-*.jsonl` |
| Why a batch escalated | `logs/agent-escalations.jsonl` |
| What status each batch is in | `state/manifest.db` (table `batch_manifest`) |
| What the LLM returned | `state/llm_cache.db` (table `llm_cache`, column `response_text`) |
| What schema a parquet has | `uv run python -c "import polars as pl; print(pl.read_parquet('<path>').schema)"` |
| Whether a test would have caught it | grep `tests/unit/` for the function name |

---

## J. Calm-down protocol

If everything is on fire 5 minutes before the call:

1. **Breathe.** A demo failure is recoverable. A flustered demo is not.
2. **Show the tests.** They run in 3 seconds and prove the system works.
3. **Show the code.** Walk the call chain on the screen. They wanted to see your design anyway.
4. **Defer the live run.** "I had this working an hour ago — let me show you the architecture and we can re-run after the call if needed."

The interviewer is judging *your reasoning*, not the demo. The reasoning is in the code and the docs. Lead with that.
