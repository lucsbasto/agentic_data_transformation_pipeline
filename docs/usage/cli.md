# CLI Reference

Complete command-line reference for the Agentic Data Transformation Pipeline.

## Top-Level Command

```
python -m pipeline [OPTIONS] COMMAND [ARGS]...
```

**Description:** Agentic data transformation pipeline (Bronze → Silver → Gold).

**Available commands:** `ingest`, `silver`, `gold`, `agent`

**Global options:**

| Flag | Type | Description |
|------|------|-------------|
| `--version` | boolean | Show package version and exit. |
| `--help` | boolean | Show help text and exit. |

---

## ingest

**Synopsis:** Read raw parquet, cast to Bronze, and register the batch.

**Description:** Reads a raw parquet file, applies schema validation and type casting to conform to the Bronze layer schema, and registers the batch in the manifest database. Assigns a unique batch_id and stores metadata for downstream Silver and Gold processing.

### Flags

| Flag | Type | Default | Required | Description |
|------|------|---------|----------|-------------|
| `--source` | file path | (project_root)/data/raw/* | No | Raw parquet file to ingest into Bronze. Must be an existing file. |
| `--bronze-root` | directory | `data/bronze` | No | Root directory under which Bronze partitions are written. |

### Examples

```bash
# Ingest with defaults
python -m pipeline ingest --source data/raw/events.parquet

# Ingest with custom Bronze root
python -m pipeline ingest --source data/raw/events.parquet --bronze-root /opt/data/bronze
```

---

## silver

**Synopsis:** Transform a completed Bronze batch into a Silver partition.

**Description:** Reads a Bronze batch that is marked COMPLETED in the manifest, applies Silver-layer transformations (row filtering, column selection, LLM-based extraction if configured), and writes the result to the Silver partition. Rejects invalid rows to a quarantine table. Fails immediately if the Bronze batch is not found or not COMPLETED.

### Flags

| Flag | Type | Default | Required | Description |
|------|------|---------|----------|-------------|
| `--batch-id` | string | — | Yes | Bronze batch_id to transform. Must be COMPLETED in the manifest. |
| `--bronze-root` | directory | `data/bronze` | No | Root directory containing Bronze `batch_id=<id>/` partitions. |
| `--silver-root` | directory | `data/silver` | No | Root directory under which Silver partitions are written. |

### Examples

```bash
# Transform a completed batch
python -m pipeline silver --batch-id batch_001

# Transform with custom root directories
python -m pipeline silver --batch-id batch_001 --bronze-root /data/bronze --silver-root /data/silver
```

---

## gold

**Synopsis:** Transform a completed Silver batch into the Gold tables + insights.

**Description:** Reads a Silver batch that is marked COMPLETED in the manifest, applies Gold-layer transformations (aggregations, persona classification, insight generation), and writes the result as Gold tables and JSON insights. Fails immediately if the Silver batch is not found or not COMPLETED.

### Flags

| Flag | Type | Default | Required | Description |
|------|------|---------|----------|-------------|
| `--batch-id` | string | — | Yes | Silver batch_id to transform. Must be COMPLETED in the manifest. |
| `--silver-root` | directory | `data/silver` | No | Root directory containing Silver `batch_id=<id>/` partitions. |
| `--gold-root` | directory | `data/gold` | No | Root directory under which Gold tables and insights are written. |

### Examples

```bash
# Transform a completed batch
python -m pipeline gold --batch-id batch_001

# Transform with custom root directories
python -m pipeline gold --batch-id batch_001 --silver-root /data/silver --gold-root /data/gold
```

---

## agent

**Synopsis:** Run the agent loop for autonomous error diagnosis and remediation.

**Description:** The agent subcommand group contains commands for autonomous error detection, diagnosis via LLM, and remediation across Bronze, Silver, and Gold layers. Two modes: `run-once` (single iteration) and `run-forever` (continuous loop).

### Shared Flags (both run-once and run-forever)

These flags are inherited by all agent subcommands:

| Flag | Type | Default | Env Var | Description |
|------|------|---------|---------|-------------|
| `--source-root` | directory | `data/raw` | — | Directory containing raw source parquet files. |
| `--bronze-root` | directory | `data/bronze` | — | Bronze partition root. |
| `--silver-root` | directory | `data/silver` | — | Silver partition root. |
| `--gold-root` | directory | `data/gold` | — | Gold table root. |
| `--manifest-path` | file path | `state/manifest.db` | — | SQLite manifest database path. |
| `--retry-budget` | integer | `3` | `AGENT_RETRY_BUDGET` | Retry budget per (batch_id, layer, error_class). |
| `--diagnose-budget` | integer | `10` | `AGENT_DIAGNOSE_BUDGET` | LLM diagnose call cap per run_once. |
| `--lock-path` | file path | `state/agent.lock` | `AGENT_LOCK_PATH` | Filesystem lock path. |

---

## agent run-once

**Synopsis:** Run one iteration of the agent loop and exit.

**Description:** Scans all layers (Bronze, Silver, Gold) for failed runs, diagnoses errors via LLM if the diagnose budget permits, applies fixes, and marks runs as COMPLETED or FAILED. Exits after one iteration.

### Flags

Inherits all shared agent flags (listed above).

### Environment Variable Overrides

| Env Var | Type | Default | Related Flag |
|---------|------|---------|--------------|
| `AGENT_RETRY_BUDGET` | integer | `3` | `--retry-budget` |
| `AGENT_DIAGNOSE_BUDGET` | integer | `10` | `--diagnose-budget` |
| `AGENT_LOCK_PATH` | string | `state/agent.lock` | `--lock-path` |

### Examples

```bash
# Run once with defaults
python -m pipeline agent run-once

# Run once with custom retry budget
python -m pipeline agent run-once --retry-budget 5

# Run once, override budget via environment variable
AGENT_DIAGNOSE_BUDGET=20 python -m pipeline agent run-once

# Run once with custom lock path
python -m pipeline agent run-once --lock-path /var/lock/agent.lock
```

---

## agent run-forever

**Synopsis:** Run the agent loop continuously.

**Description:** Runs the agent loop in an infinite loop, repeating the error detection, diagnosis, and remediation cycle at fixed intervals. Stops when interrupted (SIGINT/Ctrl+C) or when `--max-iters` is reached.

### Flags

Inherits all shared agent flags (listed above), plus:

| Flag | Type | Default | Env Var | Description |
|------|------|---------|---------|-------------|
| `--interval` | float | `60.0` | `AGENT_LOOP_INTERVAL` | Seconds between iterations. |
| `--max-iters` | integer | None | — | Stop after this many iterations (default: run until SIGINT). |

### Environment Variable Overrides

| Env Var | Type | Default | Related Flag |
|---------|------|---------|--------------|
| `AGENT_RETRY_BUDGET` | integer | `3` | `--retry-budget` |
| `AGENT_DIAGNOSE_BUDGET` | integer | `10` | `--diagnose-budget` |
| `AGENT_LOOP_INTERVAL` | float | `60.0` | `--interval` |
| `AGENT_LOCK_PATH` | string | `state/agent.lock` | `--lock-path` |

### Examples

```bash
# Run forever with defaults (60s interval)
python -m pipeline agent run-forever

# Run forever with 30-second interval
python -m pipeline agent run-forever --interval 30.0

# Run forever but stop after 100 iterations
python -m pipeline agent run-forever --max-iters 100

# Run with custom interval via environment variable
AGENT_LOOP_INTERVAL=120 python -m pipeline agent run-forever

# Run with high retry budget and long interval
python -m pipeline agent run-forever --retry-budget 10 --interval 120.0 --max-iters 1000
```

---

## Exit Codes

All commands exit with status code `0` on success. On failure:

| Code | Meaning |
|------|---------|
| `1` | Pipeline error: invalid input, missing dependencies, or transformation failure. |
| Other | Unexpected error; check logs for details. |

**Error logging:** Errors are logged to `pipeline.log` (configured by `pipeline_log_level` in settings).

---

## Environment Variables

The following environment variables override command-line flags when set:

| Env Var | Type | Default | Applies To | Description |
|---------|------|---------|-----------|-------------|
| `AGENT_RETRY_BUDGET` | integer | `3` | `agent run-once`, `agent run-forever` | Retry budget per (batch_id, layer, error_class). |
| `AGENT_DIAGNOSE_BUDGET` | integer | `10` | `agent run-once`, `agent run-forever` | LLM diagnose call cap per run_once. |
| `AGENT_LOOP_INTERVAL` | float | `60.0` | `agent run-forever` | Seconds between agent loop iterations. |
| `AGENT_LOCK_PATH` | string | `state/agent.lock` | `agent run-once`, `agent run-forever` | Filesystem lock path for inter-process synchronization. |

When both a flag and an environment variable are set, the environment variable takes precedence.

---

## Common Workflows

### Ingest a raw file

```bash
python -m pipeline ingest --source data/raw/events.parquet
# Returns: batch_001 registered in manifest
```

### Transform Bronze → Silver → Gold

```bash
# Transform Silver (requires Bronze batch COMPLETED)
python -m pipeline silver --batch-id batch_001

# Transform Gold (requires Silver batch COMPLETED)
python -m pipeline gold --batch-id batch_001
```

### Run agent once to diagnose and fix errors

```bash
python -m pipeline agent run-once
# Returns: JSON with iteration count and final status
```

### Continuous agent loop with custom settings

```bash
AGENT_RETRY_BUDGET=5 AGENT_LOOP_INTERVAL=30 \
  python -m pipeline agent run-forever --max-iters 500
```

---

## Manifest and Status

The manifest database (`state/manifest.db` by default) tracks the status of all batches and runs:

- **ingest**: Creates a batch with status `COMPLETED` if successful.
- **silver**: Reads Bronze status, fails if not `COMPLETED`, writes Silver status.
- **gold**: Reads Silver status, fails if not `COMPLETED`, writes Gold status.
- **agent**: Reads all layer statuses, diagnoses and remediates errors, updates statuses.

Check the manifest to verify batch progress:

```bash
# Example: inspect the manifest (requires sqlite3)
sqlite3 state/manifest.db "SELECT batch_id, layer, status FROM runs ORDER BY created_at DESC LIMIT 10;"
```
