# Configuration

The pipeline reads configuration from environment variables via `pydantic-settings`. Configuration is loaded at startup from a `.env` file (if present) and validated against type hints. Missing required fields fail fast with clear error messages.

All configuration values are optional except where noted otherwise.

## Overview

The pipeline uses two configuration sources:

1. **`.env` file** — loaded from the current working directory at startup. Use `.env.example` as a template.
2. **Per-subcommand overrides** — the `pipeline agent` CLI allows overriding specific values via command-line flags or environment variables.

Configuration is immutable after startup. Access it by injecting the `Settings` instance explicitly rather than reading environment variables deep in the call stack.

## Required Variables

These variables must be set; the pipeline will not start without them.

| Variable | Purpose | Example |
|----------|---------|---------|
| `ANTHROPIC_API_KEY` | DashScope API key. Reused by the Anthropic SDK (no custom client wiring needed). | `sk-replace-with-your-dashscope-key` |
| `PIPELINE_LEAD_SECRET` | HMAC-SHA256 key for deriving stable `lead_id` from normalized phone numbers in the Silver layer. Must be at least 16 bytes of cryptographic entropy. Rotating this re-hashes all `lead_id` values on the next run. | Generate with `openssl rand -hex 32` |

## LLM Provider Configuration

### API Endpoint

| Variable | Purpose | Default | Notes |
|----------|---------|---------|-------|
| `ANTHROPIC_BASE_URL` | Anthropic-protocol-compatible endpoint. Override to point at DashScope instead of Anthropic. | `https://coding-intl.dashscope.aliyuncs.com/apps/anthropic` | Validated as a valid HTTP(S) URL at startup. |

### Model Selection

| Variable | Purpose | Default | Notes |
|----------|---------|---------|-------|
| `LLM_MODEL_PRIMARY` | Primary model for classification and analytics extraction. Used for all LLM calls unless the retry budget is exhausted. | `qwen3-max` | Fallback machinery in `src/pipeline/llm/client.py`. |
| `LLM_MODEL_FALLBACK` | Fallback model used only after `LLM_MODEL_PRIMARY` fails and the retry budget is spent. | `qwen3-coder-plus` | Cheaper variant; reduces cost on provider rate limits or transient failures. |

### Response Safety

| Variable | Purpose | Default | Bounds | Notes |
|----------|---------|---------|--------|-------|
| `PIPELINE_LLM_RESPONSE_CAP` | Maximum characters accepted from a single LLM response before it is cached. Protects against hostile or broken upstream responses poisoning the SQLite cache. | `200000` | >= 1 | Enforced in `src/pipeline/llm/cache.py`. |

## Pipeline Runtime

### Retry and Recovery

| Variable | Purpose | Default | Bounds | Notes |
|----------|---------|---------|--------|-------|
| `PIPELINE_RETRY_BUDGET` | Retry attempts per (batch_id, layer, error_class) triple. Controls how many times the executor retries a failed layer before escalating. | `3` | [1, 10] | Set in `src/pipeline/settings.py` line 84. |

### Sleeping and Polling

| Variable | Purpose | Default | Bounds | Notes |
|----------|---------|---------|--------|-------|
| `PIPELINE_LOOP_SLEEP_SECONDS` | Seconds to sleep between iterations in `pipeline agent run-forever`. Spec §7 D5 requires bounds to prevent operator misconfiguration. | `60` | [1, 3600] | Set in `src/pipeline/settings.py` line 85. |

### State and Logging

| Variable | Purpose | Default | Notes |
|----------|---------|---------|-------|
| `PIPELINE_STATE_DB` | SQLite database file path for the manifest (batch status, agent runs, escalations). Relative paths are resolved against the project root. Absolute paths are accepted as-is. `..` segments are rejected to block path traversal attacks. | `state/manifest.db` | Validator in `src/pipeline/settings.py` lines 159–176. |
| `PIPELINE_LOG_LEVEL` | Log level for the pipeline. Normalized to uppercase at startup. | `INFO` | Allowed: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Validator in `src/pipeline/settings.py` lines 146–157. |

### LLM Budget Control

| Variable | Purpose | Default | Bounds | Notes |
|----------|---------|---------|--------|-------|
| `PIPELINE_LLM_MAX_CALLS_PER_BATCH` | Hard cap on LLM calls per Silver batch. Once exhausted, remaining rows are filled with null entity columns and the batch logs `llm.budget_exhausted`. Raise or split the batch if hit in production. See PRD §18.4. | `5000` | >= 1 | Enforced in `src/pipeline/silver/llm_extract.py`. |
| `PIPELINE_LLM_CONCURRENCY` | Max in-flight LLM calls per batch (F3 persona classification lane). Bounded to stay under CPython's ThreadPoolExecutor ceiling and DashScope's per-model quota. Sequential (=1) violates the M2 15-minute SLA on 5000-lead batches. | `16` | [1, 64] | Set in `src/pipeline/settings.py` lines 114–124. |

## Agent Loop Overrides

These variables override CLI defaults for `pipeline agent run-once` and `pipeline agent run-forever`. All are optional and read by Click's `envvar` mechanism.

| Variable | Purpose | Default | CLI Flag | Notes |
|----------|---------|---------|----------|-------|
| `AGENT_RETRY_BUDGET` | Retry budget per (batch_id, layer, error_class). Overrides `--retry-budget` flag. | `3` | `--retry-budget` | Set in `src/pipeline/cli/agent.py` line 120. Executor uses this value in `src/pipeline/agent/executor.py`. |
| `AGENT_DIAGNOSE_BUDGET` | LLM diagnose call cap per `run_once` iteration. Limits pattern-free diagnostic LLM calls. See spec §7 D3. | `10` | `--diagnose-budget` | Set in `src/pipeline/cli/agent.py` line 128. Classifier wraps this via `_DiagnoseBudget` in `src/pipeline/agent/diagnoser.py`. |
| `AGENT_LOOP_INTERVAL` | Seconds between iterations in `pipeline agent run-forever`. Overrides `--interval` flag. | `60.0` | `--interval` | Set in `src/pipeline/cli/agent.py` line 193. Implemented via `threading.Event.wait()` so SIGINT wakes the loop instantly (design §17 O3). |
| `AGENT_LOCK_PATH` | Filesystem lock file path. Prevents concurrent agent runs. | `state/agent.lock` | `--lock-path` | Set in `src/pipeline/cli/agent.py` line 136. Lock is acquired at the start of `run_once()` and released in the `finally` block to guarantee cleanup on SIGINT/SIGTERM. |

## Data Directories

These paths are not configurable via environment variables but are hardcoded defaults in the CLI. Override via command-line flags only.

| Path | Purpose | Default | CLI Flag | Notes |
|------|---------|---------|----------|-------|
| Source root | Directory containing raw parquet files. Scanned for pending batches. | `data/raw` | `--source-root` | Set in `src/pipeline/cli/agent.py` line 48. |
| Bronze root | Bronze partition output directory (cleaned, validated raw data). | `data/bronze` | `--bronze-root` | Set in `src/pipeline/cli/agent.py` line 50. |
| Silver root | Silver partition output directory (normalized, enriched data). | `data/silver` | `--silver-root` | Set in `src/pipeline/cli/agent.py` line 51. |
| Gold root | Gold table root (analytics-ready aggregations). | `data/gold` | `--gold-root` | Set in `src/pipeline/cli/agent.py` line 52. |

## Logging

| Path | Purpose | Notes |
|------|---------|-------|
| `state/agent.jsonl` | Structured agent loop events (iterations, batches, layers). Default escalation log path when no custom path is passed. | Set in `src/pipeline/agent/escalator.py` line 32. |

## State and Locks

| Path | Purpose | Notes |
|------|---------|-------|
| `state/manifest.db` | SQLite manifest database. Tracks batch status, agent runs, escalations, and agent locks. | Created on first access; persists across runs. Locked during `run_once()` to prevent concurrent execution. |
| `state/agent.lock` | Filesystem lock file. Held for the duration of `run_once()` to coordinate multiple agent processes. | Created if missing; platform-dependent (fcntl on Unix, msvcrt on Windows). See `src/pipeline/agent/lock.py`. |
| `state/regex_overrides.json` | (Future) User-defined error classification patterns. Not yet used; reserved for operator customization. | See spec §16 ADR-01. |

## Setting Up `.env`

Start with the provided template:

```bash
cp .env.example .env
# Edit .env and fill in:
#   - ANTHROPIC_API_KEY (your DashScope key)
#   - PIPELINE_LEAD_SECRET (output of `openssl rand -hex 32`)
#   - Optional: any PIPELINE_* or LLM_* overrides for your environment
```

The pipeline will load `.env` automatically at startup. Invalid or missing required variables will raise a `ConfigError` with the root cause.

## Example: Custom Configuration

To run with a custom manifest location and higher concurrency:

```bash
export PIPELINE_STATE_DB=/tmp/my_manifest.db
export PIPELINE_LLM_CONCURRENCY=32
export AGENT_LOOP_INTERVAL=30
pipeline agent run-forever
```

Or via `.env`:

```bash
PIPELINE_STATE_DB=/tmp/my_manifest.db
PIPELINE_LLM_CONCURRENCY=32
AGENT_LOOP_INTERVAL=30
```

Then:

```bash
pipeline agent run-forever
```

## Validation and Errors

All configuration is validated at startup using Pydantic v2. Validation failures include:

- **Type mismatch** — e.g., `PIPELINE_RETRY_BUDGET=abc` instead of an integer.
- **Out-of-bounds** — e.g., `PIPELINE_RETRY_BUDGET=0` (minimum is 1) or `=99` (maximum is 10).
- **Missing required** — e.g., `ANTHROPIC_API_KEY` not set.
- **Invalid path traversal** — e.g., `PIPELINE_STATE_DB=../../evil.db` (rejected to block attacks).

Validation errors are raised as `ConfigError` exceptions with the underlying cause. Fix the environment and try again.

## Reference: Settings Class

The source of truth is `src/pipeline/settings.py`. This file defines:

- All field names, types, defaults, and bounds
- Validators for sensitive fields (log level, state DB path)
- The Pydantic v2 model configuration (case-insensitive, extra vars ignored)

View the file to see the complete, authoritative list of configuration fields.
