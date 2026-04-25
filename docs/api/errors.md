# Exception Hierarchy

The pipeline organizes all failures into a typed exception hierarchy rooted at `PipelineError`. This enables callers at the system boundary to catch everything with a single `except` clause, while internal code narrows to specific failure modes.

All exceptions are defined in `pipeline.errors`. Agent-specific exceptions live in their respective modules under `pipeline.agent.fixes`.

## Exception Tree

```
PipelineError
├── ConfigError
├── SchemaDriftError
├── IngestError
├── SilverError
│   ├── SilverRegexMissError
│   └── SilverOutOfRangeError
├── GoldError
├── ManifestError
├── LLMError
│   ├── LLMCacheError
│   └── LLMCallError
└── AgentError
    ├── AgentBusyError
    ├── RunnerWiringError
    ├── OutOfRangeFixError
    ├── PartitionMissingFixError
    ├── RegexBreakFixError
    └── SchemaDriftFixError
```

## Exception Reference

### PipelineError

**Parent:** `Exception` (Python built-in)

Base class for every pipeline error. Never raised directly. Callers at the system boundary use `except PipelineError` to catch all pipeline failures in one clause.

**Raise sites:** None (base class only)

**Caught by:** CLI entrypoints in `src/pipeline/cli/*.py`

### ConfigError

**Parent:** `PipelineError`

Invalid or missing configuration at startup (e.g., malformed YAML, missing environment variables, invalid paths).

**Raise sites:**
- `src/pipeline/settings.py` (configuration loading)

**Caught by:** `main()` entrypoint in `src/pipeline/__main__.py`

**Recovery:** None; treated as a fatal startup error requiring manual correction.

### SchemaDriftError

**Parent:** `PipelineError`

The source parquet schema no longer matches the declared Bronze schema. This is a data quality issue indicating upstream changes the pipeline is not configured to handle.

**Raise sites:**
- None in core (raised by the agent diagnoser when the agent escalates a schema mismatch)

**Caught by:** Agent diagnoser (pattern-matches to generate a `SchemaDriftFixError`)

**Recovery:** `ErrorKind.SCHEMA_DRIFT` — agent repairs by re-emitting the partition under the canonical schema (see `SchemaDriftFixError` below)

### IngestError

**Parent:** `PipelineError`

Non-schema failure during Bronze ingest: I/O errors, atomic rename failures, validation failures, or source access problems.

**Raise sites:**
- `src/pipeline/cli/ingest.py:253` - ingest operation failure

**Caught by:** CLI ingest command handler

**Recovery:** User retries the ingest after fixing the underlying I/O or source issue.

### SilverError

**Parent:** `PipelineError`

Non-schema failure during the Bronze to Silver transform: regex failures, range checks, dedup issues, or PII masking errors.

**Raise sites:**
- `src/pipeline/cli/silver.py:138` - generic Silver transform failure
- `src/pipeline/cli/silver.py:145` - batch not registered in manifest
- `src/pipeline/cli/silver.py:147` - manifest retrieval failure
- `src/pipeline/cli/silver.py:230` - Silver layer operation failure

**Caught by:** CLI Silver command handler and agent diagnoser

**Recovery:** Narrow subclasses `SilverRegexMissError` and `SilverOutOfRangeError` trigger agent fixes; generic `SilverError` escalates to unknown.

### SilverRegexMissError

**Parent:** `SilverError`

A Silver-layer regex no longer matches a known message format. Indicates upstream log format changed or the regex definition is stale.

**Raise sites:**
- None in core (raised by agent diagnoser when pattern matching fails)

**Caught by:** Agent diagnoser (pattern-matches to generate a `RegexBreakFixError`)

**Recovery:** `ErrorKind.REGEX_BREAK` — agent regenerates regex via LLM and persists override to `state/regex_overrides.json`.

### SilverOutOfRangeError

**Parent:** `SilverError`

A Silver-layer numeric value fell outside its declared range. Indicates a data quality issue or schema mismatch on a numeric field.

**Raise sites:**
- None in core (raised by agent diagnoser when a value exceeds range bounds)

**Caught by:** Agent diagnoser (pattern-matches to generate an `OutOfRangeFixError`)

**Recovery:** `ErrorKind.OUT_OF_RANGE` — agent acknowledges the quarantine and marks the batch recovered without repairing.

### GoldError

**Parent:** `PipelineError`

Non-schema failure during the Silver to Gold transform: missing partitions, compute errors, or state access failures.

**Raise sites:**
- `src/pipeline/cli/gold.py:156` - Gold transform failure
- `src/pipeline/cli/gold.py:161` - Silver partition not found for batch
- `src/pipeline/cli/gold.py:222` - unexpected exception during Gold operation

**Caught by:** CLI Gold command handler

**Recovery:** User retries the Gold layer after verifying Silver partition exists and state is accessible.

### ManifestError

**Parent:** `PipelineError`

SQLite manifest access failed or the row shape is unexpected. Indicates a corrupt or incomplete state store, schema mismatch, or I/O failure on `state/agent_manifest.db`.

**Raise sites:**
- `src/pipeline/state/manifest.py` (multiple locations for manifest row operations)

**Caught by:** Agent loop (treats as unrecoverable; escalates to fail status)

**Recovery:** None; operator must repair the manifest database or reset state.

### LLMError

**Parent:** `PipelineError`

Base class for LLM client failures after retries are exhausted. Indicates either a provider-side error or cache store failure.

**Raise sites:** None (base class for `LLMCacheError` and `LLMCallError`)

**Caught by:** Agent modules (regex_break.py, schema_drift.py) and escalator

**Recovery:** Subclasses determine recovery strategy.

### LLMCacheError

**Parent:** `LLMError`

The LLM cache store failed to read or write. Indicates I/O failure on `state/llm_cache.db`.

**Raise sites:**
- `src/pipeline/llm/cache.py` (cache operations)

**Caught by:** Agent loop (escalates to fail status)

**Recovery:** None; operator must repair cache or reset state.

### LLMCallError

**Parent:** `LLMError`

The LLM provider returned a non-retryable error (e.g., invalid API key, request timeout after max retries, rate limit). Indicates a provider-side failure or credentials issue.

**Raise sites:**
- `src/pipeline/llm/client.py` (provider calls)

**Caught by:** Agent modules calling the LLM (regex_break.py, schema_drift.py)

**Recovery:** Operator must fix credentials, provider status, or request parameters.

### AgentError

**Parent:** `PipelineError`

Base class for agent-loop failures. Includes lock conflicts, wiring errors, and fix-specific exceptions.

**Raise sites:** None (base class)

**Caught by:** Agent loop and CLI (caught as `PipelineError`)

**Recovery:** Subclasses determine recovery strategy.

### AgentBusyError

**Parent:** `AgentError`

Another agent process already holds `state/agent.lock`. Prevents concurrent agent invocations.

**Raise sites:**
- `src/pipeline/agent/lock.py:78` - when acquiring the lock and another process holds it (PID alive and lock is fresh)

**Caught by:** `run_once()` in `src/pipeline/agent/loop.py`

**Recovery:** Caller retries after the lock holder releases it or the lock becomes stale (default 1 hour).

### RunnerWiringError

**Parent:** `AgentError`

The agent cannot resolve the source parquet for a requested `batch_id`. Happens when the source file vanished or a manifest entry is orphaned.

**Raise sites:**
- `src/pipeline/agent/runners.py:55` - when `_resolve_source()` cannot find a parquet matching the batch_id

**Caught by:** Agent executor (treated as unrecoverable; escalates to fail status)

**Recovery:** Operator must restore the source parquet or correct the batch_id.

### SchemaDriftFixError

**Parent:** `AgentError`

Raised when the Bronze partition expected by the schema drift fix is missing.

**Raise sites:**
- `src/pipeline/agent/fixes/schema_drift.py:98` - when `repair_bronze_partition()` cannot find the partition file

**Caught by:** Agent executor (catch handler re-raises as agent failure)

**Recovery:** None; fix fails and the agent retries from the planner.

### RegexBreakFixError

**Parent:** `AgentError`

Raised when the LLM regeneration cannot produce a valid regex. Occurs when the LLM reply is unparseable, missing the `regex` field, or the regex fails to compile.

**Raise sites:**
- `src/pipeline/agent/fixes/regex_break.py:120` - when `regenerate_regex()` has no samples
- `src/pipeline/agent/fixes/regex_break.py:130` - when LLM reply cannot be parsed as JSON or missing `regex` field
- `src/pipeline/agent/fixes/regex_break.py:137` - when the LLM-proposed regex fails to compile
- `src/pipeline/agent/fixes/regex_break.py:256` - when validation of the regex against fixtures fails

**Caught by:** Agent executor (catch handler re-raises as agent failure)

**Recovery:** None; fix fails and the agent retries from the planner (with a smaller sample if needed).

### PartitionMissingFixError

**Parent:** `AgentError`

Raised when the Bronze partition cannot be recreated. Typically indicates a source mismatch: the path supplied to the fix does not produce the requested `batch_id`.

**Raise sites:**
- `src/pipeline/agent/fixes/partition_missing.py:78` - when `recreate_partition()` detects a batch_id mismatch between source and expected

**Caught by:** Agent executor (catch handler re-raises as agent failure)

**Recovery:** None; fix fails and the agent retries from the planner.

### OutOfRangeFixError

**Parent:** `AgentError`

Raised when no quarantine evidence exists for the batch. The fix refuses to acknowledge a missing quarantine, keeping the executor from marking a real failure as recovered.

**Raise sites:**
- `src/pipeline/agent/fixes/out_of_range.py:73` - when `acknowledge_quarantine()` finds zero rejected rows

**Caught by:** Agent executor (catch handler re-raises as agent failure)

**Recovery:** None; fix fails and the agent retries from the planner.

## ErrorKind Mapping

The agent diagnoser pattern-matches on exceptions and maps them to one of five `ErrorKind` values. The first four are auto-correctable; `UNKNOWN` escalates immediately.

| Exception | ErrorKind | Fix Kind | Fix Module |
|-----------|-----------|----------|------------|
| `SchemaDriftError` | `schema_drift` | `schema_drift_repair` | `pipeline.agent.fixes.schema_drift` |
| `SilverRegexMissError` | `regex_break` | `regenerate_regex` | `pipeline.agent.fixes.regex_break` |
| `SilverOutOfRangeError` | `out_of_range` | `acknowledge_quarantine` | `pipeline.agent.fixes.out_of_range` |
| `ManifestError` (partition missing) | `partition_missing` | `recreate_partition` | `pipeline.agent.fixes.partition_missing` |
| Any other exception | `unknown` | (escalation) | N/A |

## Handling Patterns

### Catch Everything at the Boundary

```python
from pipeline.errors import PipelineError

try:
    run_ingest(config)
except PipelineError as e:
    logger.exception("Pipeline failed", exc_info=e)
    sys.exit(1)
```

### Narrow Handling for Agent Fixes

```python
from pipeline.agent.fixes.schema_drift import SchemaDriftFixError
from pipeline.agent.fixes.regex_break import RegexBreakFixError

try:
    result = repair_bronze_partition(path, schema)
except SchemaDriftFixError as e:
    logger.warning("Schema mismatch; partition missing", exc_info=e)
    # Attempt recovery or escalate
except RegexBreakFixError as e:
    logger.warning("Regex compilation failed", exc_info=e)
    # Attempt recovery or escalate
```

### Layer-Specific Handling

```python
from pipeline.errors import SilverError, GoldError

try:
    run_silver(config, batch_id)
except SilverError as e:
    logger.error("Silver transform failed", exc_info=e)
    # Silver-specific recovery
except GoldError as e:
    logger.error("Gold transform failed", exc_info=e)
    # Gold-specific recovery
```

## Cross-References

- **Agent loop design:** [docs/agent-flow.md](../agent-flow.md)
- **Agent types and ErrorKind enum:** [src/pipeline/agent/types.py](../../src/pipeline/agent/types.py)
- **Troubleshooting guide:** [docs/usage/troubleshooting.md](../usage/troubleshooting.md)
- **State store manifest schema:** [src/pipeline/state/manifest.py](../../src/pipeline/state/manifest.py)
