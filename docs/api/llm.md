# LLM Client API

## Overview

`pipeline.llm` provides a unified LLM client built on the Anthropic Python SDK, configured to use DashScope's Anthropic-compatible endpoint. Every LLM call in the pipeline is mediated through this client, which handles caching, retries, fallback logic, and structured response extraction.

**Key design:**
- **Caching layer:** Request/response pairs are stored in SQLite (`llm_cache` table in `state/manifest.db`). Identical requests are never charged twice.
- **Endpoint override:** The Anthropic SDK's `base_url` parameter points to DashScope instead of Anthropic's servers. The SDK API shape remains identical.
- **Retry + fallback:** Primary model (`qwen3-max`) retries up to 3 times on transient errors; if the budget is exhausted, one final attempt runs on the fallback model (`qwen3-coder-plus`).
- **Observability:** Structured logs emit cache hits, misses, retries, and completions so pipeline operators can track LLM cost and latency.

## Configuration

LLM configuration is set via environment variables at pipeline startup. The settings are validated by `pipeline.settings.Settings` and are immutable during a run.

### Required

| Variable | Type | Default | Purpose |
|----------|------|---------|---------|
| `ANTHROPIC_API_KEY` | `SecretStr` | (none) | DashScope API key. Must be set before any LLM call. Used by `LLMClient._build_anthropic()` to initialize the Anthropic SDK. |

### Optional

| Variable | Type | Default | Purpose |
|----------|------|---------|---------|
| `ANTHROPIC_BASE_URL` | URL | `https://api.dashscope.aliyun.com/compatible-mode/v1` | Endpoint URL for the LLM provider. Override only if switching providers or testing. |
| `LLM_MODEL_PRIMARY` | str | `qwen3-max` | Primary model for all calls. Use a model listed on your DashScope account. |
| `LLM_MODEL_FALLBACK` | str | `qwen3-coder-plus` | Fallback model after primary retries are exhausted. Should be cheaper/faster. |
| `PIPELINE_RETRY_BUDGET` | int | `3` | Maximum retry attempts on transient errors (RateLimitError, APIConnectionError, APITimeoutError). |
| `PIPELINE_LLM_RESPONSE_CAP` | int | `16384` | Maximum response length in characters. Responses longer than this are rejected to prevent cache pollution. |
| `PIPELINE_LLM_MAX_CALLS_PER_BATCH` | int | `5000` | Maximum number of LLM calls per batch run. Used by extraction layers to enforce budget. |

For environment setup, see [docs/usage/configuration.md](../usage/configuration.md).

## Public Client API

### Class: `LLMClient`

The main entry point for all LLM calls.

```python
from pipeline.llm import LLMClient
from pipeline.settings import Settings
from pipeline.llm import LLMCache

settings = Settings.load()
with LLMCache(settings.state_db_path()) as cache:
    client = LLMClient(settings, cache)
    response = client.cached_call(
        system="You are a helpful assistant.",
        user="Extract the vehicle make from: Driving a red Honda Civic.",
        model=None,  # uses LLM_MODEL_PRIMARY
        max_tokens=1024,
        temperature=0.0,
    )
    print(response.text)
    print(f"Cache hit: {response.cache_hit}")
```

#### Constructor

```python
def __init__(
    self,
    settings: Settings,
    cache: LLMCache,
    *,
    anthropic_client: _AnthropicLike | None = None,
    sleeper: Any = time.sleep,
) -> None:
```

**Parameters:**
- `settings`: A `pipeline.settings.Settings` instance. The client reads `anthropic_api_key`, `anthropic_base_url`, `llm_model_primary`, `llm_model_fallback`, and `pipeline_retry_budget` from this.
- `cache`: An opened `LLMCache` instance (typically used as a context manager).
- `anthropic_client` (optional): Injected Anthropic SDK client. Used in tests to pass a fake. If `None`, the client builds a real one from settings.
- `sleeper` (optional): Function called to pause between retries. Defaults to `time.sleep`; tests pass a no-op lambda.

#### Method: `cached_call`

```python
def cached_call(
    self,
    *,
    system: str,
    user: str,
    model: str | None = None,
    max_tokens: int = 1024,
    temperature: float = 0.0,
) -> LLMResponse:
```

Make an LLM call with automatic caching and fallback retry logic.

**Parameters:**
- `system` (required): System prompt (instructions for the model).
- `user` (required): User message (the actual prompt or data to process).
- `model` (optional): Model override. If `None`, uses `settings.llm_model_primary`.
- `max_tokens` (optional): Max output length. Defaults to 1024.
- `temperature` (optional): Sampling randomness (0.0 = deterministic). Defaults to 0.0 for reproducibility.

**Returns:** An `LLMResponse` with `text`, `model`, token counts, `cache_hit` flag, and `retry_count`.

**Behavior:**
1. Computes a SHA256 hash of the request signature (model, system, user, max_tokens, temperature).
2. Checks the cache: if found, returns immediately with `cache_hit=True`.
3. On miss: calls the Anthropic SDK up to `PIPELINE_RETRY_BUDGET` times (with exponential backoff + jitter) on transient errors.
4. If primary model retries are exhausted, attempts one final call on the fallback model.
5. On success, stores the response in cache and returns.
6. On fatal error (authentication, bad request, permission denied), raises `LLMCallError` immediately.

**Logs emitted:**
- `llm.cache_hit` — cache returned a result
- `llm.cache_miss` — cache miss, calling provider
- `llm.retry` — transient error, retrying
- `llm.fallback` — primary retries exhausted, trying fallback
- `llm.completed` — call succeeded (with latency, tokens, attempt number)
- `llm.failed` — call failed (fatal or all retries exhausted)

#### Method: `invalidate`

```python
def invalidate(self, *, prefix: str | None = None) -> int:
```

Drop cached responses, optionally by prefix.

**Parameters:**
- `prefix` (optional): Invalidate only cache keys starting with this string. If `None`, clears the entire cache.

**Returns:** Number of rows deleted.

**Use case:** After updating a system prompt, invalidate rows keyed by the old prompt to force re-execution:
```python
client.invalidate(prefix="v1_")  # Clear all rows starting with "v1_"
```

#### Property: `settings`

```python
@property
def settings(self) -> Settings:
    return self._settings
```

Provides read-only access to the settings object passed at construction.

### Class: `LLMResponse`

Structured result of a single cached or uncached LLM call.

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class LLMResponse:
    text: str                 # Assistant's response text
    model: str                # Model that produced this (may differ if fallback was used)
    input_tokens: int         # Tokens consumed in the request
    output_tokens: int        # Tokens generated in the response
    cache_hit: bool           # True if served from cache
    retry_count: int = 0      # Transient failures before success (0 on first attempt)
```

**Fields:**
- `text`: The assistant's complete response as a string.
- `model`: The actual model name that produced the response (helpful to know if a fallback was used).
- `input_tokens`, `output_tokens`: Token counts from the provider (or cache).
- `cache_hit`: `True` if the response was served from SQLite cache, `False` if fetched from the provider.
- `retry_count`: How many transient errors (RateLimitError, APIConnectionError, APITimeoutError) were encountered before success. Zero on first attempt, increments on each retry.

### Class: `LLMCache`

Thin wrapper over the `llm_cache` SQLite table. Typically used as a context manager.

```python
from pipeline.llm import LLMCache
from pathlib import Path

db_path = Path("state/manifest.db")
with LLMCache(db_path) as cache:
    # Cache is open and ready
    cached = cache.get(cache_key)
    if cached:
        print(f"Hit: {cached.response_text}")
```

#### Constructor

```python
def __init__(self, db_path: Path | str) -> None:
```

**Parameters:**
- `db_path`: Path to the SQLite database file, or the string `":memory:"` for an in-memory cache (tests only).

#### Context Manager Protocol

```python
def open(self) -> LLMCache:
    """Open the SQLite connection. Idempotent."""

def close(self) -> None:
    """Close the connection."""

def __enter__(self) -> LLMCache:
    return self.open()

def __exit__(self, *exc) -> None:
    self.close()
```

Use `with LLMCache(path) as cache:` to automatically manage the connection.

#### Method: `get`

```python
def get(self, cache_key: str) -> CachedResponse | None:
```

Look up a response by cache key.

**Parameters:**
- `cache_key`: SHA256 hex string (64 chars). Usually computed by `compute_cache_key()`.

**Returns:** A `CachedResponse` object or `None` if the key is not found.

#### Method: `put`

```python
def put(
    self,
    *,
    cache_key: str,
    model: str,
    response_text: str,
    input_tokens: int,
    output_tokens: int,
) -> None:
```

Insert a new cached response. Uses `INSERT OR IGNORE`: once a key is stored, the row is frozen. Re-insertion of the same key is a no-op.

**Parameters:**
- `cache_key`: SHA256 hex string.
- `model`: Model name that produced the response.
- `response_text`: The LLM's text output.
- `input_tokens`, `output_tokens`: Token counts from the provider.

#### Method: `invalidate`

```python
def invalidate(self, *, prefix: str | None = None) -> int:
```

Delete cached rows by prefix. If `prefix` is `None`, clears the entire table.

**Parameters:**
- `prefix`: Delete rows whose `cache_key` starts with this string. Prefix is escaped to handle SQL metacharacters.

**Returns:** Number of deleted rows.

### Function: `compute_cache_key`

```python
def compute_cache_key(
    *,
    model: str,
    system: str,
    user: str,
    max_tokens: int,
    temperature: float,
) -> str:
```

Compute a stable SHA256 cache key from request parameters.

**Parameters:**
- `model`: Model name.
- `system`: System prompt.
- `user`: User message.
- `max_tokens`: Max output tokens.
- `temperature`: Sampling temperature (normalized to 6 decimal places).

**Returns:** 64-character hex string.

**Details:**
- Temperature is normalized (e.g., `-0.0` → `0.0`) to ensure semantically identical requests hash to the same key.
- The key is deterministic: identical inputs always produce the same output.
- The key includes all parameters that affect the response, so two requests with different system prompts have different keys.

### Class: `CachedResponse`

Projection of one row from the `llm_cache` table. Returned by `LLMCache.get()`.

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class CachedResponse:
    cache_key: str          # The SHA256 hash
    model: str              # Model that produced the response
    response_text: str      # The LLM's text output
    input_tokens: int       # Tokens consumed
    output_tokens: int      # Tokens generated
    created_at: str         # ISO 8601 timestamp (UTC)
```

## Caching Layer

All LLM calls are cached in the `llm_cache` table (part of `state/manifest.db`). The cache is deterministic: identical requests always return the same stored response.

### Cache Key

The key is computed from:
- Model name
- System prompt
- User message
- Max tokens
- Temperature (normalized)

A request and all its parameters must be identical to hit the cache. This ensures reproducibility: re-running the pipeline with the same inputs produces the same outputs.

### Cache Validity

Cache rows never expire during a run. They persist across runs, so:
- First run of an extraction step: calls the LLM for every unique message body.
- Second run (same inputs): serves all responses from cache (zero LLM cost).

To invalidate the cache (e.g., after updating a system prompt), use `client.invalidate(prefix=...)` or clear the entire `llm_cache` table manually:

```sql
DELETE FROM llm_cache;
```

### Cache Hit Rate Monitoring

Logs track cache behavior:

```python
# High-level overview
grep "llm.cache_hit\|llm.cache_miss" pipeline.log | wc -l

# Cache hit percentage
hits=$(grep "llm.cache_hit" pipeline.log | wc -l)
misses=$(grep "llm.cache_miss" pipeline.log | wc -l)
rate=$((hits * 100 / (hits + misses)))
echo "Cache hit rate: ${rate}%"
```

## Retry and Fallback Logic

### Transient vs. Fatal Errors

**Retryable (transient):**
- `RateLimitError` — provider says "slow down"; typically recovers in seconds.
- `APIConnectionError` — TCP/DNS issue; often a brief glitch.
- `APITimeoutError` — response took too long; may succeed with backoff.

**Fatal (no retry):**
- `AuthenticationError` — wrong or missing API key; retrying is pointless.
- `BadRequestError` — malformed request; needs code fix.
- `PermissionDeniedError` — account-level deny; needs operator action.

### Retry Strategy

For each retryable error:
1. Sleep with exponential backoff (1s, 2s, 4s, ..., capped at 30s) plus 10% random jitter.
2. Retry the same primary model up to `PIPELINE_RETRY_BUDGET` times (default: 3).
3. If all retries are exhausted, attempt one final call on the fallback model (no retries on fallback).
4. If fallback also fails, raise `LLMCallError`.

The jitter prevents a "thundering herd" of clients all retrying at the exact same moment.

**Example:**
- Attempt 1: Primary model succeeds (0 retries).
- Attempt 1 (retry 1): Primary fails with RateLimitError. Sleep ~1.1s, retry primary.
- Attempt 2 (retry 2): Primary fails with RateLimitError. Sleep ~2.2s, retry primary.
- Attempt 3 (retry 3): Primary fails with RateLimitError. Sleep ~4.4s, retry primary.
- Attempt 4: All primary retries exhausted. Try fallback model once.
- Fallback succeeds: Return response (model name indicates fallback was used).

## Used By

The LLM client is invoked by:

- **`pipeline.silver.llm_extract`** — Entity extraction (vehicle, competitor, valuation, claim history).
- **`pipeline.agent.*`** — Self-healing agent loop (diagnoser stage 2, escalator).
- **Unit/integration tests** — Injected fake clients for fast, deterministic testing.

## Error Handling

All provider-level errors (authentication, rate-limit, network, malformed response) are translated to `LLMCallError`, which callers catch and handle gracefully:

```python
from pipeline.llm import LLMClient
from pipeline.errors import LLMCallError

try:
    response = client.cached_call(...)
except LLMCallError as e:
    # Log, fall back to default values, or escalate
    logger.error(f"LLM call failed: {e}")
    extracted_value = None
```

## Cross-References

- **[docs/f2-llm-handoff.md](../f2-llm-handoff.md)** — F2 LLM lane handoff and contracts.
- **[docs/api/manifest.md](manifest.md)** — `llm_cache` table schema (part of state/manifest.db).
- **[docs/usage/configuration.md](../usage/configuration.md)** — Environment variable setup.
- **[docs/adr/ADR-003.md](../adr/ADR-003.md)** — Unified SQLite state store design decision.
- **[pipeline.errors](errors.md)** — Exception hierarchy.
