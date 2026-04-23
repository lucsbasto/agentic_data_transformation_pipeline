---
name: llm-client-anthropic-compat
description: LLM client patterns for this repo. Uses Anthropic Python SDK pointed at DashScope's Anthropic-compatible endpoint. Covers base_url override, model routing, structured output, caching, retries, and cost control. Use when writing or reviewing any code that calls an LLM. Trigger keywords: LLM, Anthropic, DashScope, qwen, claude, messages.create, prompt, completion, llm_client, cached_call.
---

# LLM client (Anthropic SDK + DashScope)

Binding rules for every LLM call in this repo (per PRD ADR-002, revised D-002).

## 1. Single client, single entry point

- All LLM calls go through `pipeline.llm.LLMClient`. No direct `anthropic.Anthropic()` elsewhere.
- The client is a thin wrapper that adds: model routing, cache, retry, cost logging, structured output parsing.
- Constructed once per run (or once per process), injected via DI. Never re-instantiated per call.

## 2. Configuration

Read exclusively from env:

| Var | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | DashScope key (SDK default env var, reused) |
| `ANTHROPIC_BASE_URL` | `https://coding-intl.dashscope.aliyuncs.com/apps/anthropic` |
| `LLM_MODEL_PRIMARY` | `qwen3-max` |
| `LLM_MODEL_FALLBACK` | `qwen3-coder-plus` |

```python
from anthropic import Anthropic
import os

client = Anthropic(
    api_key=os.environ["ANTHROPIC_API_KEY"],
    base_url=os.environ["ANTHROPIC_BASE_URL"],
)
```

SDK honors `base_url` override natively — no custom transport needed.

## 3. Model routing

- Default to `LLM_MODEL_PRIMARY`.
- Switch to fallback on: `RateLimitError`, `APITimeoutError`, or explicit caller hint (`tier="fallback"`).
- Escalation is deterministic, not LLM-driven.

## 4. Structured output

- Always request JSON when the downstream consumer is code.
- Validate with Pydantic v2 model before returning. Reject + retry on parse failure (count against retry budget).
- Include the JSON schema in the system prompt so the model sees the contract.

```python
class PersonaClassification(BaseModel):
    label: Literal["lead", "customer", "unknown"]
    confidence: float = Field(ge=0, le=1)
    reasons: list[str]

result = client.classify(
    prompt=...,
    schema=PersonaClassification,
)  # returns validated PersonaClassification, not raw str
```

## 5. Cache (required)

- Cache key = `sha256(model + system + user + schema_name + temperature)`.
- Backend = SQLite table (same `state/manifest.db` per ADR-003) or local `diskcache`.
- Cache hit = skip network. Log as `llm.cache_hit` with key hash.
- Cache miss = call + store result. Log `llm.cache_miss` with token counts.
- Invalidate manually: `LLMClient.invalidate(prefix=...)`. No auto-TTL (determinism for pipeline replays).

## 6. Retries

- Respect PRD `PIPELINE_RETRY_BUDGET` (default 3) per *logical* call (not per attempt).
- Backoff: exponential with jitter, base 1s, cap 30s.
- Retry only on: `RateLimitError`, `APIConnectionError`, `APITimeoutError`, validation failure.
- Do NOT retry on: `AuthenticationError`, `BadRequestError` (caller bug), `PermissionDeniedError`.

## 7. Cost + token logging

Every call logs:
- `model`, `input_tokens`, `output_tokens`, `cache_hit`, `attempt`, `latency_ms`, `cost_usd_estimate`.
- Aggregate per run into the manifest row. Surface in CLI `watch` output.

## 8. Prompting

- System prompt + user prompt separated. System prompt is static per task-type, loaded from `pipeline/prompts/<name>.md`.
- No f-string interpolation of user data into system prompt. Use the `user` role for data.
- Keep `temperature=0` for classification and extraction. Raise only for genuinely creative tasks (none in this pipeline).
- `max_tokens` set explicitly per task. Never leave at SDK default.

## 9. Prompt versioning

- Every prompt file has a header `version: YYYY-MM-DD-N`. Cache key includes version.
- Changing prompt = bumping version = cache invalidation for that prompt.

## 10. Testing LLM code

- Unit tests mock `LLMClient` at the method level (`classify`, `extract`, `summarize`).
- Integration tests use recorded fixtures (VCR-style) under `tests/fixtures/llm/`. Never call real API in CI.
- Golden tests: prompt + fixture response + expected parsed output, diffed on every change.

## 11. Anti-patterns (do not)

- No raw `client.messages.create(...)` scattered across modules.
- No prompt built by `"\n".join(fragments)` — use templates.
- No silent fallback to primary-without-logging.
- No uncached calls in tight loops.
- No storing full prompts in logs at INFO level (token bloat). Log hash + first 120 chars.
