# F1 — Design: Bronze ingest + LLMClient base

Scope: the foundation milestone (M1). Produce a runnable ingest step and a reusable LLM client. No analytics, no agent loop yet.

## 1. Source dataset (measured, not assumed)

Measured from `data/raw/conversations_bronze.parquet` on 2026-04-22:

- Rows: **153,228** messages.
- Conversations: **15,000** (avg ~10.2 messages per conversation).
- Columns: **14**, all typed as `pl.String` in the source parquet (including `timestamp` and the JSON `metadata` blob).
- Nulls: **zero** across every column.
- Cardinalities that matter:
  - `direction`: 2 (`outbound` 79,426 / `inbound` 73,802).
  - `status`: 3 (`delivered`, `read`, `sent`).
  - `channel`: 1 (`whatsapp`) — effectively constant.
  - `message_type`: 8 (`text` dominates at 146,479).
  - `conversation_outcome`: 7 Portuguese labels (`venda_fechada`, `ghosting`, `desistencia_lead`, `perdido_concorrente`, `em_negociacao`, `proposta_enviada`, `perdido_preco`).
  - `campaign_id`: 10, all tagged `*_fev2026`.
  - `agent_id`: 20.
  - `sender_name`: 699 (~47 conversations per unique name on average; name is not a unique person).
- `timestamp` format: `YYYY-MM-DD HH:MM:SS` string, no timezone suffix.
- `metadata` format: JSON string with at least `device` and `city` keys.

### Consequences for design

- All types are string in source → Bronze is responsible for casting.
- The `conversation_outcome` appears on every message row, even though it is logically a conversation-level attribute. Bronze preserves it as-is (1:1 source fidelity). Silver dedupes to a conversations table.
- Portuguese labels everywhere → prompts must operate in Portuguese. `qwen3-max` handles PT; no translation layer needed.
- `channel` is constant → keep in Bronze (fidelity), drop as uninteresting in Gold.
- `metadata` is an embedded JSON document → parse to struct in Silver, not Bronze.

## 2. Python project layout

```
pipeline-de-transformacao-agentica-de-dados/
├── pyproject.toml                # uv-managed, Python 3.12
├── uv.lock
├── src/
│   └── pipeline/
│       ├── __init__.py
│       ├── __main__.py            # `python -m pipeline <subcommand>`
│       ├── settings.py            # pydantic-settings; loads env
│       ├── paths.py               # project_root + layer paths
│       ├── logging.py             # structlog config
│       ├── errors.py              # PipelineError hierarchy
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── bronze.py          # BRONZE_SCHEMA (pl.Schema)
│       │   └── manifest.py        # SQLAlchemy / sqlite3 table DDL
│       ├── ingest/
│       │   ├── __init__.py
│       │   ├── reader.py          # scan + validate raw parquet
│       │   ├── transform.py       # string→typed casts
│       │   └── writer.py          # partitioned write by batch_id
│       ├── state/
│       │   ├── __init__.py
│       │   └── manifest.py        # sqlite manager: batches, runs
│       ├── llm/
│       │   ├── __init__.py
│       │   ├── client.py          # LLMClient (Anthropic SDK)
│       │   ├── cache.py           # sqlite-backed cache
│       │   ├── models.py          # request/response pydantic models
│       │   └── prompts/           # versioned prompt templates
│       └── cli/
│           ├── __init__.py
│           └── ingest.py          # `ingest` subcommand
└── tests/
    ├── conftest.py
    ├── fixtures/
    │   └── tiny_conversations.parquet  # 50-row synthetic sample
    ├── unit/
    │   ├── test_ingest_transform.py
    │   ├── test_ingest_writer.py
    │   ├── test_llm_cache.py
    │   └── test_manifest.py
    └── integration/
        └── test_ingest_end_to_end.py
```

Rationale: **src layout** prevents accidental imports from the working directory and forces packaging discipline. Every module that touches Polars imports it at module top (per `polars-lazy-pipeline` skill).

## 3. Bronze schema (typed)

Declared once in `pipeline/schemas/bronze.py`:

| Column | Source dtype | Bronze dtype | Notes |
|---|---|---|---|
| `message_id` | str | `pl.String` | Primary key; 100% unique (153,228). |
| `conversation_id` | str | `pl.String` | FK to future Silver conversations. |
| `timestamp` | str | `pl.Datetime("us", None)` | Parsed `%Y-%m-%d %H:%M:%S`; no tz info in source → keep naive. |
| `direction` | str | `pl.Enum(["inbound","outbound"])` | Closed set; enum catches future drift. |
| `sender_phone` | str | `pl.String` | E.164-like; kept raw in Bronze. |
| `sender_name` | str | `pl.String` | |
| `message_type` | str | `pl.Enum([...8 values])` | Enum from measured set. |
| `message_body` | str | `pl.String` | Free text. |
| `status` | str | `pl.Enum(["delivered","read","sent"])` | |
| `channel` | str | `pl.String` | Kept as String (not Enum) to tolerate future channels. |
| `campaign_id` | str | `pl.String` | |
| `agent_id` | str | `pl.String` | |
| `conversation_outcome` | str | `pl.String` | Kept raw; dedup to Silver. |
| `metadata` | str (JSON) | `pl.String` | Parsed in Silver, not Bronze. |
| `batch_id` | — | `pl.String` | Added by Bronze writer. |
| `ingested_at` | — | `pl.Datetime("us", "UTC")` | Added by Bronze writer. |
| `source_file_hash` | — | `pl.String` | SHA-256 of source parquet file. |

Enums are frozen in code. Any new value triggers a schema-drift error at write-time (see §5).

## 4. Ingest flow

`python -m pipeline ingest --source data/raw/conversations_bronze.parquet`

Steps:
1. **Resolve settings** (`Settings.from_env()`) — fail fast on missing env.
2. **Open manifest** (`ManifestDB(path=settings.state_db)`), ensure schema.
3. **Compute `batch_id`**: `sha256(source_file_hash + source_mtime)[:12]`. Deterministic; re-running on same file + mtime yields same batch.
4. **Check for existing batch** — if `batch_id` already present with status `COMPLETED`, exit 0 (idempotent).
5. **Insert batch row** (`status=IN_PROGRESS`).
6. **Scan** source with `pl.scan_parquet`.
7. **Transform** — cast strings to typed schema (see §3). Errors surface as `SchemaDriftError`.
8. **Write** Bronze to `data/bronze/batch_id=<id>/part-0.parquet` (`zstd` + stats).
9. **Validate write** — reopen via `scan_parquet`, assert row count matches source.
10. **Update batch row** (`status=COMPLETED`, `rows_written`, `duration_ms`, `bronze_path`).
11. **Log** `ingest.complete` with the manifest row payload.

Any exception between steps 5 and 9 → set `status=FAILED`, record `error_type`, `error_message`, re-raise. No partial write remains: Bronze writes to a temp dir first, then atomic-renames. Temp dir is cleaned on failure.

## 5. Manifest DB schema

SQLite at `state/manifest.db`. Table DDL in `pipeline/state/manifest.py`:

```sql
CREATE TABLE IF NOT EXISTS batches (
    batch_id            TEXT PRIMARY KEY,
    source_path         TEXT NOT NULL,
    source_hash         TEXT NOT NULL,
    source_mtime        INTEGER NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED','FAILED')),
    rows_read           INTEGER,
    rows_written        INTEGER,
    bronze_path         TEXT,
    started_at          TEXT NOT NULL,   -- ISO 8601 UTC
    finished_at         TEXT,
    duration_ms         INTEGER,
    error_type          TEXT,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);
CREATE INDEX IF NOT EXISTS idx_batches_started ON batches(started_at);

-- Reserved for F4 agent loop; declared now to avoid schema churn later.
CREATE TABLE IF NOT EXISTS runs (
    run_id              TEXT PRIMARY KEY,
    batch_id            TEXT NOT NULL REFERENCES batches(batch_id),
    layer               TEXT NOT NULL CHECK (layer IN ('bronze','silver','gold')),
    status              TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    rows_in             INTEGER,
    rows_out            INTEGER,
    error_type          TEXT,
    error_message       TEXT
);
```

Access via a thin `ManifestDB` class. Use `sqlite3` stdlib, not an ORM — scope is tiny and we avoid a dependency.

## 6. LLMClient (stub for F1; full use arrives in F2/F3/F4)

`pipeline/llm/client.py` exposes a single class with the minimum surface required for M1 acceptance:

```python
class LLMClient:
    def __init__(self, settings: Settings, cache: LLMCache, anthropic_client: Anthropic | None = None) -> None: ...
    def cached_call(
        self,
        *,
        system: str,
        user: str,
        model: str | None = None,              # defaults to settings.llm_primary
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse: ...
    def invalidate(self, *, prefix: str | None = None) -> int: ...
```

Internals:
- Builds an `Anthropic(api_key=..., base_url=settings.anthropic_base_url)` once; reuses it.
- Cache key: `sha256("\n".join([model, system, user, str(max_tokens), str(temperature)]))`.
- Backend: **same** `state/manifest.db`, extra table `llm_cache`.

```sql
CREATE TABLE IF NOT EXISTS llm_cache (
    cache_key       TEXT PRIMARY KEY,
    model           TEXT NOT NULL,
    response_text   TEXT NOT NULL,
    input_tokens    INTEGER NOT NULL,
    output_tokens   INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);
```

- Retries: exponential backoff, max `PIPELINE_RETRY_BUDGET` (default 3). Retries only on `RateLimitError`, `APIConnectionError`, `APITimeoutError`.
- Structured output, Pydantic validation, model fallback routing: **deferred to F2**. F1 only needs the raw `cached_call` path to exist and be exercised by a test.
- Every call emits one of: `llm.cache_hit`, `llm.cache_miss`, `llm.retry`, `llm.failed` structlog events, with a hash of the cache key (never the full prompt at INFO level).

## 7. Settings

`pipeline/settings.py` uses `pydantic-settings`:

```python
class Settings(BaseSettings):
    anthropic_api_key: SecretStr
    anthropic_base_url: AnyHttpUrl = "https://coding-intl.dashscope.aliyuncs.com/apps/anthropic"
    llm_model_primary: str = "qwen3-max"
    llm_model_fallback: str = "qwen3-coder-plus"
    pipeline_retry_budget: int = 3
    pipeline_loop_sleep_seconds: int = 60
    pipeline_state_db: Path = Path("state/manifest.db")
    pipeline_log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
```

Load once in the CLI entrypoint; pass explicitly (DI) — no globals.

## 8. Dependencies (`pyproject.toml`)

Runtime:
- `polars >= 1.0` (lazy API is stable there).
- `anthropic` (official SDK).
- `pydantic >= 2.7`, `pydantic-settings`.
- `structlog`.
- `click` for the CLI (lighter than typer, plays well with `python -m`).

Dev:
- `pytest`, `pytest-cov`, `pytest-mock`.
- `ruff`, `mypy` (strict).
- `freezegun` for time-sensitive tests.
- `respx` or `anthropic`’s built-in mock transport for LLM tests.

Python: `>=3.12,<3.13`.

## 9. Smoke test plan (F1 must pass)

Unit:
- `test_manifest.py` — insert batch, update status, fetch by id, detect re-runs.
- `test_ingest_transform.py` — build a 5-row `LazyFrame` from in-memory data; run the transform; assert schema equals `BRONZE_SCHEMA`; assert enum rejection on an unknown `direction`.
- `test_ingest_writer.py` — write to `tmp_path`; re-scan; assert row count and partition path.
- `test_llm_cache.py` — insert + hit + miss paths on an in-memory sqlite DB.

Integration:
- `test_ingest_end_to_end.py` uses `tests/fixtures/tiny_conversations.parquet` (50 rows hand-crafted to exercise every enum value). Asserts:
  - `batches` row created with `COMPLETED`.
  - Bronze file exists at expected partition path.
  - Row count matches source.
  - Re-running is a no-op (manifest hit).

No network in tests. The LLM client tests mock the `Anthropic` class.

## 10. Acceptance criteria (copy from ROADMAP M1)

- `python -m pipeline ingest` reads `data/raw/conversations_bronze.parquet`, writes Bronze partitioned by `batch_id`, records the batch in `state/manifest.db`.
- `LLMClient.cached_call()` works with cache hit + miss paths.
- Test suite is green under `uv run pytest`.
- `ruff check .` and `mypy src` return zero issues.

## 11. Open questions (to resolve before implementation)

- **Q1.** Do we want `message_type` and `direction` as `pl.Enum` (strict, catches drift) or `pl.Categorical` (friendlier in joins)? Recommendation: **Enum** — drift detection is the whole point of Bronze.
- **Q2.** Commit strategy for the CLI: land ingest before LLMClient, or both in one go? Recommendation: **two atomic commits** — (a) manifest + ingest pipeline with a stub LLMClient that raises `NotImplementedError`, (b) the real LLMClient + cache. Keeps diff reviewable.
- **Q3.** Should we ship a minimum `README.md` in this milestone? The PRD doesn’t gate on it until M4, but a one-page “how to run” written now saves re-work later. Recommendation: **yes, short one** — sections: install (uv), env (.env), commands, test.
- **Q4.** Confirm `PIPELINE_LOG_LEVEL` defaults: INFO in dev, WARNING in CI? Recommendation: **INFO default, env-overridable**; CI sets `PIPELINE_LOG_LEVEL=WARNING`.
- **Q5.** `message_id` is 100% unique in the sample — do we trust that invariant and enforce uniqueness in Bronze? Recommendation: **assert in tests, don’t enforce at write** (Polars has no unique constraint; enforcement costs a sort). Surface duplicates as a warning with count.

## 12. Implementation order (tasks for the next phase)

1. Bootstrap `pyproject.toml` + `uv.lock` + empty `src/pipeline/` package skeleton.
2. `settings.py` + `.env` plumbing + tests.
3. `logging.py` (structlog JSON config) + tests.
4. `schemas/bronze.py` + `schemas/manifest.py`.
5. `state/manifest.py` + tests.
6. `ingest/` reader/transform/writer + tests.
7. `cli/ingest.py` + `__main__.py`.
8. Integration test on tiny fixture.
9. `llm/cache.py` + tests.
10. `llm/client.py` (skeleton: `cached_call` only) + tests.
11. README (short) + `ruff` + `mypy` config.
12. Smoke run on real parquet; capture timing in STATE.md.

Each step becomes one atomic commit via the `/commit` skill.
