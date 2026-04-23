# F2 LLM extraction lane — handoff brief

**Paste this into a new Claude Code session to continue.** Everything
below is self-contained: it names the files, the functions, the PRD
citations, and the exact contracts the new code must honor.

---

## Where we left off

- Branch: `feat/F2-scaffold` (pushed to origin).
- Tests: 332 pass, 96% coverage, ruff clean.
- Silver layer ships every §6.2 bullet EXCEPT the LLM extraction lane.
- Regex extraction + dedup + PII masking + audio confidence +
  quarantine are all wired through `silver/transform.py` and exposed
  via `python -m pipeline silver --batch-id <id>`.

### Silver §6.2 completion

| Bullet                                 | Status | Module                          |
| -------------------------------------- | ------ | ------------------------------- |
| Dedup sent/delivered                   | done   | `silver/dedup.py`               |
| Normalize sender_name + reconcile      | done   | `silver/normalize.py`, `reconcile.py` |
| Regex extraction (email/CPF/CEP/phone/plate) | done | `silver/extract.py`       |
| **LLM extraction**                     | TODO   | `silver/llm_extract.py` (new)   |
| PII masking                            | done   | `silver/pii.py`                 |
| Parse metadata JSON                    | done   | `silver/normalize.py`           |
| Audio confidence flag                  | done   | `silver/audio.py`               |
| has_content flag                       | done   | `silver/transform.py`           |
| lead_id                                | done   | `silver/lead.py`                |

PRD drivers: §6.2, **RF-03**, §18.1 (prompt), §18.4 (cache + budget).

---

## What to build

### New module: `src/pipeline/silver/llm_extract.py`

Contract:

```python
from dataclasses import dataclass
from typing import Final

import polars as pl

from pipeline.llm.client import LLMClient

PROMPT_VERSION: Final[str] = "v1"

@dataclass(frozen=True, slots=True)
class ExtractedEntities:
    veiculo_marca: str | None
    veiculo_modelo: str | None
    veiculo_ano: int | None
    concorrente_mencionado: str | None
    valor_pago_atual_brl: float | None
    sinistro_historico: bool | None

def extract_entities_from_body(
    body: str,
    *,
    client: LLMClient,
) -> ExtractedEntities:
    """One LLM call with JSON-mode. Returns parsed dataclass.

    - System prompt: pin JSON schema (PRD §18.1).
    - User prompt: body verbatim.
    - Cache hit = zero cost (LLMClient.cached_call handles it).
    - Invalid JSON or missing fields -> return all-null dataclass
      (do NOT raise, do NOT retry beyond client retry budget).
    """

def apply_llm_extraction(
    df: pl.DataFrame,
    *,
    client: LLMClient,
    max_calls: int | None = None,
) -> pl.DataFrame:
    """Materialize a DataFrame by adding six new columns.

    Input: a Silver DataFrame that already has message_body and
    message_body_masked. Runs llm_extract per unique message_body
    (dedup by body hash so the same text is never called twice in a
    run). Respects max_calls (from settings) - stops early and logs
    'llm.budget_exhausted' if the budget runs out; rows past the
    budget get all-null entity columns.
    """
```

### Silver schema additions

In `src/pipeline/schemas/silver.py`, after `audio_confidence`:

```python
# --- LLM-extracted entities (F2, §6.2) ----------------------------
"veiculo_marca": pl.String(),
"veiculo_modelo": pl.String(),
"veiculo_ano": pl.Int32(),
"concorrente_mencionado": pl.String(),
"valor_pago_atual_brl": pl.Float64(),
"sinistro_historico": pl.Boolean(),
```

Update `assert_silver_schema` is automatic — it diffs the whole
schema, so adding columns is a schema-only change.

### Transform wiring

`silver/transform.py:silver_transform` currently composes every
per-row expression into one `select`. The LLM path cannot be one
Polars expression (it needs Python-level batching for budget
enforcement and cache lookups). Two options:

**Option A** (preferred): `silver_transform` stays lazy, returns
LazyFrame with six placeholder null columns. A separate
`apply_llm_extraction(df, client, budget)` runs on the collected
DataFrame in the CLI layer, filling the columns in place. Keeps the
pure-Polars transform testable without an LLM fake.

**Option B**: take an optional `client` parameter on
`silver_transform`. Rejected — it couples the pure transform to an
I/O-bound collaborator.

**Decision: A.** Add null literal columns in `silver_transform` so
the schema matches immediately; CLI fills them by calling
`apply_llm_extraction` after `.collect()`.

### CLI integration

In `src/pipeline/cli/silver.py:_run_silver`, after
`assert_silver_schema(df)` but before `write_silver`:

```python
from pipeline.llm.cache import LLMCache
from pipeline.llm.client import LLMClient
from pipeline.silver.llm_extract import apply_llm_extraction

with LLMCache(settings.state_db_path()) as cache:
    client = LLMClient(settings, cache)
    df = apply_llm_extraction(
        df,
        client=client,
        max_calls=settings.pipeline_llm_max_calls_per_batch,
    )
assert_silver_schema(df)  # re-assert after enrichment
```

Log `llm.extraction.start`, `llm.extraction.complete` with
`calls_made`, `cache_hits`, `input_tokens_total`, `output_tokens_total`.

### Settings addition

`src/pipeline/settings.py`:

```python
pipeline_llm_max_calls_per_batch: int = Field(
    default=5000,
    ge=1,
    description="PRD §18.4 hard cap; agent pauses when hit.",
)
```

`.env.example`:

```
PIPELINE_LLM_MAX_CALLS_PER_BATCH=5000
```

### Error handling contract

- Invalid JSON from the LLM -> `ExtractedEntities(*[None]*6)`, log
  `llm.extraction.invalid_json` with first 200 chars of body.
- `LLMCallError` after retries exhausted -> same null dataclass, log
  `llm.extraction.failed`. Silver run continues; the row gets null
  entities rather than crashing the batch (RF-08 quarantine logic
  does not fire here — LLM unavailability is transient, not a row
  contract violation).
- Budget exhausted -> remaining rows get null entities, log
  `llm.budget_exhausted` once with `rows_skipped` count.

---

## Tests to write

### `tests/unit/test_silver_llm_extract.py`

Use an in-memory fake `LLMClient` (inject via `anthropic_client`
parameter already exposed in `LLMClient.__init__`). Reference existing
tests in `tests/unit/test_llm_client.py` for the fake-client pattern.

Cases:

1. Happy path — LLM returns well-formed JSON -> all fields populated.
2. LLM returns `null` for each field -> `ExtractedEntities(*[None]*6)`.
3. LLM returns malformed JSON -> null dataclass, no raise.
4. Cache hit path — same body twice -> one LLM call, two rows.
5. Budget enforcement — `max_calls=2` with 5 unique bodies -> rows
   3-5 have all-null entity columns, one `llm.budget_exhausted` log.
6. Non-ASCII body (acentos) round-trips cleanly.

### `tests/integration/test_cli_silver_e2e.py`

Add one case that monkeypatches `LLMClient` with a fake that returns
fixed JSON, invokes silver CLI end-to-end, and asserts that:

- All six columns are present in the output parquet.
- `audio_confidence` and `has_content` still work (no regressions).
- `cache_hit` works on a re-run (two invocations -> the second is
  idempotent via `is_run_completed`, but if we force re-run by
  clearing the run, cached extraction makes it instant).

---

## Commit discipline

Mirror the rhythm used for the earlier F2 slices. Split by layer, not
by file. Suggested order:

1. `feat(F2): settings cap on LLM calls per batch` — settings +
   .env.example + settings test.
2. `feat(F2): llm entity extractor with cache and budget` —
   `silver/llm_extract.py` + unit tests.
3. `feat(F2): wire llm extraction into silver schema and cli` —
   schema columns + transform null placeholders + CLI integration +
   transform tests + integration test.
4. `docs(F2): llm extraction lane in silver-flow` — add a step 5j.

Run `uv run pytest -q && uv run ruff check src/ tests/` before each
commit.

---

## Existing collaborators (do not re-invent)

| Piece                 | Location                                | Contract                                                |
| --------------------- | --------------------------------------- | ------------------------------------------------------- |
| LLM client            | `pipeline/llm/client.py:LLMClient`      | `cached_call(*, system, user, model=None, max_tokens=1024, temperature=0.0) -> LLMResponse` |
| LLM response          | same file, `LLMResponse`                | fields: text, model, input_tokens, output_tokens, cache_hit, retry_count |
| LLM cache             | `pipeline/llm/cache.py:LLMCache`        | Opened via `with LLMCache(path) as cache:`; same SQLite file as the manifest (ADR-003) |
| Error hierarchy       | `pipeline/errors.py`                    | Raise `LLMCallError` only from the client; extractor catches and returns nulls |
| Settings              | `pipeline/settings.py:Settings`         | `Settings.load()` at entrypoint; `get_secret_value()` for DashScope key |
| Logging               | `pipeline/logging.py:get_logger`        | Use `get_logger("pipeline.silver.llm")` for this module |

PRD §18.1 prompt is the starting point. Pin `PROMPT_VERSION` so a
prompt change invalidates the cache rows keyed by it
(`LLMCache.invalidate(prefix=...)` supports this).

PRD §18.4 budget: `MAX_LLM_CALLS_PER_BATCH=5000`,
`MAX_USD_PER_BATCH=2.00`. Only the call counter is enforced in this
slice; $ budget is a follow-up once we expose cost estimates.

---

## Golden-path smoke test

After shipping:

```bash
uv run python -m pipeline ingest --source data/raw/conversations_bronze.parquet
uv run python -m pipeline silver --batch-id <id>
# Inspect:
uv run python -c "
import polars as pl
df = pl.scan_parquet('data/silver/batch_id=<id>/part-0.parquet').collect()
print(df.select('veiculo_marca', 'concorrente_mencionado', 'valor_pago_atual_brl').drop_nulls().head(20))
"
```

If any of the six new columns are 100% null on the full fixture, the
extractor or the prompt is wrong — re-check PRD §18.1 and the
fallback-to-null error path.

---

## Commit anchors for orientation

| Commit   | What                                    |
| -------- | --------------------------------------- |
| f3ac6da  | docs(F2): audio_confidence in silver-flow |
| 3b95775  | wire audio_confidence into schema + transform |
| 5dbdfa5  | audio_confidence_expr (pure polars)     |
| 17e9e3b  | docs(F2): quarantine in silver-flow     |
| f494d7a  | CLI quarantine wiring                   |
| af2e3db  | silver quarantine validator + writer    |
| 1180426  | rows_rejected column on runs manifest   |

Start from the tip of `feat/F2-scaffold`.

---

## When LLM lane is done

- `git log --oneline main..HEAD` should show ~20 atomic commits for F2.
- Open PR `feat/F2-scaffold -> main` with summary of every §6.2 bullet.
- Close the session with `/oh-my-claudecode:cancel` if autopilot is
  still flagged active.
