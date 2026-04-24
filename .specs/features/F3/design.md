# F3 — Design: Gold analytics + personas

Translates `spec.md` into concrete modules, data flow, concurrency
strategy, and testing approach. Nothing here adds scope; it pins how
each F3-RF- requirement will be met.

## 1. Module layout

```
src/pipeline/
├── schemas/gold.py              # 4 pl.Schema + 4 assert_*_schema validators
├── gold/
│   ├── __init__.py
│   ├── transform.py             # orchestrator; composes the 4 builders
│   ├── conversation_scores.py   # Silver → gold.conversation_scores (pure Polars)
│   ├── lead_profile.py          # Silver → gold.lead_profile skeleton (pre-persona)
│   ├── agent_performance.py
│   ├── competitor_intel.py
│   ├── persona.py               # LLM classifier + PRD §18.2 hard rules
│   ├── intent_score.py          # PRD §17.1 formula (pure Polars on lead_profile inputs)
│   ├── insights.py              # insights JSON builder
│   ├── concurrency.py           # async semaphore wrapper over LLMClient.cached_call
│   └── writer.py                # atomic parquet + JSON writes
└── cli/gold.py                  # python -m pipeline gold --batch-id
```

Tests mirror the source layout: one `tests/unit/test_gold_<mod>.py`
per module, plus `tests/integration/test_cli_gold_e2e.py`.

## 2. Data flow

```
data/silver/batch_id=<id>/part-0.parquet
           │
           ▼
pl.scan_parquet (lazy)
           │
           ├── conversation_scores_lf ──► assert ──► write parquet
           │
           ├── agent_performance_lf ────► assert ──► write parquet
           │
           ├── competitor_intel_lf ─────► assert ──► write parquet
           │
           └── lead_profile_skeleton_lf ► collect
                                            │
                                            ├── persona.classify_all (async, semaphore)
                                            ├── intent_score.compute (pure)
                                            │
                                            ▼
                                         assert ──► write parquet
                                            │
                                            ▼
                                  insights.build (reads the 4 collected frames)
                                            │
                                            ▼
                                         write JSON
```

The three pure-Polars tables stay lazy end-to-end. `lead_profile`
must collect mid-flow because persona classification is Python-level:
the LLM needs an aggregated conversation per lead and the guard-rail
overrides are easier to reason about as a Python loop than as a
`pl.when/then` tree. After persona + intent_score fill, the frame is
re-asserted against `GOLD_LEAD_PROFILE_SCHEMA`. Same pattern Silver
uses for the LLM extraction lane.

## 3. Schemas

Four `pl.Schema` constants in `schemas/gold.py`, matching spec
F3-RF-05..08 column-for-column. Each ships with an
`assert_<table>_schema(df)` helper that raises `SchemaDriftError` with
the same column-diff message format Silver uses.

`persona` is a `pl.Enum` over the eight PRD §18.2 labels; enum values
live next to the schema so future callers read one source of truth.
`outcome_mix` is `pl.List(pl.Struct({"outcome": pl.String, "count":
pl.Int32}))` — drift-safe per D8.

## 4. `conversation_scores` (deterministic)

### 4.1. Aggregations by `conversation_id`

| Column | Formula |
|---|---|
| `msgs_inbound` / `msgs_outbound` | `count_when(direction='inbound')` / `count_when(direction='outbound')` |
| `first_message_at` / `last_message_at` | `min(timestamp)` / `max(timestamp)` |
| `duration_sec` | `(last_message_at - first_message_at).dt.total_seconds()` |
| `off_hours_msgs` | `count_when(metadata.is_business_hours IS FALSE)` |
| `mencionou_concorrente` | `any(concorrente_mencionado IS NOT NULL)` |
| `concorrente_citado` | first non-null `concorrente_mencionado` by `timestamp` ASC |
| `veiculo_marca` / `veiculo_modelo` / `veiculo_ano` | most-frequent non-null per conversation (ties broken by earliest timestamp) |
| `valor_pago_atual_brl` | `max(valor_pago_atual_brl)` within the conversation |
| `sinistro_historico` | `true if any true else false if any false else null` (Kleene-style aggregation) |
| `conversation_outcome` | last non-null across the conversation's rows |

### 4.2. `avg_lead_response_sec` (spec F3-RF-05, blocker #2 resolution)

For each conversation, sort by `timestamp` and walk the sequence
pairwise:

```python
# pseudo-polars
prev_outbound_ts = None
pairs = []
for row in conversation.sort("timestamp"):
    if row.direction == "outbound":
        prev_outbound_ts = row.timestamp
    elif row.direction == "inbound" and prev_outbound_ts is not None:
        pairs.append((row.timestamp - prev_outbound_ts).total_seconds())
        prev_outbound_ts = None   # first inbound after each outbound only
avg_lead_response_sec = mean(pairs)  # null if pairs is empty
```

Implementation uses a single `lazyframe` pass: shift `direction` +
`timestamp` within `conversation_id`, compute per-row delta where the
previous row was outbound and the current is inbound, average the
deltas. **`metadata.response_time_sec` is agent-side and is not
used.** `time_to_first_response_sec` uses the first such pair only
(null if no outbound precedes any inbound).

## 5. `lead_profile` skeleton + persona lane

### 5.1. Aggregations by `lead_id` (pre-persona)

| Column | Formula |
|---|---|
| `conversations` | `n_unique(conversation_id)` |
| `closed_count` | `count_when(conversation_outcome = 'venda_fechada')` per conversation |
| `close_rate` | `closed_count / conversations` (null-safe div: null when `conversations = 0`, but that cannot happen) |
| `sender_name_normalized` | mode of `sender_name_normalized` (stable from Silver reconcile) |
| `dominant_email_domain` | mode of `email_domain` (ignore null) |
| `dominant_state` / `dominant_city` | mode of `metadata.state` / `metadata.city` |
| `engagement_profile` | deterministic, evaluated in order — first match wins. `hot` if `close_rate ≥ 0.5` OR (`conversations ≥ 3` AND `close_rate > 0`); else `cold` if `conversations = 1` AND `closed_count = 0` AND `last_seen_at < batch_latest_timestamp - 48h`; else `warm`. Test fixtures cover the boundaries (`conversations=2, close_rate=0.5` ⇒ `hot`; `conversations=1, close_rate=1.0` ⇒ `hot`; `conversations=1, closed_count=0, fresh` ⇒ `warm`). |
| `first_seen_at` / `last_seen_at` | `min(timestamp)` / `max(timestamp)` |

### 5.2. Lead aggregate for the persona classifier

Per lead, build a `LeadAggregate` dataclass from the Silver rows:

- `lead_id`
- `num_msgs`, `num_msgs_inbound`, `num_msgs_outbound`
- `outcome` (last non-null `conversation_outcome` across the lead's
  conversations)
- `mencionou_concorrente` (any `concorrente_mencionado` present)
- `competitor_count_distinct` (distinct non-null values)
- `forneceu_dado_pessoal` = **`has_cpf OR email_domain IS NOT NULL OR
  has_phone_mention`** (deterministic regex-derived Silver columns
  only; spec F3-RF-17 / D13 — `valor_pago_atual_brl` excluded).
- `last_message_at` (for R1 staleness)
- `conversation_text` — concatenation of up to the last 20 lead-side
  `message_body_masked` strings ordered by `timestamp`, truncated to
  2000 chars.

### 5.3. Guard-rail gate (pre-LLM)

Evaluate the three PRD §18.2 rules in priority order **R2 → R1 →
R3** (spec F3-RF-09). First hit wins; skip the LLM call.

| Order | Rule | Condition | Forced persona |
|-------|------|-----------|----------------|
| 1 | R2 | `outcome == 'venda_fechada' AND num_msgs ≤ 10` | `comprador_rapido` |
| 2 | R1 | `num_msgs ≤ 4 AND outcome is null AND last_message_at < batch_latest_timestamp - 48h` | `bouncer` |
| 3 | R3 | `forneceu_dado_pessoal is False` | `cacador_de_informacao` |

**R2 runs first** so a 4-msg conversation that closed (rare but
possible) is classified as `comprador_rapido`, not `bouncer`. R1 gets
the **48h staleness** check so an in-flight conversation with few
messages isn't prematurely labelled. `batch_latest_timestamp` is
computed as `max(timestamp)` over the whole Silver frame — one pass,
fed into every `LeadAggregate`. A rule hit records
`persona_source='rule'`, `persona_confidence=1.0`.

### 5.4. LLM path

If no guard-rail fires, build the §18.2 prompt with the aggregate's
fields and call the async-semaphore-wrapped
`LLMClient.cached_call` (see §9) with `PROMPT_VERSION_PERSONA='v1'`
embedded in the system prompt. Expected reply: a single line matching
one of the eight enum values (trim + lower). Any other output ⇒
fallback to `comprador_racional` and log `persona.llm_invalid` with
`body_hash` + `response_len`. `persona_source='llm'`,
`persona_confidence=0.8`.

### 5.5. Post-LLM override

Re-check R1/R2/R3 on the returned label. If any rule would have
forced a different persona, override, emit `persona.override` with
the rule id, and set `persona_source='rule_override'`,
`persona_confidence=1.0`. Belt-and-braces.

### 5.6. Budget

Reuse `Settings.pipeline_llm_max_calls_per_batch`. Counted only on
cache misses. On exhaustion: `persona=null`, `persona_confidence=null`,
`persona_source='skipped'`, one `persona.budget_exhausted` log per
run. Intent score still computed via F3-RF-10's persona-null fallback
(`coerencia_outcome_historico_persona = 0.5`).

## 6. Intent score (`gold/intent_score.py`)

Pure Polars expression on lead-level aggregates. Exact weights from
PRD §17.1:

```
score =  25 * fornecimento_dados_pessoais
      +  20 * velocidade_resposta_normalizada
      +  15 * presenca_termo_fechamento
      +  15 * perguntas_tecnicas_normalizado
      +  10 * ausencia_palavras_evasivas
      +  10 * coerencia_outcome_historico_persona
      +   5 * janela_horaria_comercial
```

Each component ∈ `[0, 1]`; the final score is cast to `Int32` and
clamped to `[0, 100]`. Out-of-range emits
`intent_score.out_of_range`.

Component sources (deterministic, no LLM):

| Component | Source |
|-----------|--------|
| `fornecimento_dados_pessoais` | 1.0 if `forneceu_dado_pessoal` else 0.0 (same signal as R3) |
| `velocidade_resposta_normalizada` | `1 - min(avg_lead_response_sec / 600, 1)` (lead-side latency from F3-RF-05; 10-minute cap ⇒ 0) |
| `presenca_termo_fechamento` | regex hit on lead-side body: `{"fechou", "manda boleto", "tá bom"}` |
| `perguntas_tecnicas_normalizado` | `min(count of {"franquia", "cobre", "assistência", "carro reserva"} / 3, 1.0)` |
| `ausencia_palavras_evasivas` | `1 - min(count of {"vou pensar", "depois te aviso", "preciso ver"} / 3, 1.0)` |
| `coerencia_outcome_historico_persona` | lookup `PERSONA_EXPECTED_OUTCOME` in `persona.py`: 1.0 on match, 0.0 on contradiction, **0.5 when `persona is null`** (F3-RF-10), 0.5 for neutral personas like `comprador_racional` |
| `janela_horaria_comercial` | `1 - off_hours_msgs / num_msgs_inbound` (null → 0.5 when `num_msgs_inbound = 0`) |

Property test: score ∈ `[0, 100]` on every permutation of component
values ∈ `{0.0, 0.5, 1.0}^7`.

## 7. `agent_performance` / `competitor_intel`

### 7.1. `agent_performance`

Group-by `agent_id`:

- `conversations`, `closed_count`, `close_rate` — same shape as
  `lead_profile`.
- `avg_response_time_sec` — mean of `metadata.response_time_sec` for
  the agent's outbound rows (agent-side latency; this one IS allowed
  to use Bronze's `response_time_sec`).
- `outcome_mix` — **`List[Struct{outcome, count}]`**. Build via a
  `group_by(['agent_id','conversation_outcome'])` then collapse to a
  list per agent. No outcome is dropped; unknown Bronze values get
  their own entry, no warning, no data loss (D8).
- `top_persona_converted` — for each agent, find the persona with the
  highest `close_rate` among that agent's leads; ties broken by
  absolute `closed_count` (open question from spec §8 resolved here).
  Null when the agent has zero closed conversations.

### 7.2. `competitor_intel`

Group-by lower-cased + stripped `concorrente_mencionado` (D7):

- `mention_count` — count of rows with that competitor.
- `avg_quoted_price_brl` — `mean(valor_pago_atual_brl)` filtered to
  rows mentioning this competitor; null if no prices observed. After
  the F2 v1 prompt tightening, only lead-stated current premiums feed
  this — the earlier seller-quoted contamination is gone.
- `loss_rate` — `sum(conversation_outcome != 'venda_fechada') /
  mention_count` computed on distinct conversations (one lead can
  mention the same competitor many times).
- `top_states` — top 3 states by mention volume.

## 8. Insights (`gold/insights.py`)

Reads the four collected Gold frames (no re-scan) and emits a
`summary.json`:

- `ghosting_taxonomy` — ghosted leads (`outcome is null AND closed_count
  = 0`) bucketed by message count `{1-2, 3-4, 5-9, 10+}`, with count
  + rate vs all leads.
- `objections` — regex catalog on lead-side text for
  `{"tá caro", "vou pensar", "depois", "me dá desconto", "outro lugar"}`,
  each with `count`, `close_rate_when_present`, one-sentence `note`.
- `disengagement_moment` — for ghosted leads, distribution of the
  outbound message index at which the lead stopped responding,
  bucketed by the *message index* `{first, 2nd–3rd, 4th+}` **AND**
  the content regex `{after first quote, after price ask, after
  personal data ask, other}`. Two-axis breakdown (count per
  index×content) so "other" cannot drown the signal.
- `persona_outcome_correlation` — crosstab `persona × outcome` with
  count and rate; plus the top 1 "surprising" pair (largest absolute
  deviation from the overall close rate).

Determinism caveat per D9: persona labels are LLM-sourced, so
`persona_outcome_correlation` is only deterministic under fixed
Silver + warm cache + unchanged `PROMPT_VERSION_PERSONA`. The other
three insights are pure regex + count aggregates over Silver and so
are always deterministic given the same Silver.

Schema:

```json
{
  "generated_at": "<iso UTC>",
  "batch_id": "<id>",
  "determinism": {
    "silver_batch_id": "<id>",
    "prompt_version_persona": "v1",
    "llm_cache_warm": true
  },
  "ghosting_taxonomy": [...],
  "objections": [...],
  "disengagement_moment": [...],
  "persona_outcome_correlation": { "matrix": [...], "top_surprise": {...} }
}
```

## 9. LLM concurrency (`gold/concurrency.py`)

Spec blocker #1 resolution. PRD §17.1 wants 5000 persona calls well
under 15 minutes; sequentially that is ~2.7 hours.

### 9.1. Settings pre-requisite

`Settings.pipeline_llm_concurrency` is a NEW field added by the
F3.2 task (see `tasks.md`):

```python
pipeline_llm_concurrency: int = Field(
    default=16,
    ge=1,
    le=64,
    description=(
        "Max in-flight LLM calls per batch. Defaults match DashScope "
        "qwen3-coder-plus per-model quota and CPython's default "
        "thread-pool ceiling. Sequential persona classification on "
        "5000 leads would blow the M2 SLA."
    ),
)
```

Without this field the rest of the section fails to import.

### 9.2. Thread-safety contract

`LLMClient.__init__` documents *"not thread-safe: give each
concurrent consumer its own instance"*; `LLMCache` opens a SQLite
connection that, by default, refuses cross-thread access. The naïve
"share one client across an `executor`" pattern raises
`sqlite3.ProgrammingError` at concurrency > 1. The design therefore
gives **each worker thread its own `LLMClient` + `LLMCache`**, both
opened against the same on-disk SQLite file (SQLite's WAL +
`busy_timeout=5000` already in F1.3 makes multi-connection access
safe).

### 9.3. Module shape

```python
# gold/concurrency.py
from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from pipeline.llm.cache import LLMCache
from pipeline.llm.client import LLMClient
from pipeline.settings import Settings

_thread_local = threading.local()


def _init_worker(settings: Settings, db_path: Path) -> None:
    """Run once per worker thread; build that thread's own client.

    No explicit shutdown hook is required: when the worker thread
    exits at ``executor.shutdown(wait=True)`` time, the
    ``_thread_local`` storage is garbage-collected and
    ``sqlite3.Connection.__del__`` closes the per-thread connection.
    SQLite WAL + ``busy_timeout=5000`` (set in ``LLMCache.open``)
    keep concurrent writes safe in the meantime.
    """
    _thread_local.cache = LLMCache(db_path).open()
    _thread_local.client = LLMClient(settings, _thread_local.cache)


async def classify_all(
    aggregates: list[LeadAggregate],
    *,
    settings: Settings,
    db_path: Path,
    budget: _BudgetCounter,
) -> dict[str, PersonaResult]:
    sem = asyncio.Semaphore(settings.pipeline_llm_concurrency)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(
        max_workers=settings.pipeline_llm_concurrency,
        initializer=_init_worker,
        initargs=(settings, db_path),
        thread_name_prefix="gold-persona",
    )

    async def one(agg: LeadAggregate) -> tuple[str, PersonaResult]:
        async with sem:
            return await loop.run_in_executor(
                executor, _classify_one_sync, agg, budget,
            )

    try:
        results = await asyncio.gather(*[one(a) for a in aggregates])
    finally:
        # ``shutdown(wait=True)`` joins the workers; their
        # ``threading.local`` storage is then refcount-collected and
        # the SQLite connections close as part of normal teardown.
        executor.shutdown(wait=True)

    return dict(results)


def _classify_one_sync(
    agg: LeadAggregate, budget: _BudgetCounter,
) -> tuple[str, PersonaResult]:
    """Runs on a worker thread. Reads ``_thread_local.client``."""
    if not budget.try_charge_provider_call():
        return agg.lead_id, PersonaResult.skipped()
    result = _classify_with_overrides(
        agg, client=_thread_local.client
    )
    budget.refund_if_cache_hit(result)
    return agg.lead_id, result
```

Notes:

- **`max_workers=settings.pipeline_llm_concurrency`** — explicit
  executor sizing. Without this, `run_in_executor(None, …)` would
  fall back to CPython's default pool (`min(32, cpu_count + 4)`),
  capping effective concurrency around 32 even when the operator
  raises the setting.
- `_init_worker` opens one `LLMClient` + `LLMCache` per thread; the
  `_thread_local` storage avoids passing them as call arguments.
- `_shutdown_worker` is registered via `executor.shutdown(wait=True)`
  inside `classify_all`'s `finally` — the thread is torn down before
  the function returns, and SQLite connections close with it.
- Cache hits short-circuit the semaphore implicitly: `cached_call`
  returns in < 5 ms on a hit, the executor task finishes, the
  semaphore releases.

### 9.4. Budget back-pressure (`_BudgetCounter`)

A small thread-safe counter lives next to `classify_all`:

```python
class _BudgetCounter:
    def __init__(self, max_provider_calls: int) -> None:
        self._lock = threading.Lock()
        self._remaining = max_provider_calls
        self.calls_made = 0
        self.cache_hits = 0
        self.budget_exhausted_logged = False

    def try_charge_provider_call(self) -> bool:
        """Returns False once the budget is spent."""
        with self._lock:
            if self._remaining <= 0:
                return False
            self._remaining -= 1
            self.calls_made += 1
            return True

    def refund_if_cache_hit(self, result: PersonaResult) -> None:
        if result.cache_hit:
            with self._lock:
                self._remaining += 1
                self.calls_made -= 1
                self.cache_hits += 1
```

We charge optimistically (PRD §18.4 "5000 max calls" = 5000 *cache
misses*), then refund when the response turns out to have come from
cache. That removes the obvious race where two threads both decide
"budget is fine, fire" past the cap.

### 9.5. Math + budget

5000 cache-miss calls ÷ 16 concurrency × ~2 s/call ≈ **10 minutes**
worst case; with any cache warmth it drops below 5 min. Leaves the
15-minute Bronze+Silver+Gold envelope comfortable. With concurrency
raised to 32 (within the setting bound), the cold-cache run drops to
~5 min.

### 9.6. Synchronous entry point

```python
budget = _BudgetCounter(settings.pipeline_llm_max_calls_per_batch)
results = asyncio.run(
    classify_all(
        aggregates,
        settings=settings,
        db_path=settings.state_db_path(),
        budget=budget,
    )
)
```

Called from `transform.py` once per batch.

## 10. CLI (`cli/gold.py`)

Copy the Silver CLI shape (F2) and adjust:

- Option parsing: `--batch-id <id>`, `--silver-root`, `--gold-root`.
- `_validate_batch_id` identical to Silver.
- Open ManifestDB; require a COMPLETED silver run for the batch;
  raise `GoldError` otherwise.
- Idempotency short-circuit on COMPLETED `(batch_id, layer='gold')`.
- `insert_run(layer='gold')`, wrap the transform call in `try/except
  PipelineError`, `mark_run_failed` on any exception.
- `_build_gold_tables(silver_lf, settings, batch_id)` keeps
  `_run_gold` under the ruff `PLR0915` budget.
- Success path: `mark_run_completed(rows_in=<silver row count>,
  rows_out=<sum of 4 table row counts>, output_path=<gold root>)`.

Structured logs mirror Silver's names with `gold.` prefix
(F3-RF-15).

## 11. Error handling contract

- Missing Silver run / not COMPLETED ⇒ `GoldError`, exit 1, no
  partial writes.
- Persona LLM call fails ⇒ `persona=null` on that lead, batch keeps
  going.
- Persona LLM budget exhausted ⇒ remaining leads null, one log line.
- Insights JSON build error ⇒ `GoldError` (deterministic; structural
  failure).
- Any schema drift ⇒ `SchemaDriftError` on the first offending table,
  no downstream tables are written.

## 12. Testing strategy

| Module | What the tests lock |
|--------|---------------------|
| `schemas/gold.py` | Column order, dtypes, persona enum values, `outcome_mix` list-of-struct shape, `assert_*` drift messages. |
| `gold/conversation_scores` | Grain (1 row per conversation), every aggregation, **`avg_lead_response_sec` pairwise formula** (table-driven fixture with crafted outbound/inbound sequences), reconciliation of vehicle fields. |
| `gold/lead_profile` | Grain, close_rate arithmetic, dominant email/state/city logic, `engagement_profile` bucket thresholds. |
| `gold/persona` | Each of R1/R2/R3 with positive + negative (R1 with and without 48h staleness; R2 winning over R1 on 4-msg closed conversation; R3 positive = no personal data, negative = CPF present); LLM path valid labels; LLM path invalid reply → fallback; budget exhaustion → null; post-LLM override. |
| `gold/intent_score` | Property test over random inputs ⇒ score always `Int32 ∈ [0,100]`; each component isolated; **persona-null branch uses 0.5 for coerência**. |
| `gold/agent_performance` | `outcome_mix` preserves unknown outcomes; `top_persona_converted` tie-break on close_rate then absolute closed_count. |
| `gold/competitor_intel` | Normalization of competitor strings; `mention_count`, `avg_quoted_price_brl` with/without prices, `loss_rate` on distinct conversations. |
| `gold/insights` | Shape stable; empty inputs ⇒ empty arrays + explicit `count=0`; determinism block present in JSON. |
| `gold/concurrency` | Semaphore limit respected (mock sleep inside classify, assert no more than N concurrent); cache hits bypass semaphore; budget back-pressure stops provider calls past cap. |
| `cli/gold` | Happy path; idempotent re-run; missing Silver; path traversal rejection. Autouse `_StubLLMClient` pattern from Silver + one populated-persona case. |

## 13. Open items resolved during design

| Spec open Q | Resolution |
|-------------|------------|
| `engagement_profile` derivation | Deterministic thresholds on `close_rate`, `conversations`, and `last_seen_at` vs `batch_latest - 48h`. No LLM. Exact cut-points in `lead_profile.py`. |
| `outcome_mix` shape | `List[Struct{outcome, count}]` per D8; drift-safe; unknown outcomes preserved. |
| `top_persona_converted` tie-break | Highest `close_rate`; ties broken by absolute `closed_count`. |
| Reprocess companion table | Out of F3 scope; revisit under F4 incremental lane. |
