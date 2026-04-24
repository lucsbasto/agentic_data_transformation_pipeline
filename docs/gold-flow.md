# Gold flow — Silver → Gold, step by step

Audience: engineers new to the codebase. The PRD (§6.3, §17, §17.1)
specifies *what* Gold must produce; this doc walks through *how* the
code actually builds it, file by file, from the moment the CLI is
invoked to the moment the four parquet tables and the insights JSON
land on disk and the manifest row is committed.

Every section ends with the exact file/function you can jump to if you
want to see the implementation. Keep this doc in sync when the
sequence changes — reviewers rely on it to sanity-check PRs.

---

## 0. Entry point: `python -m pipeline gold`

File: `src/pipeline/__main__.py` → `cli.add_command(gold)`

Click dispatches `gold` to `pipeline.cli.gold:gold`. The command
accepts three flags:

| Flag            | Default        | Purpose                                          |
| --------------- | -------------- | ------------------------------------------------ |
| `--batch-id`    | required       | Silver batch to transform                        |
| `--silver-root` | `data/silver/` | Root dir holding the Silver `batch_id=<id>/` partition |
| `--gold-root`   | `data/gold/`   | Where to write the four Gold tables + insights   |

The runtime loads `Settings` once at entry, binds a fresh correlation
id (`run_id`), and clears any stale structlog context from a previous
command running in the same process.

Gold is deterministic given the same Silver input + a warm LLM cache
+ an unchanged `PROMPT_VERSION_PERSONA`. Re-running under identical
inputs produces byte-identical parquet rows modulo `computed_at` /
`generated_at` timestamps (D9).

File: `src/pipeline/cli/gold.py:gold`

---

## 1. Preflight & idempotency

Before any parquet is read, the CLI makes three cheap checks and bails
out fast if any of them fails. Order is deliberate: filesystem first
(cheapest), then manifest lineage check, then short-circuit.

1. **Path hygiene** — `_validate_batch_id` rejects anything that looks
   like a path fragment; `_safe_resolve` rejects `..` segments in
   `--silver-root` / `--gold-root`. Same shape as the Silver CLI.
2. **Silver lineage is COMPLETED** —
   `manifest.is_run_completed(batch_id, 'silver')` must be true. A
   missing or non-completed Silver run raises `GoldError` with the
   exact message `"silver run for batch '<id>' is not COMPLETED in
   the manifest; run 'python -m pipeline silver --batch-id <id>'
   first"` (F3-RF-02). Exit 1, no partial writes.
3. **Silver partition exists** — Gold reads from
   `silver_root/batch_id=<id>/part-0.parquet`. Missing file → another
   `GoldError`; no manifest row is opened.
4. **Manifest alignment** —
   - `manifest.is_run_completed(batch_id, 'gold')` short-circuits a
     repeat run with `gold already computed for batch <id>` (F3-RF-12,
     idempotency: re-running is free).
   - `manifest.delete_runs_for(batch_id, 'gold')` wipes stale
     `IN_PROGRESS` / `FAILED` runs so the new `insert_run` does not
     collide on the primary key.

File: `src/pipeline/cli/gold.py:_run_gold`

---

## 2. Open a `runs` row

A fresh `run_id` (UUID hex) is inserted as `IN_PROGRESS` with
`layer='gold'`. `started_at` is UTC wall clock; `started_wall`
captures `time.monotonic()` so the duration stopwatch survives any
clock shift. Mirror of the Silver CLI exactly.

File: `src/pipeline/state/manifest.py:insert_run`

---

## 3. Lazy scan of Silver

`pl.scan_parquet(silver_part)` returns a `LazyFrame` — a *plan*, not
a materialized table. The orchestrator uses `silver_lf.cache()` so
the four downstream builders share the same physical scan instead of
re-reading the parquet four times.

We also capture `rows_in = silver_lf.select(pl.len()).collect().item()`
up-front so the manifest row can record the input count even when the
group-bys collapse rows.

File: `src/pipeline/cli/gold.py:_run_gold` (section "try:")

---

## 4. End-to-end flow

```
data/silver/batch_id=<id>/part-0.parquet
           │
           ▼
pl.scan_parquet (lazy, cached)
           │
           ├── conversation_scores_lf ──► collect ──► assert ──► write parquet
           │
           ├── competitor_intel_lf ─────► collect ──► assert ──► write parquet
           │
           ├── lead_profile_skeleton_lf ► (collect deferred)
           │            │
           │            ├── aggregate_leads ──► persona.classify_all (async, semaphore)
           │            ├── intent_score.compute (pure Polars)
           │            │
           │            ▼
           │        collect ──► assert ──► write parquet
           │            │
           │            └── lead_personas (lead_id, persona) ──┐
           │                                                    │
           ├── agent_performance_lf (joined with lead_personas) ┘
           │            │
           │            ▼
           │        collect ──► assert ──► write parquet
           │
           └── insights.build (Silver + lead_profile)
                        │
                        ▼
                    write JSON (atomic)
```

The three pure-Polars tables stay lazy end-to-end. `lead_profile`
collects mid-flow because persona classification is Python-level: the
LLM needs an aggregated conversation per lead and the guard-rail
overrides are easier to reason about as a Python loop than as a
`pl.when/then` tree. After persona + intent_score fill, the frame is
re-asserted against `GOLD_LEAD_PROFILE_SCHEMA`. `agent_performance`
is built *after* persona is known so `top_persona_converted` can see
the LLM output (design §7.1).

File: `src/pipeline/gold/transform.py:transform_gold`

---

## 5. `gold.conversation_scores` — one row per `conversation_id`

Pure Polars. One `sort -> with_columns -> group_by -> agg -> select`
pass; Polars fuses the whole chain into a single physical execution.

### 5a. Aggregations by `conversation_id`

| Column                       | Source / formula                                                               |
| ---------------------------- | ------------------------------------------------------------------------------ |
| `lead_id`                    | first non-null per conversation (lead is invariant within a conversation)      |
| `campaign_id`, `agent_id`    | first non-null per conversation                                                |
| `msgs_inbound`               | `count_when(direction='inbound')` (Int32)                                      |
| `msgs_outbound`              | `count_when(direction='outbound')` (Int32)                                     |
| `first_message_at`           | `min(timestamp)` (Datetime[µs, UTC])                                            |
| `last_message_at`            | `max(timestamp)` (Datetime[µs, UTC])                                            |
| `duration_sec`               | `(max(ts) - min(ts)).dt.total_seconds()` (Int64; multi-day chats fit)           |
| `avg_lead_response_sec`      | mean of pairwise lead-response deltas (see §5b) (Float64)                      |
| `time_to_first_response_sec` | first non-null pairwise delta in chronological order (Float64)                  |
| `off_hours_msgs`             | `count_when(metadata.is_business_hours == FALSE)` (Int32, three-valued logic)   |
| `mencionou_concorrente`      | `concorrente_mencionado is not null` for any row in the conversation (Boolean)  |
| `concorrente_citado`         | most-frequent non-null `concorrente_mencionado` (`mode().first()`)              |
| `veiculo_marca` / `_modelo` / `_ano` | most-frequent non-null per conversation (`mode().first()`)              |
| `valor_pago_atual_brl`       | `max(valor_pago_atual_brl)` within the conversation (Float64)                  |
| `sinistro_historico`         | Kleene OR — `true` if any `true`, else `false` if any `false`, else `null`     |
| `conversation_outcome`       | last non-null after `sort_by('timestamp')`                                     |

### 5b. `avg_lead_response_sec` — pairwise lead latency (F3-RF-05)

Sort by `(conversation_id, timestamp)` then `shift(1).over('conversation_id')`
to look at the previous row. A row contributes a delta when the row is
inbound AND the previous row was outbound. Subsequent inbound replies
before the next outbound contribute nothing — they belong to the same
lead-response burst, not to a new round-trip.

Formula:
- For each `(outbound_i, inbound_j)` pair where `inbound_j` is the
  first inbound after `outbound_i`, take
  `inbound_j.timestamp - outbound_i.timestamp`.
- `avg_lead_response_sec` = mean of those deltas; `null` when no
  pair exists.
- `time_to_first_response_sec` = the first such delta in chronological
  order; `null` when only one side spoke.

This measures **lead-side** latency. The Bronze
`metadata.response_time_sec` column is **agent-side** and is *not*
used here — `agent_performance.avg_response_time_sec` is the column
that consumes it (§7).

Files: `src/pipeline/gold/conversation_scores.py:build_conversation_scores`,
`_add_pair_deltas`, `_kleene_sinistro_expr`.

---

## 6. `gold.lead_profile` — one row per `lead_id`

Two-stage build: a deterministic Polars skeleton (six columns + four
typed-null placeholders), then the persona lane fills the LLM-driven
columns and the intent-score expression fills the rest.

### 6a. Skeleton aggregations

| Column                    | Source / formula                                                    |
| ------------------------- | ------------------------------------------------------------------- |
| `conversations`           | `n_unique(conversation_id)` per lead (Int32)                        |
| `closed_count`            | per-conversation `outcome == 'venda_fechada'` summed per lead       |
| `close_rate`              | `closed_count / conversations` (Float64)                            |
| `sender_name_normalized`  | mode of Silver's reconciled `sender_name_normalized`                |
| `dominant_email_domain`   | mode of `email_domain` (ignore null)                                |
| `dominant_state` / `_city` | mode of `metadata.state` / `metadata.city`                          |
| `engagement_profile`      | first-match-wins bucket, see §6b (Enum: `hot`/`warm`/`cold`)        |
| `first_seen_at` / `last_seen_at` | `min(timestamp)` / `max(timestamp)` per lead                  |

The skeleton seeds `persona`, `persona_confidence`,
`price_sensitivity`, and `intent_score` as *typed nulls* so
`GOLD_LEAD_PROFILE_SCHEMA` is satisfied at collect time even before
the persona lane runs. Schema-drift validation works the same before
and after enrichment.

### 6b. `engagement_profile` (deterministic, no LLM)

Evaluated in order — first match wins:

1. **`hot`** if `close_rate >= 0.5` OR (`conversations >= 3` AND
   `close_rate > 0`). Clause 2 is *not* redundant: it catches
   "engaged but mediocre closer" leads (e.g. 3 conversations with 1
   close ≈ 0.33 rate) that volume-justify hot status.
2. **`cold`** if `conversations == 1` AND `closed_count == 0` AND
   `last_seen_at < batch_latest_timestamp - 48h`.
3. **`warm`** otherwise.

Boundary cases locked by tests: `(2 conv, rate 0.5)` ⇒ hot; `(1 conv,
rate 1.0)` ⇒ hot; `(1 conv, 0 closed, fresh)` ⇒ warm; `(1 conv, 0
closed, stale)` ⇒ cold.

The staleness anchor is `batch_latest_timestamp = max(timestamp)`
over the Silver frame (D12), **not** wall-clock `now()`. This keeps
re-runs of the same Silver byte-identical; using `now()` would label
more leads as `cold` if the same Silver were reprocessed a week later.

File: `src/pipeline/gold/lead_profile.py:build_lead_profile_skeleton`,
`_add_engagement_profile`.

---

## 7. Persona lane — guard-rails + LLM classifier

The persona lane runs after the skeleton collects. One persona per
`lead_id` (D2: ~15k LLM calls per batch instead of ~150k per
message).

### 7a. `LeadAggregate` — input to the classifier

`aggregate_leads` walks Silver and emits one `LeadAggregate` per
`lead_id` with these fields (design §5.2):

- `lead_id`, `num_msgs`, `num_msgs_inbound`, `num_msgs_outbound`
- `outcome` — last non-null `conversation_outcome` across the lead
- `mencionou_concorrente`, `competitor_count_distinct`
- `forneceu_dado_pessoal` = `has_cpf OR email_domain IS NOT NULL OR
  has_phone_mention`. **Only deterministic regex-derived Silver
  columns** (F3-RF-17 / D13). `valor_pago_atual_brl` is excluded —
  it is LLM-inferred and would couple two LLM lanes.
- `last_message_at` — fed to R1 for the 48h staleness check.
- `conversation_text` — concatenation of up to the last 20 lead-side
  `message_body_masked` strings ordered by `timestamp`, truncated to
  2000 chars. This is what the §18.2 prompt sees.

File: `src/pipeline/gold/persona.py:LeadAggregate`, `aggregate_leads`.

### 7b. Guard-rail engine (R2 → R1 → R3)

The three PRD §18.2 hard rules are evaluated **before** the LLM in
strict priority order (F3-RF-09). First match wins; LLM call is
skipped:

| Order | Rule | Condition                                                                        | Forced persona            |
| ----- | ---- | -------------------------------------------------------------------------------- | ------------------------- |
| 1     | R2   | `outcome == 'venda_fechada' AND num_msgs <= 10`                                   | `comprador_rapido`        |
| 2     | R1   | `num_msgs <= 4 AND outcome is null AND last_message_at < batch_latest - 48h`      | `bouncer`                 |
| 3     | R3   | `forneceu_dado_pessoal is False`                                                  | `cacador_de_informacao`   |

**R2 wins over R1.** A 4-message conversation that closed (rare but
real) has both R2 and R1's surface shape — the priority ensures it
labels as `comprador_rapido`, not `bouncer`.

**R1's 48h staleness check uses `batch_latest_timestamp`**, not
`now()`. PRD §17 defines bouncer as "sem resposta nas últimas 48h
após cotação"; the batch-anchored reference keeps Gold deterministic
under replay (D12). Trade-off: a fresh 4-msg conversation only labels
as `bouncer` after the next batch carries the staleness past the
threshold; F4's incremental lane is the right place to refine this.

A rule hit emits a `persona.override` log with the rule id, sets
`persona_source='rule'`, `persona_confidence=1.0`.

File: `src/pipeline/gold/persona.py:evaluate_rules`.

### 7c. LLM path

If no guard-rail fires, build the §18.2 prompt with the aggregate's
fields and call the async-semaphore-wrapped `LLMClient.cached_call`
(see §8) with `PROMPT_VERSION_PERSONA='v1'` embedded in the system
prompt. Expected reply: a single line matching one of the eight enum
values (trim + lower).

Failure contract:

- Any other output (unknown label, multi-line, empty) ⇒ fallback to
  `comprador_racional`, log `persona.llm_invalid` with `body_hash` +
  `response_len`. `persona_source='llm'`, `persona_confidence=0.8`.
- Provider error after the client's own retry budget ⇒ `persona=null`
  for that lead, batch keeps going. LLM unavailability is operational,
  not a row-contract violation.

### 7d. Post-LLM override

Re-check R1/R2/R3 on the returned label. If any rule would have
forced a different persona, override, emit `persona.override`, set
`persona_source='rule_override'`, `persona_confidence=1.0`. Belt-and-
braces — keeps the spec's "LLM cannot contradict hard criteria" (D3)
true even if the LLM model drifts.

File: `src/pipeline/gold/persona.py:classify_with_overrides`.

### 7e. Eight persona labels (PRD §18.2)

`pesquisador_de_preco`, `comprador_racional`, `negociador_agressivo`,
`indeciso`, `comprador_rapido`, `refem_de_concorrente`, `bouncer`,
`cacador_de_informacao`. Encoded as `pl.Enum(PERSONA_VALUES)`; the
list lives in `pipeline.schemas.gold` so callers read one source of
truth and reordering would break cached parquet.

### 7f. Budget

Reuses `Settings.pipeline_llm_max_calls_per_batch` (default 5000,
PRD §18.4). Counted only on cache misses (cache hits refund the
counter, see §8b). On exhaustion: `persona=null`,
`persona_confidence=null`, `persona_source='skipped'`, one
`persona.budget_exhausted` log per run. `intent_score` still
computes via the F3-RF-10 persona-null fallback
(`coerencia_outcome_historico_persona = 0.5`).

File: `src/pipeline/gold/persona.py:PersonaResult.skipped`.

---

## 8. LLM concurrency — `gold/concurrency.py`

5000 sequential persona calls × ~2 s each ≈ 2.7 hours, blowing the
15-minute M2 SLA. A new `Settings.pipeline_llm_concurrency` (default
16, bounded `[1, 64]`) caps in-flight calls via an `asyncio.Semaphore`
wrapping a `ThreadPoolExecutor`. 5000 calls / 16 in-flight ≈ 10 min
worst case (D11 / F3-RNF-06).

### 8a. Thread-safety contract

`LLMClient` is documented as "not thread-safe: give each concurrent
consumer its own instance"; `LLMCache` opens a SQLite connection that
refuses cross-thread access by default. The naive "share one client
across an executor" pattern raises `sqlite3.ProgrammingError` at any
concurrency > 1.

Solution: each worker thread builds its own `LLMClient` + `LLMCache`
in `_init_worker`, both opened against the same on-disk SQLite file.
SQLite WAL + `busy_timeout=5000` (set in `LLMCache.open`) keeps
concurrent writes safe. Workers store their per-thread client in
`threading.local()`; teardown happens automatically when
`executor.shutdown(wait=True)` GCs the thread-locals and SQLite
connections close as part of normal teardown.

`max_workers=settings.pipeline_llm_concurrency` is set explicitly on
the executor — `run_in_executor(None, …)` would otherwise fall back
to CPython's default pool (`min(32, cpu_count + 4)`), capping
effective concurrency around 32 even when the operator raises the
setting.

File: `src/pipeline/gold/concurrency.py:_init_worker`, `classify_all`.

### 8b. `_BudgetCounter` — back-pressure

A small thread-safe counter protects the `pipeline_llm_max_calls_per_batch`
cap from races where two threads both decide "budget is fine, fire"
past the cap:

- `try_charge_provider_call()` acquires the lock and decrements
  optimistically. Returns `False` once the budget hits zero.
- `refund_if_cache_hit(result)` re-credits the budget when the call
  turned out to be a cache hit. PRD §18.4 counts "5000 max calls" as
  5000 *cache misses*; cache hits are free by design.

The counter also tracks `calls_made` / `cache_hits` so the run's log
line carries the actual provider-call count.

File: `src/pipeline/gold/concurrency.py:_BudgetCounter`.

### 8c. Sync entry point

The orchestrator drives the coroutine via `asyncio.run(classify_all(...))`.
The CLI is sync; this is the simple bridge. A future caller from an
already-running event loop would need a different bridge.

File: `src/pipeline/gold/transform.py:_default_persona_classifier`.

---

## 9. Intent score — `gold/intent_score.py`

Pure Polars expression on lead-level aggregates. Seven weighted
components ∈ `[0, 1]`; final score cast to `Int32` and clamped to
`[0, 100]` (F3-RF-10). Any pre-clamp value outside the range emits
`intent_score.out_of_range`.

### 9a. Formula

```
score =  25 * fornecimento_dados_pessoais
      +  20 * velocidade_resposta_normalizada
      +  15 * presenca_termo_fechamento
      +  15 * perguntas_tecnicas_normalizado
      +  10 * ausencia_palavras_evasivas
      +  10 * coerencia_outcome_historico_persona
      +   5 * janela_horaria_comercial
```

The weights sum to 100 (import-time guard in `intent_score.py`).

### 9b. Component sources

| Component                              | Source                                                                          |
| -------------------------------------- | ------------------------------------------------------------------------------- |
| `fornecimento_dados_pessoais`          | `1.0 if forneceu_dado_pessoal else 0.0` (same signal as R3)                     |
| `velocidade_resposta_normalizada`      | `1 - min(avg_lead_response_sec / 600, 1)` (lead-side latency; 10-min cap ⇒ 0)   |
| `presenca_termo_fechamento`            | regex hits on lead-side body for `{"fechou", "manda boleto", "tá bom"}`         |
| `perguntas_tecnicas_normalizado`       | `min(count of {"franquia","cobre","assistência","carro reserva"} / 3, 1.0)`     |
| `ausencia_palavras_evasivas`           | `1 - min(count of {"vou pensar","depois te aviso","preciso ver"} / 3, 1.0)`     |
| `coerencia_outcome_historico_persona`  | `PERSONA_EXPECTED_OUTCOME` lookup: 1.0 match / 0.0 contradiction / 0.5 neutral  |
| `janela_horaria_comercial`             | `1 - off_hours_msgs / num_msgs_inbound` (0.5 fallback when `num_msgs_inbound = 0`) |

### 9c. F3-RF-10 carve-out — null persona

When `persona is null` (budget-skipped or LLM-failed) **OR**
`outcome is null`, `coerencia_outcome_historico_persona` falls back
to `0.5` (neutral) instead of computing the lookup. This is the
single concession that lets `intent_score` survive an unavailable LLM
without losing all 10 of its weight points; it keeps the score
comparable across leads regardless of LLM lane status (D6).

A "neutral" persona (`comprador_racional`) also returns 0.5 on the
lookup — by design, since that label has no predicted outcome.

Component inputs are aggregated per lead in
`transform._compute_intent_score_inputs`: `forneceu_dado_pessoal`,
`avg_lead_response_sec`, `closing_phrase_hits`,
`technical_question_hits`, `evasive_phrase_hits`, `outcome`,
`off_hours_msgs`, `num_msgs_inbound`. `persona` is joined separately
from the LLM-lane output before `compute_intent_score` runs.

Files: `src/pipeline/gold/intent_score.py:compute_intent_score`,
`intent_score_expr`; `src/pipeline/gold/_regex.py:CLOSING_PHRASES`,
`TECHNICAL_QUESTION_PHRASES`, `EVASIVE_PHRASES`, `count_phrase_hits`.

### 9d. `price_sensitivity` (deterministic, F3-RF-16)

Computed alongside the intent-score inputs. Let
`c = competitor_count_distinct` and `h = haggling_phrase_hits` (regex
on lead-side body for `{"desconto","abaixar","mais barato","outro
lugar","tá caro","melhor preço"}`):

- `high` if `c >= 3 OR h >= 2`
- `medium` if `c >= 1 OR h >= 1`
- `low` otherwise

Cast to `pl.Enum(PRICE_SENSITIVITY_VALUES)`. No LLM dependency.

File: `src/pipeline/gold/_regex.py:HAGGLING_PHRASES`.

---

## 10. `gold.agent_performance` — one row per `agent_id`

Built **after** the persona lane so `top_persona_converted` sees the
LLM output. Group-by `agent_id`:

| Column                     | Source / formula                                                                  |
| -------------------------- | --------------------------------------------------------------------------------- |
| `conversations`            | distinct `conversation_id` count per agent (Int32)                                |
| `closed_count`             | distinct conversations with `outcome == 'venda_fechada'` (Int32)                  |
| `close_rate`               | `closed_count / conversations` (Float64)                                          |
| `avg_response_time_sec`    | `mean(metadata.response_time_sec)` over the agent's outbound rows (agent-side)    |
| `outcome_mix`              | `List[Struct{outcome: String, count: Int32}]` — every outcome value preserved     |
| `top_persona_converted`    | persona with highest `close_rate` among the agent's leads; ties → absolute `closed_count`; null when the agent has zero closed conversations |

`outcome_mix` is **`List[Struct]`, not fixed-field `Struct`** (D8).
Bronze's `conversation_outcome` enum can widen later; fixed fields
would silently drop new outcomes (same class as the F1 Enum-metadata
parquet bug). The list always sums to `conversations`, no warning,
no data loss when an unknown outcome appears.

`top_persona_converted` may be null (zero closed conversations); the
`pl.Enum(PERSONA_VALUES)` constraint still applies to non-null values.

File: `src/pipeline/gold/agent_performance.py:build_agent_performance`.

---

## 11. `gold.competitor_intel` — one row per normalized competitor

Group-by `concorrente_mencionado.str.strip_chars().str.to_lowercase()`
(D7) so `"Porto Seguro"` and `"porto seguro"` collapse. Rows with a
null mention are dropped before grouping — every output row
represents at least one mention.

| Column                | Source / formula                                                                |
| --------------------- | ------------------------------------------------------------------------------- |
| `competitor`          | normalized name (PK)                                                             |
| `mention_count`       | count of raw rows mentioning this competitor (Int32)                            |
| `avg_quoted_price_brl`| `mean(valor_pago_atual_brl)` filtered to rows mentioning this competitor; `null` when no prices observed |
| `loss_rate`           | `sum(distinct conversation_outcome != 'venda_fechada') / distinct mentioning conversations` (one lead can mention the same competitor many times — distinct-conversation count is the meaningful denominator) |
| `top_states`          | top 3 `metadata.state` values by mention volume (`List[String]`); null-state competitors land with `top_states = null` after the left-join |

After the F2 v1 prompt tightening, `valor_pago_atual_brl` carries
only lead-stated current premiums (the earlier seller-quoted
contamination is gone), so `avg_quoted_price_brl` reflects what the
lead said they currently pay.

File: `src/pipeline/gold/competitor_intel.py:build_competitor_intel`.

---

## 12. Insights JSON — `gold/insights.py`

Reads the four collected Gold frames (no re-scan of Silver beyond
the already-cached LazyFrame) and writes
`gold_root/insights/batch_id=<id>/summary.json`. Four keys:

- `ghosting_taxonomy` — ghosted leads (`outcome is null AND
  closed_count = 0`) bucketed by message count `{1-2, 3-4, 5-9, 10+}`,
  with `count` + `rate` vs all leads.
- `objections` — regex catalog on lead-side text for `{"tá caro",
  "vou pensar", "depois", "me dá desconto", "outro lugar"}`, each
  with `count`, `close_rate_when_present`, one-sentence `note`.
- `disengagement_moment` — for ghosted leads, distribution of the
  outbound message index at which the lead stopped responding.
  Two-axis breakdown: *message index* `{first, 2nd–3rd, 4th+}` ×
  *content regex* `{after first quote, after price ask, after
  personal data ask, other}`. The two-axis shape stops "other" from
  drowning the signal.
- `persona_outcome_correlation` — crosstab `persona × outcome` with
  `count` and `rate`, plus the top 1 "surprising" pair (largest
  absolute deviation from the overall close rate).

### 12a. `determinism` block

Every JSON carries a `determinism` field describing the determinism
contract of *this* run (D9):

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

Three of the four insights are pure regex + count aggregates over
Silver and so are **always** deterministic given the same Silver.
`persona_outcome_correlation` depends on LLM-sourced persona labels
and is therefore deterministic only under fixed Silver + warm cache
+ unchanged `PROMPT_VERSION_PERSONA`. Over-claiming "always
deterministic" would hide the LLM dependency; under-claiming would
miss that cache hits genuinely are deterministic.

File: `src/pipeline/gold/insights.py:build_insights`.

---

## 13. Atomic write protocol — `gold/writer.py`

Every output is staged in a `.tmp-...` directory next to its final
partition, then promoted with `os.replace` (POSIX `rename(2)`) once
the bytes are flushed. `rename` is atomic within the same filesystem
— readers see either the previous good file or the new one, never a
half-written byte range. The temp dir lives under the same
`gold_root` so the swap stays on one filesystem.

### 13a. Parquet writers

`_write_parquet_atomic(df, gold_root, batch_id, table)`:

1. Re-asserts the canonical Gold schema before any bytes touch disk;
   drift surfaces as `SchemaDriftError` instead of a corrupt parquet
   that only fails downstream.
2. Stages `<gold_root>/<table>/.tmp-batch_id=<id>/part-0.parquet`
   (zstd compressed).
3. If a final dir exists (re-run scenario) → `shutil.rmtree`. Then
   `tmp_dir.replace(final_dir)` — the atomic swap.
4. On any failure, `shutil.rmtree(tmp_dir, ignore_errors=True)` so
   repeat runs start clean.

Output paths (per F3-RF-03):

```
<gold_root>/conversation_scores/batch_id=<id>/part-0.parquet
<gold_root>/lead_profile/batch_id=<id>/part-0.parquet
<gold_root>/agent_performance/batch_id=<id>/part-0.parquet
<gold_root>/competitor_intel/batch_id=<id>/part-0.parquet
```

Each `write_gold_*` call returns a `GoldWriteResult(gold_path,
rows_written)`.

### 13b. Insights JSON writer

`write_gold_insights(payload, gold_root, batch_id)` follows the same
temp-then-rename pattern at file granularity:

1. Serialise to a sibling `summary.json.tmp.<pid>` file.
2. `flush()` then `os.fsync(fh.fileno())` to push the bytes through
   the OS page cache.
3. `mkdir` the final dir.
4. `os.replace(tmp_file, final_file)`.

Tracks `final_dir_pre_existed` so a failed first-time write leaves
no trace; a failed re-write leaves the previous good file alone.

Output path: `<gold_root>/insights/batch_id=<id>/summary.json`.

File: `src/pipeline/gold/writer.py:_write_parquet_atomic`,
`write_gold_insights`.

---

## 14. Commit manifest run

On success, `mark_run_completed` flips the `IN_PROGRESS` row to
`COMPLETED` and records (F3-RF-13):

- `rows_in` — Silver row count (the `pl.len()` collect from §3).
- `rows_out` — sum of the four Gold table heights (insights JSON is
  not counted).
- `output_path` — `<gold_root>/batch_id=<id>/` (parent dir).
- `duration_ms` — `time.monotonic()` delta.
- `finished_at` — UTC wall clock (ISO-8601).

On any raised `PipelineError` in the try-block, `mark_run_failed` is
called instead with the error class name and message. A non-pipeline
exception (raw `RuntimeError`, etc.) is wrapped in `GoldError` so
callers see a single error hierarchy.

File: `src/pipeline/state/manifest.py:mark_run_completed`,
`mark_run_failed`.

---

## 15. Operator-visible output

`click.echo(...)` prints a one-liner with the four table row counts,
the gold root, the `run_id`, and `duration_ms`. The final `structlog`
event is `gold.complete` with the same fields as structured JSON.
Per-table `gold.<table>.done` events fire at each builder leaf
(F3-RF-15); the persona lane emits `gold.persona.done` with a
`persona_distribution` dict keyed by enum label.

---

## 16. Error handling contract

| Failure                                  | Behaviour                                                              |
| ---------------------------------------- | ---------------------------------------------------------------------- |
| Silver run missing or not COMPLETED      | `GoldError` at preflight; exit 1; no manifest row opened; no writes    |
| Silver partition file missing            | `GoldError`; same as above                                             |
| Persona LLM call fails after retries     | `persona=null` on that lead; batch keeps going; `persona.llm_invalid` log |
| Persona LLM budget exhausted             | remaining leads get `persona=null`; one `persona.budget_exhausted` log |
| Insights JSON build error                | `GoldError`; deterministic structural failure aborts the run           |
| Schema drift on any of the 4 tables      | `SchemaDriftError` on the first offending table; no downstream writes  |
| Any other `PipelineError`                | `mark_run_failed` records it; partial writes are NOT rolled back from disk (the next idempotent re-run overwrites them via the atomic swap) |

Error hierarchy: `PipelineError` → `GoldError` (file
`src/pipeline/errors.py`). `SchemaDriftError`, `ManifestError`, and
the LLM-side `LLMCallError` are siblings, all caught by the CLI's
`except PipelineError` clause.

---

## Glossary of files touched

| Module                                | Role                                                  |
| ------------------------------------- | ----------------------------------------------------- |
| `pipeline/cli/gold.py`                | Click command + orchestration + manifest gating       |
| `pipeline/gold/transform.py`          | Compose the four builders + persona + intent_score    |
| `pipeline/gold/conversation_scores.py`| Silver → `gold.conversation_scores` (pure Polars)     |
| `pipeline/gold/lead_profile.py`       | Silver → `gold.lead_profile` skeleton + engagement    |
| `pipeline/gold/agent_performance.py`  | Silver + lead_personas → `gold.agent_performance`     |
| `pipeline/gold/competitor_intel.py`   | Silver → `gold.competitor_intel`                      |
| `pipeline/gold/persona.py`            | LeadAggregate, R1/R2/R3 engine, LLM classifier        |
| `pipeline/gold/concurrency.py`        | Async semaphore + per-thread LLM client + budget      |
| `pipeline/gold/intent_score.py`       | PRD §17.1 weighted-sum formula (pure Polars)          |
| `pipeline/gold/insights.py`           | Four-key `summary.json` builder + determinism block   |
| `pipeline/gold/writer.py`             | Atomic parquet + JSON writers                         |
| `pipeline/gold/_regex.py`             | Phrase catalogs (closing/technical/evasive/haggling/objection) |
| `pipeline/schemas/gold.py`            | 4 `pl.Schema` constants + 4 `assert_*_schema` validators + persona/engagement/price-sensitivity enums |
| `pipeline/schemas/manifest.py`        | `runs` table layer constants                          |
| `pipeline/state/manifest.py`          | Manifest DB API                                       |
| `pipeline/errors.py`                  | `GoldError` (subclass of `PipelineError`)             |

---

## Test map

| Test file                                          | Covers                                                |
| -------------------------------------------------- | ----------------------------------------------------- |
| `tests/unit/test_gold_schemas.py`                  | Column order, dtypes, persona enum, `outcome_mix` shape, `assert_*` drift messages |
| `tests/unit/test_gold_conversation_scores.py`      | Grain, every aggregation, pairwise `avg_lead_response_sec`, vehicle-field reconciliation |
| `tests/unit/test_gold_lead_profile.py`             | Grain, `close_rate`, dominant email/state/city, engagement bucket boundaries |
| `tests/unit/test_gold_persona.py`                  | R1/R2/R3 positive + negative; LLM valid path; LLM invalid → `comprador_racional` fallback; budget exhaustion → null; post-LLM override |
| `tests/unit/test_gold_intent_score.py`             | Property test (Int32 ∈ [0,100] over `{0.0,0.5,1.0}^7`); each component isolated; persona-null branch uses 0.5 for coerência |
| `tests/unit/test_gold_agent_performance.py`        | `outcome_mix` preserves unknown outcomes; `top_persona_converted` tie-break |
| `tests/unit/test_gold_competitor_intel.py`         | Competitor normalization; `mention_count` / `avg_quoted_price_brl` / `loss_rate` distinct-conversation behaviour |
| `tests/unit/test_gold_insights.py`                 | Shape stable; empty inputs ⇒ empty arrays + explicit `count=0`; `determinism` block present |
| `tests/unit/test_gold_concurrency.py`              | Semaphore limit respected; cache hits bypass semaphore; budget back-pressure stops past cap |
| `tests/unit/test_gold_writer.py`                   | Atomic swap; re-run overwrite; failed write leaves no trace |
| `tests/integration/test_cli_gold_e2e.py`           | End-to-end CLI → 4 parquet + JSON → manifest; idempotent re-run; missing Silver; path traversal rejection |
