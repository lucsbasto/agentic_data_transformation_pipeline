# F3 — Spec: Gold analytics + personas

Drives PRD §6.3 (Gold tables), §8 RF-04, §17 (persona taxonomy + hard
guard-rails), §17.1 (intent score), §18.2 (persona prompt), §18.4
(cache + budget). Depends on F2 Silver being COMPLETED for the same
`batch_id`.

## 1. Intent

Turn a Silver batch into four analytical parquet tables and one
insights summary, keyed off the same `batch_id`:

- `gold.conversation_scores` — 1 row per `conversation_id`. (PRD
  §6.3 calls this `conversation_features`; renamed per D10.)
- `gold.lead_profile` — 1 row per `lead_id`, with LLM persona + intent
  score (PRD §17, §17.1).
- `gold.agent_performance` — 1 row per `agent_id`.
- `gold.competitor_intel` — 1 row per normalized competitor name.
- `gold.insights/batch_id=<id>/summary.json` — non-obvious findings
  (PRD §6.3 bullets: ghosting, objeções, desengajamento,
  perfil × outcome).

Gold is deterministic **given the same Silver inputs + a warm LLM
cache + an unchanged `PROMPT_VERSION_PERSONA`**. Any one of those
moving changes persona labels and therefore the downstream insights
(see D9). Re-running under identical inputs produces byte-identical
parquet rows modulo `computed_at` / `generated_at` timestamps.

## 2. Source contract (input)

- `data/silver/batch_id=<id>/part-0.parquet` matching
  `pipeline.schemas.silver.SILVER_SCHEMA` (32 columns including the
  six F2 LLM-extracted entity columns).
- The manifest has a `runs` row with `layer='silver'`,
  `batch_id=<id>`, `status='COMPLETED'`. F3 refuses to start
  otherwise.

## 3. Functional requirements

| ID | Requirement |
|---|---|
| F3-RF-01 | CLI: `python -m pipeline gold --batch-id <bid>`. `--batch-id` required; no `--latest` (defer to F5). |
| F3-RF-02 | Refuse to run unless the companion Silver run for `batch_id` is `status='COMPLETED'`. Error: `"silver run for batch {bid} is not COMPLETED"`. |
| F3-RF-03 | Writes four parquet partitions: `data/gold/<table>/batch_id=<bid>/part-0.parquet` where `<table>` ∈ {`conversation_scores`, `lead_profile`, `agent_performance`, `competitor_intel`}. Plus `data/gold/insights/batch_id=<bid>/summary.json`. |
| F3-RF-04 | Each table has a declared `pl.Schema` in `pipeline/schemas/gold.py` — same drift-guard pattern as Silver. A per-table `assert_<table>_schema(df)` raises `SchemaDriftError` on mismatch. |
| F3-RF-05 | **`gold.conversation_scores` columns.** `conversation_id` (PK), `lead_id`, `campaign_id`, `agent_id`, `msgs_inbound` / `msgs_outbound` (Int32), `first_message_at` / `last_message_at` (Datetime[µs, UTC]), `duration_sec` (Int64), `avg_lead_response_sec` (Float64; see below), `time_to_first_response_sec` (Float64, null if only one side spoke), `off_hours_msgs` (Int32), `mencionou_concorrente` (Boolean), `concorrente_citado` (String, most-frequent non-null), `veiculo_marca` / `veiculo_modelo` / `veiculo_ano` (reconciled most-frequent non-null), `valor_pago_atual_brl` (Float64, max), `sinistro_historico` (Boolean, `true` if any `true`, else `false` if any `false`, else null), `conversation_outcome` (last non-null). **`avg_lead_response_sec` is the mean, over all `(outbound_i, inbound_j)` pairs where `inbound_j` is the first inbound after `outbound_i`, of `inbound_j.timestamp - outbound_i.timestamp`. Null when no such pair exists.** It measures lead-side latency, not agent-side. `metadata.response_time_sec` from Bronze is agent-side and is NOT used here. |
| F3-RF-06 | **`gold.lead_profile` columns.** `lead_id` (PK), `conversations` (Int32), `closed_count` (Int32), `close_rate` (Float64), `sender_name_normalized` (String, canonical from Silver reconcile), `dominant_email_domain` (String), `dominant_state` / `dominant_city` (String), `engagement_profile` (Enum: `hot`/`warm`/`cold`), `persona` (Enum, eight PRD §17 values — F3-RF-09), `persona_confidence` (Float64 ∈ [0,1]), `price_sensitivity` (Enum: `low`/`medium`/`high`; derivation in F3-RF-16), `intent_score` (Int32 ∈ [0,100]), `first_seen_at` / `last_seen_at` (Datetime[µs, UTC]). |
| F3-RF-07 | **`gold.agent_performance` columns.** `agent_id` (PK), `conversations` (Int32), `closed_count` (Int32), `close_rate` (Float64), `avg_response_time_sec` (Float64, mean of `metadata.response_time_sec` for agent's outbound messages), `outcome_mix` (**`List[Struct{outcome: String, count: Int32}]`** — enum-drift-safe; see D8), `top_persona_converted` (Enum of the eight personas, or null when the agent has zero closed conversations). |
| F3-RF-08 | `gold.competitor_intel` columns: `competitor` (PK; lower-cased + trimmed), `mention_count` (Int32), `avg_quoted_price_brl` (Float64, null if no prices observed), `loss_rate` (Float64 ∈ [0,1], null if mention_count=0 after filter), `top_states` (List[String], up to 3). |
| F3-RF-09 | Persona classification follows PRD §17 + §18.2 exactly — eight labels: `pesquisador_de_preco`, `comprador_racional`, `negociador_agressivo`, `indeciso`, `comprador_rapido`, `refem_de_concorrente`, `bouncer`, `cacador_de_informacao`. Three PRD §18.2 hard rules override the LLM output when they fire, evaluated in priority order R2 → R1 → R3: **(R2)** `outcome='venda_fechada' AND messages ≤10` ⇒ `comprador_rapido`; **(R1)** `messages ≤4 AND outcome is null AND last_message_at < batch_latest_timestamp - 48h` ⇒ `bouncer` (48h silence window from PRD §17); **(R3)** `forneceu_dado_pessoal is False` ⇒ `cacador_de_informacao`. An override emits `persona.override` with the triggered rule. |
| F3-RF-10 | **Intent score** is the PRD §17.1 formula (25+20+15+15+10+10+5 = 100 weights). The `coerencia_outcome_historico_persona` component is **0.5 (neutral)** when `persona` is null (budget-skipped or LLM-failed); 1.0 on match, 0.0 on contradiction otherwise. The result is cast to `Int32` and clamped to `[0, 100]`; any pre-clamp value outside the range emits `intent_score.out_of_range`. |
| F3-RF-11 | Insights JSON carries four keys: `ghosting_taxonomy`, `objections`, `disengagement_moment`, `persona_outcome_correlation`. Each value is a list of records with `label`, `count`, optional `rate`, and a one-sentence `note`. Output is deterministic with the caveats documented in §1. |
| F3-RF-12 | Idempotency: short-circuit when `runs` has `(batch_id=<bid>, layer='gold', status='COMPLETED')`. Stale `IN_PROGRESS` / `FAILED` rows are wiped before the new `insert_run`. Mirrors F2 pattern. |
| F3-RF-13 | Manifest: reuse `runs` table with `layer='gold'`. `rows_in` = total Silver rows consumed, `rows_out` = sum of the 4 table row counts, `output_path` = `data/gold/batch_id=<id>/` (parent dir). |
| F3-RF-14 | LLM budget: reuse `pipeline_llm_max_calls_per_batch`. Persona extraction dedupes by `lead_id`, so at most `n_leads` calls per batch. Budget exhaustion ⇒ `persona=null`, `persona_confidence=null`, `intent_score` still computed (F3-RF-10 covers the persona-null branch). |
| F3-RF-15 | Structured log events: `gold.start`, `gold.conversation_scores.done`, `gold.lead_profile.done` (with `persona_distribution`), `gold.agent_performance.done`, `gold.competitor_intel.done`, `gold.insights.done`, `gold.complete` / `gold.failed`. |
| F3-RF-16 | **`price_sensitivity` derivation** (deterministic, no LLM). Let `c = competitor_count_distinct` across the lead's Silver rows and `h = haggling_phrase_hits` (regex on lead-side `message_body_masked` for `{"desconto", "abaixar", "mais barato", "outro lugar", "tá caro", "melhor preço"}`). `high` if `c ≥ 3 OR h ≥ 2`; `medium` if `c ≥ 1 OR h ≥ 1`; `low` otherwise. |
| F3-RF-17 | **`forneceu_dado_pessoal`** (used by R3 + intent-score `fornecimento_dados_pessoais`) = `has_cpf` OR `email_domain present` OR `has_phone_mention`. **Excludes `valor_pago_atual_brl`** — that column is LLM-inferred and would couple two LLM lanes even after the F2 prompt tightening. |

## 4. Non-functional requirements

| ID | Requirement |
|---|---|
| F3-RNF-01 | Polars lazy-first: `scan_parquet` on Silver → lazy group-bys → `collect(streaming=True)` on write. No pandas. |
| F3-RNF-02 | Persona classifier tested with a fake LLM client (mirror `test_silver_llm_extract.py`). No live DashScope in unit or integration tests. |
| F3-RNF-03 | Guard-rail enforcement is pure (no LLM dependency) and has its own unit tests — one positive + one negative per rule. |
| F3-RNF-04 | Intent-score formula is a pure Polars expression on lead-level aggregates. |
| F3-RNF-05 | Ruff clean, pytest coverage ≥ 90% on `src/pipeline/gold/**` and `schemas/gold.py`. |
| F3-RNF-06 | **End-to-end budget** — on the 153k-row fixture, Bronze → Silver → Gold completes under 15 minutes with the default `pipeline_llm_max_calls_per_batch=5000` **AND** `pipeline_llm_concurrency ≥ 16` (F3-RF-18 / D11). Sequential persona calls would blow the SLA (5000 × ~2 s ≈ 2.7 h); concurrency is load-bearing, not optional. |
| F3-RNF-07 | `PROMPT_VERSION_PERSONA='v1'` embedded in the persona system prompt so cache keys bust on edits — same pattern as Silver extraction. |
| F3-RF-18 / F3-RNF-08 | **LLM concurrency primitive.** A new `pipeline_llm_concurrency` setting (default 16, bounded [1, 64]) caps the number of in-flight persona calls. Implemented as an `asyncio.Semaphore` wrapping `LLMClient.cached_call` in a thin async adapter (blocking calls inside `run_in_executor`). Cache hits short-circuit the semaphore — free is free. The same primitive is reusable from F2 Silver extraction in a follow-up; F3 introduces it. |

## 5. Decisions

| # | Question | Decision | Why |
|---|---|---|---|
| D1 | Gold table layout | Four separate parquet partitions (`data/gold/<table>/batch_id=<id>/…`). One JSON per batch for insights. | Each table has its own grain; merging would force readers to filter. Insights are narrative — JSON fits better than parquet. |
| D2 | Persona grain | One per `lead_id`, not per conversation or per message. | Cost: 15k leads × one call vs ~150k messages. Matches PRD §17. |
| D3 | Guard-rail vs LLM precedence | Hard criteria **win** every time. LLM label is overridden with a `persona.override` log event. | PRD §17: "LLM não pode contradizer critérios duros". |
| D4 | Incremental Gold | Out of scope for F3. Full Gold rebuild per Silver batch. Incremental lands in F4 alongside the agent loop. | Keeps F3 linear. Full M2 fixture still runs under 15 min because Gold work is mostly Polars group-bys, and the LLM lane is concurrency-capped (D11). |
| D5 | Insights output | JSON, not parquet. One file per batch under `gold/insights/batch_id=<id>/summary.json`. | Insights mix counts, rates, and narrative strings. Parquet would flatten them. |
| D6 | Missing LLM budget | `persona=null`; `intent_score` still computed with `coerencia_outcome_historico_persona = 0.5` neutral. | Matches Silver's lane contract: LLM unavailability is transient, not a row-contract violation. |
| D7 | Competitor_intel grain | Lower-cased, stripped `concorrente_mencionado` (so `"Porto Seguro"` and `"porto seguro"` collapse). | LLM output is free-text; normalization is the first thing any analytics reader would do. |
| D8 | `outcome_mix` shape | `List[Struct{outcome: String, count: Int32}]`, NOT fixed-field Struct. | Bronze's outcome enum can widen later; fixed fields would silently drop new outcomes (same class as the F1 Enum-metadata bug in parquet). A list preserves every count, always sums to `conversations`. |
| D9 | Insights determinism claim | Deterministic **given fixed Silver + warm cache + unchanged `PROMPT_VERSION_PERSONA`**. Documented in §1 and F3-RF-11. | Over-claiming "always deterministic" hides the LLM dependency; under-claiming would miss that cache hits genuinely are deterministic. |
| D10 | PRD table rename | PRD §6.3 `conversation_features` → F3 `conversation_scores`. | The deliverable is a per-conversation *scoring* table, and "features" collides with ML feature-store vocabulary; the rename pre-empts confusion when F4/F5 bolts on. |
| D11 | LLM concurrency story | Async semaphore with default 16 in-flight calls (F3-RF-18). | 5000 sequential calls × ~2 s ≈ 2.7 h blows the 15-minute M2 SLA. 16 in-flight × ~2 s ≈ ~10 min for 5000 calls, well within budget. DashScope per-model rate limits accommodate 16+ concurrent calls on `qwen3-coder-plus`. |
| D12 | Bouncer rule staleness | R1 requires `last_message_at < batch_latest_timestamp - 48h` in addition to `messages ≤ 4` and `outcome is null`. **Reference is `batch_latest_timestamp = max(timestamp)` over the Silver frame, NOT wall-clock `now()`.** | PRD §17 Bouncer definition explicitly says "sem resposta nas últimas 48h após cotação". Without the staleness check, an in-flight 2-msg conversation is wrongly forced to `bouncer`. Using `now()` would make Gold non-deterministic (the same Silver batch reprocessed a week later would label more leads as `bouncer`); using `batch_latest_timestamp` keeps a re-run of the same Silver byte-identical, at the cost of potentially marking some leads `bouncer` later than wall-clock would. F4's incremental rebuild lane is the right place to refine this if the lag becomes operationally visible. |
| D13 | `forneceu_dado_pessoal` signal | `has_cpf OR email_domain OR has_phone_mention`; NOT `valor_pago_atual_brl`. | All three are deterministic regex-derived Silver columns. `valor_pago_atual_brl` is LLM-inferred; using it to gate the `cacador_de_informacao` rule would couple two LLM lanes and re-introduce the false-positive exposure that F2's prompt fix just closed. |

## 6. Data contract (output)

```
data/gold/
  conversation_scores/batch_id=<id>/part-0.parquet
  lead_profile/batch_id=<id>/part-0.parquet
  agent_performance/batch_id=<id>/part-0.parquet
  competitor_intel/batch_id=<id>/part-0.parquet
  insights/batch_id=<id>/summary.json
```

Each parquet matches its declared `pl.Schema` (F3-RF-05..08). The
`batch_id=<id>` partitioning mirrors Silver so lineage stays 1:1.

## 7. Acceptance criteria

- `python -m pipeline gold --batch-id <id>` on a COMPLETED Silver run
  produces the four parquet tables + the insights JSON, records a
  `runs` row with `layer='gold'`, `status='COMPLETED'`, and exits 0.
- `pytest tests/unit/test_gold_*.py tests/integration/test_cli_gold_e2e.py`
  green with ≥90% coverage on `src/pipeline/gold/**` and
  `schemas/gold.py`.
- Re-running on the same batch short-circuits with
  `"gold already computed"`.
- Every persona rule (R1, R2, R3) has at least one positive and one
  negative unit-test case.
- `intent_score` is always an `Int32` in `[0, 100]` (property test).
- E2E: on the 153k-row fixture, Bronze → Silver → Gold completes
  under 15 minutes with default LLM budget + concurrency, and
  `gold.insights.summary.json` contains at least three non-trivial
  entries per category.

## 8. Open questions (non-blocking)

- Does `engagement_profile` deserve its own LLM pass, or are the
  deterministic thresholds in `design.md` §3 good enough? Decide from
  a small annotated sample during F3 execution.
- Should `top_persona_converted` tie-break on `close_rate` or
  absolute `closed_count`? Pin during design.
- Incremental Gold companion table (`gold.leads_to_reprocess`) —
  belongs to F4 alongside the agent loop; listed here so it is not
  forgotten.
