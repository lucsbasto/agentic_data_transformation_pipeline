# F3 — Tasks: Gold analytics + personas

Atomic, commit-sized tasks. Each ships as its own conventional commit
(`feat(F3)` / `docs(F3)` / `test(F3)` / `fix(F3)` / `refactor(F3)`),
mirroring the F2 rhythm.

Legend: ✅ done · 🟡 in progress · ⚪ pending

## Backbone (settings, schema, infra)

- ⚪ **F3.0** — `docs(F3): scaffold spec/design/tasks and update roadmap` (this commit; bumps ROADMAP F3 status to 🟡 spec'd).
- ⚪ **F3.1** — `feat(F3): declare gold parquet schemas + assert_*_schema validators` (`src/pipeline/schemas/gold.py`, four `pl.Schema` constants + per-table validators + persona enum + `outcome_mix` `List[Struct]`). Tests: `tests/unit/test_schemas_gold.py` locks every column / dtype / enum value.
- ⚪ **F3.2** — `feat(F3): wire pipeline_llm_concurrency through settings` (`src/pipeline/settings.py` adds the new field, `.env.example` ships the default, `tests/unit/test_settings.py` covers default + override + bound rejections).
- ⚪ **F3.3** — `feat(F3): centralise gold regex catalogues` (`src/pipeline/gold/_regex.py` declares the five shared sets — `OBJECTION_PHRASES`, `HAGGLING_PHRASES`, `CLOSING_PHRASES`, `EVASIVE_PHRASES`, `TECHNICAL_QUESTION_PHRASES` — so `intent_score`, `price_sensitivity`, `insights` import the same constants and never drift).

## Pure-Polars Gold builders

- ⚪ **F3.4** — `feat(F3): conversation_scores builder with avg_lead_response_sec` (`gold/conversation_scores.py` + `tests/unit/test_gold_conversation_scores.py`). Locks the pairwise-pair averaging from spec F3-RF-05 with table-driven sequences.
- ⚪ **F3.5** — `feat(F3): competitor_intel builder with normalized competitor names` (`gold/competitor_intel.py` + tests; covers price-loss-rate computation on distinct conversations).
- ⚪ **F3.6** — `feat(F3): agent_performance builder with drift-safe outcome_mix` (`gold/agent_performance.py` + tests; asserts `List[Struct]` shape, `top_persona_converted` tie-break, and that an unknown outcome value lands in `outcome_mix` rather than being dropped).
- ⚪ **F3.7** — `feat(F3): lead_profile skeleton (pre-persona)` (`gold/lead_profile.py` + tests; per-lead aggregations, `engagement_profile` boundary tests, leaves `persona`/`persona_confidence`/`intent_score` as typed nulls so the schema asserts).

## Persona lane (LLM + guard-rails)

- ⚪ **F3.8** — `feat(F3): lead aggregate + guard-rail rule engine` (`gold/persona.py` skeleton with `LeadAggregate`, the `R2 → R1 → R3` evaluator, `PERSONA_EXPECTED_OUTCOME` lookup table, and `batch_latest_timestamp` helper). Tests: positive + negative for each rule, including R1's 48-h staleness against an in-flight conversation (regression for D12).
- ⚪ **F3.9** — `feat(F3): persona LLM classifier with prompt and post-LLM override` (`gold/persona.py` LLM path; system prompt embeds `PROMPT_VERSION_PERSONA='v1'`; invalid response ⇒ fallback + log; rule re-check after LLM). Unit tests use the same fake-Anthropic pattern as `test_silver_llm_extract.py`.
- ⚪ **F3.10** — `feat(F3): thread-safe persona concurrency lane` (`gold/concurrency.py`: `ThreadPoolExecutor` with per-thread `LLMClient`+`LLMCache` via `_init_worker`, `_BudgetCounter` with optimistic charge + cache-hit refund, async semaphore wrapper). Tests: semaphore caps in-flight tasks, cache hits refund the budget, budget exhaustion short-circuits without scheduling further provider work.

## Intent score + insights

- ⚪ **F3.11** — `feat(F3): intent_score formula with persona-null fallback` (`gold/intent_score.py` + tests). Property test over random component vectors ⇒ score `Int32 ∈ [0,100]`; explicit case for `persona=null ⇒ coerencia=0.5`.
- ⚪ **F3.12** — `feat(F3): insights JSON builder` (`gold/insights.py` + tests). Three deterministic insights (`ghosting_taxonomy`, `objections`, `disengagement_moment`) and one LLM-dependent (`persona_outcome_correlation`). JSON carries the `determinism` block.

## Wiring + CLI + writer

- ⚪ **F3.13** — `feat(F3): atomic gold writer (parquet + json)` (`gold/writer.py` + tests; tmp-then-rename pattern, mirrors Silver writer).
- ⚪ **F3.14** — `feat(F3): gold transform orchestrator` (`gold/transform.py` composes the four builders, runs the persona lane, computes intent_score, asserts every schema). Unit-tested with a mini Silver fixture.
- ⚪ **F3.15** — `feat(F3): python -m pipeline gold cli` (`cli/gold.py` mirrors Silver CLI: `--batch-id` required, manifest run idempotency, refuses to run unless Silver `runs.layer='silver'` is COMPLETED). Integration test `tests/integration/test_cli_gold_e2e.py` autouse-stubs `LLMClient` and adds a populated-persona case.
- ⚪ **F3.16** — `feat(F3): wire gold subcommand into pipeline __main__` (`src/pipeline/__main__.py` registers the gold command; smoke test that `python -m pipeline --help` lists it).

## Smoke + close

- ⚪ **F3.17** — `chore(F3): smoke run on the 153k fixture` (clear runs, run Bronze→Silver→Gold end-to-end with default budget + concurrency, capture wall-clock + populated-column counts in a STATE.md update; verifies the M2 SLA).
- ⚪ **F3.18** — `docs(F3): gold-flow walkthrough` (analogue of `docs/silver-flow.md`: step-by-step what Gold does, where each column comes from, persona lane semantics, concurrency story, insights schema).
- ⚪ **F3.19** — `chore(F3): ruff/mypy/coverage sweep + roadmap update` (push `gold/**` and `schemas/gold.py` ≥ 90% coverage, ROADMAP M2 row to ✅).
- ⚪ **F3.20** — `docs(F3): archive multi-agent review sign-off` (drop pre-merge reviewer + critic transcripts under `.specs/features/F3/REVIEWS/`).

## Commit map

One commit per atomic task — no bundling unless the diff is < 30 LOC AND
the two tasks share a single test file.

## Pause gates

- After F3.10 (concurrency lane): hold for explicit user sign-off
  before invoking the persona LLM at scale. Cost guard.
- After F3.15 (CLI integration): hold for the smoke run (F3.17). Real
  DashScope cost.
- After F3.17: hold for an M2-close adversarial review (F3.20)
  before declaring M2 done.

## Definition of done (feature-level)

- All four parquet tables + insights JSON ship for the 153k-row
  fixture under the 15-min budget.
- Every persona rule (R1, R2, R3) has positive + negative tests;
  R1's 48-h staleness is locked.
- Coverage ≥ 90% on `src/pipeline/gold/**` and `schemas/gold.py`.
- Ruff clean, mypy strict clean.
- ROADMAP M2 row flips to ✅ with the smoke evidence linked from STATE.

## Carry-forward to F4 / M3

- Incremental Gold rebuild (`gold.leads_to_reprocess` companion
  table) — out of F3 scope per D4. Belongs to F4 alongside the agent
  loop.
- Cost / token-budget metric ($) — PRD §18.4 names a `MAX_USD_PER_BATCH`
  but only the call counter is enforced today. Wait until token-cost
  data is exposed by `LLMClient`.
- F2 prompt v2 (the `PROMPT_VERSION` bump for entity extraction) —
  separate slice once we have annotated data to regress against.
