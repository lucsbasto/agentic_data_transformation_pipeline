# F5 — Sentiment Gold Column — Task Tracker

Source plan: `.omc/plans/autopilot-impl.md`. Architect-validated.

## Group A — Schema foundation

- [x] **F5.1a** `SENTIMENT_VALUES` constant + `__all__` export in `schemas/gold.py`.
- [x] **F5.1b** `sentiment` (Enum) + `sentiment_confidence` (Float64) in `_LEAD_PROFILE_FIELDS`.
- [x] **F5.1c** Schema column-order test updated (`tests/unit/test_schemas_gold.py`).

## Group B — Hard-rule sentiment module

- [x] **F5.2a** `gold/sentiment.py` — `SentimentResult` + `evaluate_sentiment_rules`.
- [x] **F5.2b** SR1/SR2/SR3 predicates + priority order.
- [x] **F5.8a** Hard-rule unit tests T-S1..T-S4 + priority test (`test_gold_sentiment.py`).

## Group C — Combined LLM prompt

- [x] **F5.3** `PersonaResult` extended w/ `sentiment` / `sentiment_confidence` / `sentiment_source` (kw_only defaults preserve back-compat).
- [x] **F5.4a** `SYSTEM_PROMPT` modified to combined JSON output + label glossary.
- [x] **F5.4b** `PROMPT_VERSION_PERSONA` v2 → v3 (cache flush).
- [x] **F5.5a** `parse_classifier_reply` — markdown-fence-tolerant JSON parser.
- [x] **F5.5b** `parse_persona_reply` kept as raw-label parser for back-compat.
- [x] **F5.8b** Parser unit tests T-S5/T-S6/T-S6b/T-S6c/T-S7.

## Group D — Wiring

- [x] **F5.6** `_add_persona_and_score_placeholders` extended w/ sentiment columns.
- [x] **F5.7a** `_apply_persona_and_intent_score` fills sentiment columns from classifier output.
- [x] **F5.7b** `.select([...])` list at `transform.py:335-355` extended w/ sentiment columns (per Critic finding — silent column drop otherwise).

## Group E — Integration + observability

- [x] **F5.9a** Combined-prompt happy-path test (existing `test_llm_path_parses_valid_reply`).
- [x] **F5.9b** Combined-prompt error path → skipped sentinel.
- [x] **F5.11a** E2E test `test_sentiment_columns_populated_end_to_end`.
- [x] **F5.11b** ~~Golden parquet fixture~~ — superseded by inline e2e test (Architect approved deviation).
- [x] **F5.13** `gold.sentiment.batch_complete` structlog event w/ source mix + label histogram.

## Group F — Backfill / docs

- [x] **F5.12a** `docs/F5_sentiment_backfill.md`.
- [x] **F5.12b** `.specs/features/F5/{DESIGN,tasks}.md`.

## Acceptance gates

- [x] `pytest tests/unit/ -k gold` — 131 tests green.
- [x] `ruff check src/pipeline/gold/ src/pipeline/schemas/` — clean.
- [x] `mypy src/pipeline/gold/ src/pipeline/schemas/` — clean.
- [x] `pytest tests/integration/ -k gold` — 9/9 green.
- [ ] **Calibration check** (post first real batch) — `misto` ≤ 25% of LLM-classified leads.

## Validation lane

- [x] Architect — APPROVE. AC 1-11 met; T-S13 golden fixture deferred (E2E covers it).
- [ ] Code-reviewer — pending.
- [ ] Security-reviewer — pending.

## Open follow-ups

- F5.1 (per-conversation sentiment in `conversation_scores`) — wait for analyst demand.
- A/B harness comparing prompt v2 vs v3 persona accuracy — only if calibration gate trips.
