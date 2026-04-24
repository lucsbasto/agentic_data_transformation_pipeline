# Critic Review — F3 Gold Module

**VERDICT: ACCEPT-WITH-RESERVATIONS**

## Pre-commitment Predictions

Problem areas predicted before reading the code:

1. **Intent score formula range violation** — components claiming [0,1] but leaking outside. **CONFIRMED (coerencia emits 1.5).**
2. **price_sensitivity left as null** — placeholder never filled. **CONFIRMED.**
3. **Concurrency race conditions** — budget counter or thread-local issues. **Not found; design is sound.**
4. **Insights being just aggregates, not "non-obvious"** — **Partially confirmed.**
5. **Edge cases with all-null data** — **Mostly handled via fill_null guards.**

---

## Critical Findings

### C1. `_coerencia_expr()` emits 1.5, violating the [0,1] component contract

- **Confidence:** HIGH
- **Evidence:** `intent_score.py:97` — `.then(pl.lit(1.0) + pl.lit(_NULL_COERENCIA_FALLBACK))` produces 1.5 when persona-outcome match. The doc (`gold-flow.md` section 9a) says components are in [0,1]. The raw max score is **105**, not 100 (all other components at 1.0 contribute 90 points + coerencia at 1.5 contributes 15 = 105). The `clip(0, 100)` masks this.
- **Why this matters:**
  - The formula is no longer a clean weighted average. A persona-outcome match contributes **15 points** not 10, making it the 2nd-heaviest signal despite its declared weight of 10. The real effective weight of `coerencia` is 15/105 = 14.3%, not 10%.
  - The property test (`test_gold_intent_score.py`, section 4) tests `intent_score_from_component_columns` with values `{0, 0.5, 1.0}` — it never sees the 1.5 production value from `_coerencia_expr()`. Test coverage creates false sense of safety.
  - The `test_match_gives_full_weight` test passes because it uses deltas from a baseline of 5 (null fallback), hiding that the component exceeds 1.0.
- **Fix:** Either:
  - (A) change `_coerencia_expr` match branch to `pl.lit(1.0)` and null fallback to `pl.lit(0.5)` and contradiction to `pl.lit(0.0)` — a true [0,1] range.
  - (B) document that this component intentionally exceeds 1.0 and adjust the WEIGHTS to `{coerencia: 10, ...}` with understanding that 105 is the raw max.
  - Option A is cleaner.

---

## Major Findings

### M1. `price_sensitivity` column is permanently null — never computed

- **Confidence:** HIGH
- **Evidence:** `lead_profile.py:210` sets `price_sensitivity` as `pl.lit(None, dtype=price_dtype)`. The `transform.py:307` `select()` list includes `"price_sensitivity"` but never fills it. `HAGGLING_PHRASES` defined in `_regex.py:62` but never imported in `transform.py`. Grep shows zero consumers of `HAGGLING_PHRASES`.
- **Why this matters:** Schema declares `pl.Enum(["low", "medium", "high"])` column (F3-RF-16), but every row ships null. Downstream analytics consuming `lead_profile` see a dead column. `HAGGLING_PHRASES` catalog built but never wired.
- **Fix:** Add `_compute_price_sensitivity()` function in `transform.py` that counts haggling phrase hits per lead and maps counts to low/medium/high buckets. Wire it in `_apply_persona_and_intent_score`.

### M2. `build_conversation_scores` called twice — redundant Silver scan

- **Confidence:** HIGH
- **Evidence:** `transform.py:146` calls `build_conversation_scores(silver_lf)` for main output. `transform.py:389` in `_avg_lead_response_per_lead` calls it again internally. Polars `.cache()` caches scan, not downstream transforms.
- **Why this matters:** At 153k rows, pair-delta walk + group_by runs twice unnecessarily. Wasteful and violates "minimum code that solves the problem."
- **Fix:** Pass already-computed `conversation_scores_lf` into `_compute_intent_score_inputs`.

### M3. Insights `objections` and `ghosting_taxonomy` are plain aggregates, not "non-obvious"

- **Confidence:** MEDIUM
- **Evidence:** `insights.py:106-138` — `build_ghosting_taxonomy` is message-count bucket distribution. `build_objections` (lines 146-186) is regex match count with close rate. M2 criterion demands insights that are "non-obvious." A count of "leads with 1-2 messages who ghosted" or "leads who said 'ta caro'" is a basic aggregate any SQL query would produce.
- **Why this matters:** If M2 acceptance genuinely requires "non-obvious" insights, two of four fail this bar. `disengagement_moment` (two-axis breakdown) and `persona_outcome_correlation` (deviation-based surprise ranking) are more defensible.
- **Fix:** Either lower M2 claim to "structured analytics" or add a non-obvious dimension (ghosting taxonomy could cross with engagement_profile to show which hot leads ghost vs cold leads).

---

## Minor Findings

1. **7 `type: ignore` in `persona.py:441-448`** — All in `_row_to_aggregate` for Polars row dict typing. Acceptable given `iter_rows(named=True)` returns `dict[str, Any]`. Low debt.
2. **Zero TODOs, zero FIXMEs, zero HACKs** in all gold source. Clean.
3. **Single `pragma: no cover`** at `intent_score.py:37` for import-time weight sum guard. Justified — static invariant check.
4. **3 `noqa` suppressions** in `transform.py` — two for local imports (`PLC0415`), one for Polars' `== False` pattern (`E712`). All justified.
5. **`_DEFAULT_BATCH_ANCHOR` (Unix epoch)** in `transform.py:90` used for empty Silver frames. Harmless but undocumented as invariant.
6. **`_top_surprise` deviation is absolute but signed** — `insights.py:312` uses `abs()` for ranking but returns signed `deviation` in output. Consumers need to know negative deviation means below-average close rate. Minor documentation gap.

---

## What's Missing

- **price_sensitivity computation** — column exists in schema, phrases exist in `_regex.py`, but no wiring. (M1.)
- **No test for `_coerencia_expr()` returning 1.5** — property test bypasses production expression.
- **No test for `_default_persona_classifier` production path** — only tested via integration test with stubs. `asyncio.run` bridge inside closure untested in isolation.
- **No rollback documentation** — if Gold writes 3 of 4 tables then crashes on 4th, no recovery procedure documented. Atomic writer handles per-table atomicity but 4-table batch not transactional.
- **No monitoring/alerting hooks** — structured logging thorough, but no metric emissions (counters, histograms) for ops dashboards.
- **`conversation_text` truncation is character-based, not token-based** — `persona.py:300` slices at `_MAX_PROMPT_CHARS=2000` characters. LLM token limits are token-based. Portuguese averages ~1.3 tokens/char, so 2000 chars could be ~2600 tokens.

---

## Multi-Perspective Notes

**Executor perspective:** Orchestrator (`transform.py`) well-structured for extension. DI for persona classifier enables clean testing. Column-pinning via `select(list(SCHEMA.names()))` at every builder exit is strong pattern.

**Stakeholder perspective:** Intent score formula's effective weights are not what the weights dict advertises. Stakeholder reading `WEIGHTS = {coerencia: 10}` would believe persona-outcome coherence is 10% factor. In practice it is 14.3% for matching leads and 4.8% (5/105) for null-persona leads. Communication gap between analytical spec and actual model behavior.

**Skeptic perspective:** Concurrency lane (thread pool + semaphore + budget counter with optimistic charge/refund) well-designed for purpose. At current scale (14938 leads, 6s gold runtime) LLM calls are bottleneck, not framework. ThreadPoolExecutor + per-thread SQLite cache is right call given synchronous Anthropic SDK. No over-engineering.

**Security perspective:** `_validate_batch_id` in `cli/gold.py:116-126` guards against path traversal. `_safe_resolve` rejects `..` segments. Adequate for CLI tool.

---

## Verdict Justification

One critical finding (C1) identified. `clip(0, 100)` masks overflow in all production outputs, and test suite exercises actual end-to-end scores correctly. Effective weight distortion is real but does not produce incorrect outputs — produces *differently weighted* outputs than documented. Documentation/design-intent issue, not data corruption.

Three reservations block clean ACCEPT:
1. Coerencia component range violation.
2. price_sensitivity dead column.
3. Insights quality claim.

Items (1) and (2) are concrete code issues; (3) is product-level question.

**What would upgrade to ACCEPT:** Fix C1 (normalize coerencia to [0,1] or document the intentional deviation), wire price_sensitivity computation, add test that directly asserts `_coerencia_expr()` output range.

---

## Open Questions

- Is the `1.5` coerencia value intentional "boost" for persona-outcome alignment, or bug from delta-from-baseline test methodology?
- Should `build_conversation_scores` be shared between main output and intent-score input path, or is redundant computation acceptable for code clarity?
- At what scale does thread pool concurrency lane become necessary vs sequential calls? Current 14938 leads with cache hits likely run fast enough sequentially.
