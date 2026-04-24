# Code Review Report — F3 Gold Module

**Files Reviewed:** 13 (12 in `src/pipeline/gold/` + `src/pipeline/cli/gold.py`)
**Test State:** 616 tests passing at 95.40% coverage, ruff clean, mypy --strict clean.

## Summary

| Severity | Count |
|---|---|
| CRITICAL | 0 |
| HIGH | 1 |
| MEDIUM | 5 |
| LOW | 2 |

**Recommendation:** COMMENT — No critical or high-blocking issues. Code is well-structured, well-documented, and demonstrates strong engineering discipline.

---

## Issues

### [HIGH] Budget charges wasted on rule-hit leads in concurrency lane

**File:** `src/pipeline/gold/concurrency.py:155-156`

**Issue:** `_classify_one_sync` calls `budget.try_charge_provider_call()` **before** checking whether the lead will hit a guard-rail rule. When `classify_with_overrides` returns a rule-hit result (`persona.py:378-382`), no LLM call is ever made, but the budget slot is consumed and never refunded. The only refund path is `client.last_cache_hit` (line 164). With a dataset where, say, 60% of leads hit rules (R2/R1/R3), the effective budget shrinks to 40% of the configured `pipeline_llm_max_calls_per_batch`. The design doc acknowledges this as "acceptable overhead" (design SS9.3), but it means the operator-facing setting lies: configuring 5000 max calls may yield only 2000 actual LLM calls if 60% are rule-hits.

**Fix:** Either (a) pre-filter rule-hit aggregates **before** dispatching to the thread pool (run `evaluate_rules` upstream in `classify_all` and only submit LLM-needing leads to the executor), or (b) add a `refund_rule_hit()` path in `_classify_one_sync` when `result.persona_source == "rule"`. Option (a) also saves thread scheduling overhead.

---

### [MEDIUM] `_coerencia_expr` produces component value 1.5, breaking [0,1] component contract

**File:** `src/pipeline/gold/intent_score.py:93-98`

**Issue:** When persona+outcome match, the expression returns `1.0 + _NULL_COERENCIA_FALLBACK` = 1.5. Every other component produces values in [0.0, 1.0]. While `_weighted_sum` clips the final score to [0, 100], the coerencia component contributes up to `1.5 * 10 = 15` points instead of the declared weight of 10. The import-time guard `sum(WEIGHTS) == 100` passes but the actual max achievable score from this component exceeds its declared weight.

**Fix:** If the design intent is a 3-level scale (0.0 / 0.5 / 1.0), change the match branch from `1.0 + 0.5` to `1.0`. If a bonus is intentional, document the effective range [0, 1.5] and adjust the WEIGHTS docstring.

---

### [MEDIUM] Non-deterministic `generated_at` timestamp in insights payload

**File:** `src/pipeline/gold/transform.py:196`

**Issue:** `datetime.now(tz=UTC).isoformat()` is injected into the insights JSON payload. Design D12 and the determinism flags (`insights.py:46-51`) carefully ensure that re-running Gold on the same Silver produces identical analytical content. But `generated_at` changes on every run, so the output file will never be byte-identical across replays. This undermines the determinism contract for downstream consumers that hash or diff the JSON.

**Fix:** Derive `generated_at` from `batch_latest_timestamp` (already computed at line 144) instead of wall-clock time. Alternatively, move `generated_at` outside the `summary.json` payload into a sidecar metadata file or the manifest row.

---

### [MEDIUM] `_write_parquet_atomic` has a TOCTOU window between `rmtree` and `replace`

**File:** `src/pipeline/gold/writer.py:91-93`

**Issue:** Between `shutil.rmtree(final_dir)` (line 92) and `tmp_dir.replace(final_dir)` (line 93), a concurrent reader that scans `final_dir` sees it missing. Not a crash-safety issue (CLI holds manifest lock), but if any monitoring or downstream process polls the partition directory, it will briefly see a missing partition. The docstring claims "readers see either the previous good file or the new one", technically violated.

**Fix:** Acceptable for single-writer CLI usage. Document the single-writer assumption explicitly in the docstring. No code change needed if the single-writer contract is maintained.

---

### [MEDIUM] `_default_top_surprise` division-by-zero on zero-count matrix

**File:** `src/pipeline/gold/insights.py:309-311`

**Issue:** `overall_close_rate = sum(row["count"] for row in closed_rows) / sum(row["count"] for row in matrix)`. If `matrix` contains rows where all `count` values are 0, this divides by zero. Unreachable in practice given upstream `group_by + agg(pl.len())` guarantees count >= 1, but function signature accepts `list[dict]` with no enforcement.

**Fix:** `total = sum(row["count"] for row in matrix); if total == 0: return None`.

---

### [MEDIUM] CLI imports private `_default_persona_classifier`

**File:** `src/pipeline/cli/gold.py:40-41`

**Issue:** CLI imports underscore-prefixed private function from `transform.py`. Couples CLI to internal implementation detail.

**Fix:** Either (a) rename to `default_persona_classifier` and add to `transform.__all__`, or (b) have CLI pass `persona_classifier=None` and let `transform_gold` build the default internally (it already does at line 153). Option (b) is cleaner.

---

### [LOW] Duplicate `__all__` declaration location in `writer.py`

**File:** `src/pipeline/gold/writer.py:214`

**Issue:** `__all__` at bottom of file, not near imports. Style inconsistency with other Gold modules.

**Fix:** Move `__all__` to top of the module.

---

### [LOW] `build_conversation_scores` called twice for same Silver frame

**File:** `src/pipeline/gold/transform.py:146, 389`

**Issue:** `transform_gold` calls `build_conversation_scores(silver_lf)` at line 146; `_avg_lead_response_per_lead` at line 350 calls it again internally at line 389. Polars `.cache()` caches the scan, not downstream aggregation.

**Fix:** Pass already-computed `conversation_scores_lf` into `_compute_intent_score_inputs`.

---

## Focus Area Verdicts

### 1. Guard-rails in persona rule engine — can LLM output bypass them?

No. `classify_with_overrides` (`persona.py:361-383`) runs `evaluate_rules` first; rule hit returns immediately without LLM. `parse_persona_reply` requires exact enum match (strip+casefold). Invalid replies fall back to `comprador_racional` with source `llm_fallback`. Guard-rails sound.

### 2. Thread safety in persona concurrency lane

`_BudgetCounter` uses `threading.Lock` for all mutations. Each worker gets own `LLMClient` + `LLMCache` + `_CacheHitTrackingClient` via `_init_worker` on `_thread_local` storage. `asyncio.Semaphore` bounds in-flight tasks. No shared mutable state crosses thread boundaries except `_BudgetCounter` (properly locked). Well-implemented.

### 3. Intent score determinism — float ordering, null handling

All components handle nulls: `fill_null(_LATENCY_CAP_SEC)` for latency, `fill_null(0.5)` for null persona/outcome, `fill_null(0.5)` for zero inbound messages. `_weighted_sum` clips to [0, 100] after rounding. One concern: `_coerencia_expr` producing 1.5 (MEDIUM above), but final clip prevents overflow past 100. Float ordering deterministic given identical inputs.

### 4. Insights JSON — PII leakage, determinism

Insights JSON contains only aggregate counts, rates, bucket labels, persona labels. `lead_id` values do NOT appear (verified: all builders count per bucket, never serialize lead_id). `message_body_masked` used only for regex matching, never serialized. No PII leakage. Determinism solid for three Silver-only insights; `persona_outcome_correlation` correctly flagged as non-deterministic. Only gap: `generated_at` (MEDIUM above).

### 5. Idempotence + refusal contracts in CLI

Excellent. CLI (`cli/gold.py:140-159`) checks Silver lineage, already-completed Gold run, clears stale runs, inserts new manifest row within `ManifestDB` context manager. Path traversal guarded. Refusal contract clear and well-tested.

---

## Positive Observations

- Dependency injection of `PersonaClassifier` in `transform_gold` enables clean unit testing.
- Import-time weight validation (`intent_score.py:37-38`) excellent fail-fast guard.
- Atomic write protocol with temp-then-rename correctly implemented for both parquet and JSON.
- Determinism flags in `insights.py` honest and useful.
- Thread-local storage pattern in `concurrency.py` correctly solves SQLite single-thread constraint.
- Batch-anchored staleness (`batch_latest_timestamp` instead of `now()`) ensures deterministic replay.
- Schema assertions before writes catch drift before bytes touch disk.
- Comprehensive test coverage at 95.40% with 616 passing tests, including concurrent budget counter stress tests.
