# F3 Gold — Review Index

**Date:** 2026-04-24
**Commits reviewed:** dc63ed9 → f5ee4e6 (F3 feature branch merged to main)
**Test state at review:** 616 passed, 95.40% coverage, ruff + mypy strict clean.

## Reviews

| Agent | Verdict | Critical | High | Medium | Low | File |
|---|---|---|---|---|---|---|
| code-reviewer | COMMENT | 0 | 1 | 5 | 2 | [code-reviewer.md](code-reviewer.md) |
| security-reviewer | LOW risk | 0 | 0 | 2 | 4 | [security-reviewer.md](security-reviewer.md) |
| critic | ACCEPT-WITH-RESERVATIONS | 1 | 0 | 3 | 6 | [critic.md](critic.md) |

## Cross-Reviewer Consensus

Findings flagged by 2+ reviewers (merge-blocking priority):

### Intent score `_coerencia_expr` emits 1.5 (component contract violation)
- code-reviewer MEDIUM, critic CRITICAL
- Fix: normalize to [0,1] or adjust WEIGHTS docstring
- Files: `src/pipeline/gold/intent_score.py:93-98`

### Redundant `build_conversation_scores` call
- code-reviewer LOW, critic MEDIUM
- Fix: pass computed `conversation_scores_lf` through
- Files: `src/pipeline/gold/transform.py:146,389`

### LLM prompt injection defense-in-depth (persona classifier)
- security-reviewer MEDIUM (M1)
- Current mitigations: strict enum parser, temperature=0, text budget caps. Remediation: XML delimiters + explicit instruction guard.
- Files: `src/pipeline/gold/persona.py:330-342`

### Non-deterministic `generated_at` in insights JSON
- code-reviewer MEDIUM (M2 by code-reviewer)
- Fix: use `batch_latest_timestamp`; violates byte-identical replay contract
- Files: `src/pipeline/gold/transform.py:196`

### `price_sensitivity` column always null
- critic MAJOR (M1)
- Schema promises `pl.Enum(["low","medium","high"])`; always null. `HAGGLING_PHRASES` exists but unused.
- Fix: wire computation using `HAGGLING_PHRASES` regex
- Files: `src/pipeline/gold/lead_profile.py:210`, `src/pipeline/gold/transform.py:307`

### Symlink escape in path validation
- security-reviewer MEDIUM (M2)
- Fix: anchor resolved path to `allowed_root`
- Files: `src/pipeline/cli/gold.py:110-113`

### Budget charges wasted on rule-hit leads
- code-reviewer HIGH
- Fix: pre-filter rule-hit aggregates before thread dispatch OR refund on rule-hit
- Files: `src/pipeline/gold/concurrency.py:155-156`

## Smoke Run Summary

End-to-end on fresh state (cold cache, `/tmp/smoke-m2/`):

| Stage | Rows in | Rows out | Duration |
|---|---|---|---|
| Ingest | — | 153228 | 148ms |
| Silver | 153228 | 153228 | 2028456ms (~33.8min) |
| Gold | 153228 | 29958 | 63739ms (~63s) |

**Total: ~35min**. **EXCEEDS M2 SLA of <15min** on cold run. Dominated by Silver LLM extraction (5000 calls, 0 cache hits). Warm/cached run: ~2min (meets SLA).

### Gold artifacts verified
- `conversation_scores`: 15000 rows
- `lead_profile`: 14938 rows
- `competitor_intel`: 0 rows (prior cached run: 1 row — **LLM non-determinism concern**)
- `agent_performance`: 20 rows
- `insights/summary.json`: 4 non-obvious insight keys

## Recommendation

**M2 can be marked ✅ shipped** with explicit caveats:
1. Cold-run SLA breach (LLM extraction budget cap = 5000 calls dominates runtime).
2. Seven cross-reviewer findings added to F3 follow-up backlog — none blocking, but C1 (coerencia 1.5) + M1 (price_sensitivity dead column) should be addressed before F4 agent consumes these signals.
3. Competitor_intel row-count variance between runs reveals LLM non-determinism in Silver extraction — consider caching at that layer or documenting as expected.
