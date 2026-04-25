# GAPS — What is missing to close PRD

**Date:** 2026-04-25
**Baseline:** M1 (F1 Bronze + LLMClient) and M2 (F2 Silver + F3 Gold + CI) shipped. 621 tests, 95.4%+ coverage, ruff + mypy strict clean. F1/F2/F3 review lanes archived. F3 review-lane MED backlog (symlink escape, deterministic insights timestamp, prompt-injection delimiters, dedup `build_conversation_scores`) closed in 5924934..4cbbe38.

## Why is it not completed yet?

The project was executed in sequential feature milestones with an intentional pedagogic pause between each: the user validates ROADMAP, approves each feature design, decides trade-offs, then the next feature starts. The PRD (`/PRD.md`, 408 lines) is complete as a specification document — 18 sections covering functional/non-functional requirements, acceptance criteria, risks, deliverables, persona taxonomy, LLM prompts, and ADR stubs. What is missing is **implementation scope** for milestones M3 (autonomy) and M4 (quality + differentials), plus housekeeping surfaced by the M2 review lane (security hardening, determinism polish, SLA cold-run breach).

Concretely:
- **F4 and F5 were never started.** ROADMAP declared them `⚪ blocked` behind F3 and the user has not authorized the next milestone.
- **Demo, README, license, and Databricks runner** are milestone M4 items scheduled after F4.
- **F3 follow-ups** (seven findings from the cross-reviewer consensus) were deferred from the M2 ship to avoid blocking F4 indefinitely; three of them (C1 coerencia, M1 price_sensitivity, HIGH budget waste) were fixed in the M2 closure commits, the remaining four are carried here.
- **Cold-run SLA breach** (~35min vs. RNF-06 <15min target) surfaced in the fresh smoke run; cached/incremental runs meet the target at ~2min.

## Missing work, grouped by scope

### 1. M3 — F4 Agent core (observe → diagnose → act → verify)

The entire agent loop is unimplemented. This is the largest remaining block.

| Task | Estimate |
|---|---|
| F4 spec + design + tasks (retry budget, escalation policy, log schema, failure taxonomy) | 1–2h |
| Failure detection: structured error catch around each CLI subcommand, classify by `errors.py` type | 3–4h |
| Diagnosis + action: LLM-driven fix proposal bounded by retry budget (PRD §18.3 prompt shape) | 4–6h |
| Retry/escalation state machine — charge budget per retry, escalate to human via structured log when exhausted | 2–3h |
| Fault-injection hooks so the demo can reproduce "schema drift", "regex broken", "missing partition" without manual sabotage | 1–2h |
| Unit + integration tests (fake LLM, deterministic failure scenarios) | 3–4h |
| **F4 subtotal** | **14–21h** |

### 2. M3 — F5 CLI + observability

PRD §10 demands `python -m pipeline init`, `run`, and `watch`. Today only `ingest`, `silver`, `gold` exist as discrete subcommands.

| Task | Estimate |
|---|---|
| `pipeline init` — create bronze/silver/gold/state directories, seed `.env` from `.env.example`, write stamped `run_manifest.json` skeleton | 1h |
| `pipeline run` — orchestrate ingest → silver → gold in one invocation, respecting manifest lineage and stopping on any stage failure | 2h |
| `pipeline watch` — poll Bronze source mtime/rowcount, trigger incremental silver+gold when delta detected, honor sleep interval from settings | 3–4h |
| `run_manifest.json` emission per execution (RNF-04) — consolidated across stages, not just manifest DB rows | 1–2h |
| Metric counters surfaced in logs (messages processed, LLM calls, cache hits, retries, escalations) | 1–2h |
| Tests (watch loop with fake filesystem, incremental delta test) | 2h |
| **F5 subtotal** | **10–13h** |

### 3. M4 — Differentials + housekeeping

| Task | Estimate |
|---|---|
| README.md — clone → run walkthrough, architecture overview, troubleshooting, <10min target | 2h |
| LICENSE — pick and commit (MIT/Apache-2.0 depending on user intent) | 5min |
| `docs/agent-design.md` — decisions on auto-correction, personas, insights per PRD §12.6 | 2h |
| Demo script — inject a synthetic fault, capture agent log, verify recovery, reproducible via `make demo` or equivalent | 2h |
| F6 Databricks runner (bonus) — parameterize paths/state for workspace, notebook wrapper, README section | 6h |
| **M4 subtotal** | **~12h** |

### 4. RNF-06 SLA cold-run breach

Cold run took ~35min (vs. <15min). Silver LLM extraction dominates (5000 calls, zero cache hits on fresh state).

| Task | Estimate |
|---|---|
| Profile Silver LLM extract — confirm extraction is the bottleneck and not IO | 30min |
| Tune `pipeline_llm_concurrency` and `pipeline_llm_max_calls_per_batch` for the target hardware; re-benchmark | 1h |
| If tuning insufficient: introduce per-lead sampling (PRD §11 risk mitigation already allows this) | 3–5h |
| Document the actual cold-run and warm-run numbers in the README | 15min |
| **SLA subtotal** | **5–7h** |

### 5. F3 follow-up backlog (from the M2 review lane)

Four MED findings closed on 2026-04-25 (commits 5924934, 347bb19, ba4c0b7, 4cbbe38). Three LOW findings still open.

| Task | Severity | Status | Estimate |
|---|---|---|---|
| Symlink escape in `_safe_resolve` — abspath/resolve mismatch check (`cli/gold.py:110-123`) | MED | DONE 347bb19 | — |
| Non-deterministic `generated_at` in insights — anchored to `batch_latest_timestamp` (`transform.py:200`) | MED | DONE 5924934 | — |
| Prompt-injection delimiters + system-prompt guard (`persona.py:336-358`, PROMPT_VERSION_PERSONA v2) | MED | DONE ba4c0b7 | — |
| `build_conversation_scores` deduped — threaded through `_compute_intent_score_inputs` (`transform.py:165,406`) | MED | DONE 4cbbe38 | — |
| Explicit file permissions on parquet/JSON writes (umask 0o077 or explicit 0o600) | LOW | open | 20min |
| LLM cache TTL sweep (e.g. `DELETE WHERE created_at < now - 30d`) | LOW | open | 45min |
| `batch_id` length + ASCII validation | LOW | open | 20min |
| **Open backlog subtotal** | | | **~1h25min** |

### 6. Critic tech debt (lower priority)

| Task | Estimate |
|---|---|
| Direct unit test on `_coerencia_expr` output range (not via property test) | 15min |
| Rollback documentation for partial 4-table Gold writes | 30min |
| Token-based truncation for `conversation_text` (swap char budget for tokenizer) | 1h |
| **Debt subtotal** | **~1h45min** |

## Grand total

| Bucket | Estimate |
|---|---|
| F4 Agent core | 14–21h |
| F5 CLI + observability | 10–13h |
| M4 differentials (README, license, design doc, demo, Databricks) | ~12h |
| RNF-06 SLA fix | 5–7h |
| F3 follow-up backlog (LOW only) | ~1h25min |
| Critic tech debt | ~1h45min |
| **Total** | **~44–56h** |

## Suggested order of execution

1. ~~**F3 follow-up MEDIUM findings**~~ — DONE 2026-04-25 (4 commits 5924934..4cbbe38).
2. **F4 spec + design + tasks review** (~1–2h) — pedagogic pause: user validates the scaffolded retry budget, escalation policy, and log schema (already drafted in commit b6a3534).
3. **F4 implementation + tests** (~14–19h).
4. **F5 CLI + observability** (~10–13h) — unblocks `watch`, demo, and the <10min clone-to-run metric.
5. **RNF-06 SLA tuning** (~5–7h) — needs F5 in place to benchmark the whole `run` command.
6. **M4 README + license + design doc + demo** (~6h) — the final closeout, covers PRD §10 + §12.
7. **Critic tech debt + LOW findings** (~3h) — optional polish once the pipeline is demo-ready.
8. **F6 Databricks bonus** (~6h) — diferencial, only if schedule allows.

## Calibration notes

Estimates assume the current pair-programming cadence (Claude Code + user review), atomic conventional commits, and the existing review-lane pattern (three-agent code+security+critic pass per feature). Actual time depends on:

- How many pedagogic pauses the user wants between F4 sub-steps.
- Whether F6 Databricks is skipped or attempted.
- Whether the SLA breach requires sampling (expensive) or just concurrency tuning (cheap).
- External variance: LLM provider latency, DashScope availability, CI runner cold-starts.
