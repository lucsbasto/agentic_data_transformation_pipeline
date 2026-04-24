# Napkin Runbook

Per-repo curated runbook. Re-prioritize every read, merge duplicates, cap 10 per category.

## Curation Rules
- Re-prioritize on every read.
- Keep recurring, high-value notes only.
- Max 10 items per category.
- Each item: date + "Do instead".

## Execution & Validation (Highest Priority)

1. **[2026-04-23] Bare `python` / `pytest` / `ruff` / `mypy` not on PATH.**
   Do instead: invoke via `.venv/bin/pytest`, `.venv/bin/ruff`, `.venv/bin/mypy`. Same for any CLI declared in `pyproject.toml [project.optional-dependencies].dev`.

2. **[2026-04-23] Project coverage gate is 80% global; single-file pytest runs fail it.**
   Do instead: for focused TDD iteration use `.venv/bin/pytest path/to/test --no-cov`. Run full suite (`.venv/bin/pytest tests/ --no-cov`) before declaring done.

3. **[2026-04-23] Verify pass order before claiming done: pytest → ruff check → ruff format --check → mypy.**
   Do instead: batch all four via `ctx_batch_execute` in one round-trip. Do not skip `ruff format --check` — repo enforces it.

4. **[2026-04-23] Spec/design docs may drift from actual module names (e.g. design §9 called `_classify_with_overrides`, real code exports `classify_with_overrides` + requires extra kw args).**
   Do instead: read the actual imported module before writing the integrating slice. Flag deviations in summary, extend signatures as needed, do not blindly copy design.

5. **[2026-04-23] Ruff rejects unused imports (F401) even in tests and auto-format reshapes tests.**
   Do instead: drop unused imports while writing the file; run `.venv/bin/ruff format` after every new file.

## Shell & Command Reliability

1. **[2026-04-23] PreToolUse hook pushes large-output commands to `ctx_batch_execute`.**
   Do instead: for any command that may exceed ~20 lines (cat, find, grep -r, full test runs), use `ctx_batch_execute` with a labeled section. Bash reserved for git/mkdir/rm/mv and short commands.

2. **[2026-04-23] `ls` inside paths with spaces needs literal double quotes.**
   Do instead: quote the entire path; prefer absolute paths from the working directory instead of `cd`.

3. **[2026-04-23] `grep -r … | xargs …` noisy on big trees; prefer `ctx_batch_execute` + indexed search.**
   Do instead: batch discovery commands with descriptive labels, then use `ctx_search` with targeted queries.

## Domain Behavior Guardrails

1. **[2026-04-23] `LLMClient` not thread-safe; `LLMCache` SQLite connection refuses cross-thread access.**
   Do instead: give every worker thread its own `LLMClient` + `LLMCache` via `ThreadPoolExecutor(initializer=_init_worker, ...)` storing on `threading.local`. Do not share a single client across a pool.

2. **[2026-04-23] `pipeline_llm_max_calls_per_batch` (PRD §18.4) caps cache *misses*, not total evaluations.**
   Do instead: when spending the budget, charge optimistically before the provider call and refund on cache hit. Surface `calls_made` + `cache_hits` counters for observability.

3. **[2026-04-23] `PersonaResult` has no `cache_hit` field despite design §9.4 referencing one.**
   Do instead: capture cache hits at the `LLMResponse.cache_hit` boundary via a thin client wrapper with `last_cache_hit` flag; do not add fields to the F3.9 dataclass.

4. **[2026-04-23] `classify_with_overrides` requires `batch_latest_timestamp` (D12 staleness anchor).**
   Do instead: plumb `batch_latest_timestamp` through any orchestrator calling into persona. Use the max of the Silver batch, never `datetime.now()`.

5. **[2026-04-23] Rule hits inside `classify_with_overrides` short-circuit the LLM but still consume a budget slot in the F3.10 lane (design §9.3 choice).**
   Do instead: pre-filter rule-eligible aggregates upstream if budget capacity matters; otherwise accept the overhead and document it.

6. **[2026-04-23] `PROMPT_VERSION_PERSONA = 'v1'` is embedded in `SYSTEM_PROMPT` so cache keys bust on edits.**
   Do instead: bump the version string whenever `SYSTEM_PROMPT` changes. Never call `LLMCache.invalidate` by hand.

7. **[2026-04-23] `pipeline_llm_concurrency` default 16, bounded [1, 64] — load-bearing for F3-RNF-06 (15-min SLA).**
   Do instead: never set to 1 in production (breaks SLA); `asyncio.Semaphore(settings.pipeline_llm_concurrency)` is the sole in-flight knob.

8. **[2026-04-23] Polars lazy-first contract (F3-RNF-01): scan_parquet → lazy group-bys → collect(streaming=True).**
   Do instead: stay lazy as long as possible; never call `.collect()` mid-pipeline just to inspect. No pandas anywhere.

9. **[2026-04-23] Tests must use fake LLM client, never live DashScope (F3-RNF-02).**
   Do instead: mirror `_FakeLLMClient` from `tests/unit/test_gold_persona_llm.py` — duck-type `cached_call(**kwargs) -> LLMResponse`. For the concurrency lane, monkeypatch `LLMClient` + `LLMCache` at `pipeline.gold.concurrency` module level.

10. **[2026-04-23] LLMCache uses SQLite WAL + `busy_timeout=5000` (F1.3) — safe for multi-conn writes.**
    Do instead: per-worker connections against the same file is fine; no manual locking needed.

## User Directives

1. **[2026-04-23] Caveman mode active by default (`full` level) via SessionStart hook.**
   Do instead: respond terse — drop articles/filler/pleasantries/hedging, fragments OK. Code/commits/PRs/security: normal prose. Revert only on `stop caveman` / `normal mode`.

2. **[2026-04-23] User pedagogic mode: wants "why" behind every non-trivial choice.**
   Do instead: when deviating from spec, list deviations explicitly in the summary. Keep LEARN comments where they already exist; add them for non-obvious stdlib/library choices.

3. **[2026-04-23] Atomic F3.x commits: one slice per commit with `feat(F3): <subject>` style.**
   Do instead: never bundle multiple F3.x tasks in one commit. Mirror prior subjects (see `git log --oneline`).

4. **[2026-04-23] Retry 3x on blockers before reporting; do not pause mid-task to ask permission.**
   Do instead: keep iterating until goal reached or hard blocker hit. Report blockers with evidence (exact error, tried alternatives), not speculation.

5. **[2026-04-23] Tests pattern: `clean_env` fixture + `_set_required_env(monkeypatch)` helper lives in `tests/unit/test_settings.py`.**
   Do instead: duplicate locally in new test files (imports are cheap) rather than extending the shared conftest. Match the pattern exactly so future readers grep across files.
