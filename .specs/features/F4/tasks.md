# F4 tasks

> Decomposição atômica do design F4. Cada task = 1 commit `conventional-commit`. Tests verdes obrigatórios antes de fechar a task.
> Ordem é dependência-respeitada; tasks paralelas marcadas como tal.
> Status: ⚪ pending · 🟡 in-progress · ✅ done.

## Backbone (state, lock, types)

- ✅ **F4.0** — `docs(F4): scaffold spec/design/tasks and bump roadmap` (this commit; vira F4 para 🟡 spec'd no `ROADMAP.md`).
- ✅ **F4.1** — `feat(F4): declare agent enums + dataclasses` (commit 6e05439). `src/pipeline/agent/types.py` com `ErrorKind`, `Layer`, `RunStatus`, `Fix`, `FailureRecord`, `AgentResult`; 11 tests em `tests/unit/test_agent_types.py`.
- ✅ **F4.2** — `feat(F4): manifest migrations for agent_runs + agent_failures`. DDL + 6 métodos `ManifestDB` (`start_agent_run`, `end_agent_run`, `record_agent_failure`, `record_agent_fix`, `mark_agent_failure_escalated`, `count_agent_attempts`); 28 tests em `tests/unit/test_agent_state.py` (CRUD, idempotência, FK cascade, counters scoped por triple).
- ✅ **F4.3** — `feat(F4): filesystem lock with PID + stale detection` (`src/pipeline/agent/lock.py` + `AgentBusyError` em `pipeline.errors`). 16 tests em `tests/unit/test_agent_lock.py` cobrindo acquire fresh/reentrant/corrupt/empty, AgentBusyError on live peer, stale takeover (PID morto + mtime > stale_after_s), release com PID-match guard, context manager (release on exception), defaults. Signal handler integration ainda em F4.14 (loop).

## Diagnoser + fixes

- ✅ **F4.4** — `feat(F4): diagnoser deterministic patterns + LLM fallback`. `src/pipeline/agent/diagnoser.py` com stage 1 (polars.SchemaError / SchemaFieldNotFoundError / ColumnNotFoundError → SCHEMA_DRIFT, SilverRegexMissError → REGEX_BREAK, SilverOutOfRangeError → OUT_OF_RANGE, FileNotFoundError → PARTITION_MISSING) + stage 2 LLM fallback com `_DiagnoseBudget` cap default 10 + `DIAGNOSE_SYSTEM_PROMPT` v1 com defesa contra prompt injection. Novas exceções `SilverRegexMissError` / `SilverOutOfRangeError` em `pipeline.errors`. 21 tests em `tests/unit/test_agent_diagnoser.py` (deterministic hits, LLM fallback com fake client, JSON malformado / shape inválida / kind hallucinated → UNKNOWN, budget exhausted short-circuit).
- ✅ **F4.5** — `feat(F4): schema_drift fix`. `src/pipeline/agent/fixes/schema_drift.py` com `detect_delta`/`format_delta_message` (≤512 chars) + `repair_bronze_partition` (drop extras, fill missing nulls, cast strict=False, atomic temp-then-rename, byte-stable reorder) + `build_fix(parquet_path)` factory. 15 tests em `tests/unit/test_fix_schema_drift.py` (delta detection extra/missing/type, msg cap, repair drop/fill/reorder, idempotência byte-identical, no .tmp leftover, missing partition raises `SchemaDriftFixError`).
- ✅ **F4.6** — `feat(F4): regex_break fix with override persistence`. `src/pipeline/agent/fixes/regex_break.py` com `regenerate_regex` (LLM JSON reply, compile-check), `validate_regex` (baseline match), `save_override`/`load_overrides` (atomic temp-then-rename JSON em `state/regex_overrides.json`), `build_fix(...)` factory. `src/pipeline/silver/regex.py` com `load_override(batch_id, pattern_name)` read-only — F2 hookup nos PII regexes existentes ainda pendente (tracked como tech-debt). 28 tests (`test_fix_regex_break.py` + `test_silver_regex_overrides.py`) cobrindo regenerate happy/malformed/uncompilable, validation true/false, override merge/overwrite, baseline regression rejection sem persistir, malformed JSON / non-string collapse → None.
- ✅ **F4.7** — `feat(F4): partition_missing fix`. `src/pipeline/agent/fixes/partition_missing.py` reusa F1 ingest pipeline (scan_source → transform_to_bronze → collect_bronze → write_bronze) para re-emitir Bronze partition ausente. Idempotente (no-op se path existe), refuse on source/batch_id mismatch via `compute_batch_identity` check. 6 tests cobrindo recreate happy/no-op/mismatch/canonical layout/build_fix.
- ✅ **F4.8** — `feat(F4): out_of_range fix acknowledges quarantine`. `src/pipeline/agent/fixes/out_of_range.py` reusa `silver/quarantine` partition layout: `quarantine_partition_path`, `quarantine_row_count`, `acknowledge_quarantine` (raises `OutOfRangeFixError` se sem evidência), `build_fix(silver_root, batch_id)`. 9 tests cobrindo path canonical, count missing/empty/positive, ack happy/raises, build_fix happy/raises. Nota: o flag `had_quarantine` em `runs` (design §8.4) requer DDL change na tabela F1 e fica como follow-up; o ack atual é registrado em `agent_failures.last_fix_kind="acknowledge_quarantine"` via `record_agent_fix`.

## Observer + planner + executor

- ✅ **F4.9** — `feat(F4): observer scans pending batches`. `src/pipeline/agent/observer.py` enumera `source_root/*.parquet`, computa identity, consulta `batches` table; pending iff missing OR FAILED OR stale IN_PROGRESS (default 1h, configurável). 19 tests (`test_agent_observer.py`) cobrindo discover empty/sorted, is_pending missing/FAILED/COMPLETED/fresh-IP/stale-IP/unparseable-ts/naive-ts, scan end-to-end com sorted output determinístico.
- ✅ **F4.10** — `feat(F4): planner emits sequential bronze→silver→gold plan`. `src/pipeline/agent/planner.py` com `LAYER_ORDER` pinned, `is_layer_completed` (consulta `get_latest_run`), `plan(batch_id, *, manifest, runners)` retorna subset em ordem canônica. Pure (no side effects). 12 tests cobrindo is_layer_completed (no-runs/COMPLETED/FAILED/isolated), plan (fresh/sorted/skip-COMPLETED/all-done/retry-FAILED/runner-identity/missing-runner-noop/order-pinned).
- ✅ **F4.11** — `feat(F4): executor with retry budget per (batch_id, layer, error_class)`. `src/pipeline/agent/executor.py` com `Executor` class injetando classifier/fix_builder/escalator + `Outcome` StrEnum + `RecoveryResult` dataclass. Loop fielmente espelha design §6: invoke → classify → record_failure → UNKNOWN/no-fix → immediate escalate; com fix → apply (sucesso = record_fix + retry; falha = log + retry); budget esgotado = escalate. 8 tests cobrindo happy path, fix recovery, budget exhaustion, latest-row escalated marking, UNKNOWN immediate escalate, no-fix escalate, default budget pinned (3).

## Escalator + logging

- ✅ **F4.12** — `feat(F4): escalator emits structured JSON alert + suggested fix table`. `src/pipeline/agent/escalator.py` com `SUGGESTED_FIX` table (5 entradas, uma por ErrorKind), `build_payload` (event/batch_id/layer/error_class/last_error_msg≤512/suggested_fix/ts), `write_event` (atomic append JSONL com mkdir parent), `escalate` end-to-end + opcional flip da latest run para FAILED, `make_escalator` curried adapter compatível com Executor.Escalator. 16 tests cobrindo every-kind hint, payload canonical+truncate, JSONL append/parent-mkdir, escalate happy/no-run/already-failed, make_escalator wiring, default path pinned.
- ✅ **F4.13** — `feat(F4): structlog JSON sink for logs/agent.jsonl`. `src/pipeline/agent/_logging.py` com `AgentEventLogger` (jsonl append + structlog stdout parallel), `Clock` protocol, `default_clock`/`fixed_clock` helpers, `CANONICAL_EVENTS` table (9 events: loop_started/iteration/stopped, batch_started, layer_started/completed, failure_detected, fix_applied, escalation). 11 tests cobrindo canonical events, jsonl append/order, return payload, lazy parent mkdir, byte-stable replay com fixed clock, stdout sync, default clock UTC-aware, default path pinned.

## Loop + CLI

- ✅ **F4.14** — `feat(F4): run_once orchestrator`. `src/pipeline/agent/loop.py` com `run_once` cabling lock → start_agent_run → scan → for batch_id: plan → for (layer, fn): executor.run_with_recovery → break-on-escalation (per-batch isolation F4-RF-08) → end_agent_run → release lock. Re-exporta `run_once` em `pipeline.agent.__init__`. 7 integration tests (`test_agent_run_once.py`) cobrindo no-op completion, lock release happy/exception, all-3-layers happy path, canonical event sequence (loop_started → batch_started → layer_started → layer_completed → loop_stopped), agent_run row written com COMPLETED, failure isolation entre batches (A escala, B continua).
- ✅ **F4.15** — `feat(F4): run_forever loop with cancelable interval`. `pipeline.agent.loop.run_forever` itera `run_once` com `threading.Event().wait(interval)` (cancelável, design §17 O3); `max_iters` cap para teste; `DEFAULT_LOOP_INTERVAL_S=60.0` pinado per spec §7 D5; sem sleep após última iter; retorna `list[AgentResult]`. Re-exportado em `pipeline.agent.__init__`. 5 integration tests cobrindo max_iters cap, max_iters=0 → empty, stop_event mid-loop short-circuit, max_iters=1 → no sleep, default interval pinned.
- ✅ **F4.16** — `feat(F4): pipeline agent CLI subcommand`. `src/pipeline/cli/agent.py` com `agent` click group + `run-once` / `run-forever` subcommands + env var overrides (AGENT_RETRY_BUDGET / AGENT_DIAGNOSE_BUDGET / AGENT_LOOP_INTERVAL / AGENT_LOCK_PATH). Wired em `__main__.py`. Default classifier curries `diagnoser.classify` (sem LLM client por enquanto, deterministic patterns ainda fire); placeholder `_empty_runners` factory + `_default_build_fix` returns None — real F2/F3 wiring deferred. 9 integration tests cobrindo help surfaces, run-once happy/manifest/lock/env, run-forever max_iters.

## E2E + demo

- ✅ **F4.17** — `feat(F4): inject_fault demo script`. `scripts/inject_fault.py` com 4 kinds (`schema_drift` adds injected_col, `regex_break` replaces first message_body com formato novo `❌ R$ 1.500,00`, `partition_missing` deletes dir/file, `out_of_range` appends negative-value row). Click CLI `--kind --target` + dispatch table + helper functions importable. 12 integration tests cobrindo cada kind happy path + edge cases (missing target raises, empty parquet raises, unknown kind raises) + CLI dispatch + help. Tests usam `tmp_path` em vez de `tests/fixtures/agent/<kind>/` por preferência de isolamento por teste.
- ✅ **F4.18** — `test(F4): budget exhaustion integration test`. `tests/integration/test_agent_budget_exhausted.py` exercita F4-RF-04..F4-RF-08 do nível do loop: runner+fix sempre falham → exatamente `retry_budget` rows em `agent_failures` + 1 escalação; respeita custom budget=5; per-batch isolation (A escala em Silver com 3 rows, B continua e completa todas 3 layers em ordem). 3 testes.
- ✅ **F4.19** — `test(F4): property test for monotonic count_attempts`. `tests/property/test_agent_state_props.py` (novo dir) com 4 testes deterministicos cobrindo invariants 1 (monotonic non-decreasing single triple + sob permutação seeded de 200 calls cross 36 triples) e 2 (triple isolation: gravar em A não incrementa B/C, unseen triple = 0). Hypothesis seria fit natural mas NFR-05 proíbe novas deps; usei seeded `random.Random(2026)` em vez disso.
- ⚪ **F4.20** — `chore(F4): smoke run on 153k fixture via agent` (`python -m pipeline agent run-once` end-to-end; valida SLA ≤15min warm-cache; anota tempo em `.specs/features/F4/SMOKE.md`).

## Closeout

- ⚪ **F4.21** — `docs(F4): walkthrough of agent flow + fault demo` (`docs/agent-flow.md` espelhando `docs/silver-flow.md` / `docs/gold-flow.md` do F2/F3).
- ⚪ **F4.22** — Lane de review (code-reviewer + security-reviewer + critic) em `.specs/features/F4/REVIEWS/`. Critical issues → tasks F4.23+.
- ⚪ **F4.23** — `docs(F4): close review lane and flip roadmap status` (M3 parcial → F5 desbloqueado).

---

## Dependências

```
F4.0 → F4.1 → F4.2 → F4.3
                 ├── F4.4 ── F4.5 / F4.6 / F4.7 / F4.8 (paralelos)
                 ├── F4.9
                 ├── F4.10
                 └── F4.11 ─ depende de F4.4 + fixes
F4.11 + F4.12 + F4.13 → F4.14 → F4.15 → F4.16
F4.14 → F4.17 → F4.18, F4.19
F4.16 → F4.20 → F4.21 → F4.22 → F4.23
```

## Definition of done (F4)

- 23/23 tasks ✅, todos commits atômicos.
- ≥90% cobertura em `src/pipeline/agent/**`.
- `ruff check` + `mypy --strict` limpos.
- Smoke run (F4.20) dentro do SLA M2.
- Review lane assinada sem critical pendente.
- ROADMAP M3 mostra F4 ✅ shipped + F5 ⚪ unblocked.
