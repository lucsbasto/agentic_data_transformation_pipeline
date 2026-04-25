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

- ⚪ **F4.9** — `feat(F4): observer scans pending batches` (`src/pipeline/agent/observer.py` com detecção de stale `IN_PROGRESS`). Tests: `tests/unit/test_agent_observer.py` cobre fresh/dirty/COMPLETED/FAILED + ordenação determinística.
- ⚪ **F4.10** — `feat(F4): planner emits sequential bronze→silver→gold plan` (`src/pipeline/agent/planner.py` pula layers já COMPLETED). Tests: `tests/unit/test_agent_planner.py`.
- ⚪ **F4.11** — `feat(F4): executor with retry budget per (batch_id, layer, error_class)` (`src/pipeline/agent/executor.py`). Tests: `tests/unit/test_agent_executor.py` cobre 3 retries → escala, fix sucesso, fix falha, `UNKNOWN` escala imediato.

## Escalator + logging

- ⚪ **F4.12** — `feat(F4): escalator emits structured JSON alert + suggested fix table` (`src/pipeline/agent/escalator.py`). Tests: `tests/unit/test_agent_escalator.py` valida shape JSON, stdout summary, manifest update (`escalated=1`, `runs.status='FAILED'`).
- ⚪ **F4.13** — `feat(F4): structlog JSON sink for logs/agent.jsonl` (`src/pipeline/agent/_logging.py` + clock injetável). Tests: `tests/unit/test_agent_logging.py` com clock mockado valida log byte-idêntico.

## Loop + CLI

- ⚪ **F4.14** — `feat(F4): run_once orchestrator` (`src/pipeline/agent/loop.py` glue: lock → start_run → observer → planner → executor → end_run). Tests: `tests/integration/test_agent_run_once_clean.py` (no-op), `tests/integration/test_agent_run_once_full.py` (Bronze→Gold from scratch).
- ⚪ **F4.15** — `feat(F4): run_forever loop with cancelable interval` (uso de `threading.Event().wait` para cancelamento limpo, `max_iters` para teste). Tests: `tests/integration/test_agent_run_forever_max_iters.py` valida 2 iterações + SIGINT clean shutdown.
- ⚪ **F4.16** — `feat(F4): pipeline agent CLI subcommand` (`src/pipeline/cli/agent.py` com `run-once` / `run-forever`, env var overrides). Tests: `tests/integration/test_cli_agent.py` cobre exit codes, JSON output, env override.

## E2E + demo

- ⚪ **F4.17** — `feat(F4): inject_fault demo script` (`scripts/inject_fault.py` com 4 kinds + fixtures em `tests/fixtures/agent/<kind>/`). Tests: `tests/integration/test_agent_per_kind.py` parametriza por `ErrorKind`.
- ⚪ **F4.18** — `test(F4): budget exhaustion integration test` (`tests/integration/test_agent_budget_exhausted.py` força 4 falhas → 1 escalação, isola `batch_id_B` continuando).
- ⚪ **F4.19** — `test(F4): property test for monotonic count_attempts` (Hypothesis em `tests/property/test_agent_state_props.py`).
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
