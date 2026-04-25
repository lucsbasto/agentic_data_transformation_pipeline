# FIC tasks

> Decomposição atômica da lane FIC. Cada task = 1 commit `conventional-commit`. Tests verdes obrigatórios antes de fechar a task.
> Ordem é dependência-respeitada; tasks paralelas marcadas como tal.
> Status: ⚪ pending · 🟡 in-progress · ✅ done.

---

## Scaffolding

- ✅ **FIC.0** — `docs(FIC): scaffold spec/matrix/tasks` — este commit. Cria `.specs/features/FIC/spec.md`, `matrix.md`, `tasks.md`. Marca FIC.0 como done.
- ⚪ **FIC.1** — `test(FIC): shared campaign harness` — `tests/integration/fault_campaign/__init__.py` + `conftest.py` com fixtures `pipeline_tree`, `seeded_batch`, `fake_llm_client`, helpers `run_agent_once` / `assert_run_status` / `assert_agent_failure_count` / `tail_jsonl`. Registra marcador `fault_campaign` em `pyproject.toml`. Smoke tests em `test_harness_smoke.py`.
- ⚪ **FIC.2** — `feat(FIC): extend inject_fault.py with unknown kind` — Adiciona `inject_unknown` + kind `unknown` ao choice + dispatch. Testes cobrem happy path + edge cases. Verifica via `classify` com budget=0.

---

## Bundle B1: Single-kind E2E

- ⚪ **FIC.3** — `test(FIC): schema_drift recovers under budget` — inject extra column → run-once → assert `agent_failures` row, fix applied, retry succeeds, `runs` latest=COMPLETED, bronze byte-stable.
- ⚪ **FIC.4** — `test(FIC): regex_break override persists` — inject novel format → assert regex regenerated, `state/regex_overrides.json` atomic write, second run uses override.
- ⚪ **FIC.5** — `test(FIC): partition_missing re-emits bronze` — delete partition → F1 ingest re-runs via fix, partition recreated, identity matches.
- ⚪ **FIC.6** — `test(FIC): out_of_range ack closes loop` — append negative-value row → quarantine counted, `agent_failures.last_fix_kind=acknowledge_quarantine`, run COMPLETED.
- ⚪ **FIC.7** — `test(FIC): unknown kind escalates immediately` — malformed bytes → UNKNOWN, no fix, escalation event, run FAILED.

---

## Bundle B2: Budget + layer matrix

- ⚪ **FIC.8** — `test(FIC): budget exhaustion per kind` — for each fixable kind, force fix-side failure → exactly `retry_budget` rows + 1 escalation.
- ⚪ **FIC.9** — `test(FIC): per-batch isolation under fault storm` — A escalates, B completes all 3 layers.
- ⚪ **FIC.10** — `test(FIC): per-layer budget independence` — same kind at Silver and Gold tracked independently.
- ⚪ **FIC.11** — `test(FIC): bronze-layer fault matrix` — schema_drift + partition_missing.
- ⚪ **FIC.12** — `test(FIC): silver-layer fault matrix` — regex_break + out_of_range + schema_drift propagation.
- ⚪ **FIC.13** — `test(FIC): gold-layer fault matrix` — schema_drift + aggregate failure.

---

## Bundle B3: Compound + concurrency + lifecycle

- ⚪ **FIC.14** — `test(FIC): compound faults sequential resolution` — schema_drift + out_of_range same batch.
- ⚪ **FIC.15** — `test(FIC): cascading kind during fix` — second different kind on retry, no double-count.
- ⚪ **FIC.16** — `test(FIC): lock contention` — second runner raises `AgentBusyError`, no duplicate row.
- ⚪ **FIC.17** — `test(FIC): stale lock takeover` — dead PID + old mtime → atomic replace.
- ⚪ **FIC.18** — `test(FIC): SIGINT mid-loop marks INTERRUPTED` — latest `agent_runs` status, `loop_stopped` event.

---

## Bundle B4: Persistence + diagnoser

- ⚪ **FIC.19** — `test(FIC): state survives restart` — `agent_failures` counters preserved, budget continues.
- ⚪ **FIC.20** — `test(FIC): idempotent fix re-application` — schema_drift twice → no-op, byte-identical.
- ⚪ **FIC.21** — `test(FIC): regex override survives agent restart` — loaded, no LLM call, instant recovery.
- ⚪ **FIC.22** — `test(FIC): deterministic patterns shadow LLM` — fake client call count = 0.
- ⚪ **FIC.23** — `test(FIC): LLM fallback budget cap` — 11th unknown short-circuits.
- ⚪ **FIC.24** — `test(FIC): LLM malformed reply collapses to UNKNOWN`.

---

## Closeout

- ⚪ **FIC.25** — `docs(FIC): campaign matrix results table` — atualiza `matrix.md` com Status=✅ para todas as células e adiciona sumário de resultados.
- ⚪ **FIC.26** — `test(FIC): nightly campaign runner script` — `scripts/run_fault_campaign.py` com `-m fault_campaign`; verifica que sai com código 0 em campanha verde.

---

## Dependências

```
FIC.0 → FIC.1 → FIC.2
FIC.2 → FIC.3..FIC.7  (B1, paralelos entre si, dependem do harness FIC.1)
FIC.7 → FIC.8..FIC.13 (B2, paralelos entre si)
FIC.13 → FIC.14..FIC.18 (B3, paralelos entre si)
FIC.18 → FIC.19..FIC.24 (B4, paralelos entre si)
FIC.24 → FIC.25 → FIC.26
```

---

## Definition of done (FIC)

- 27/27 tasks ✅, todos commits atômicos.
- Cada célula de `matrix.md` com `Status = ✅ passing`.
- `pytest -m fault_campaign -q` verde em ≤60s wall.
- `ruff check tests/integration/fault_campaign/` limpo.
- Zero chamadas LLM reais em toda a campanha.
- `scripts/inject_fault.py --kind unknown` funcional e testado.
