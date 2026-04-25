# FIC — Spec: Fault Injection Campaigns (test lane)

> Status: **active** — lane pura de testes; nenhum código de produção é alterado.
> Origem: F4 milestone shipped; lane FIC garante cobertura exaustiva de cenários de falha.
> Dependências: F4 completo (todas as 23 tasks ✅).

---

## 1. Visão

A lane FIC é o **test harness de cobertura exaustiva** do agente F4. Não entrega features;
entrega um conjunto de testes determinísticos que provam que cada célula da matriz
`(layer × ErrorKind × outcome)` se comporta conforme o contrato do spec F4.

Enquanto F4 prova *que o agente existe*, FIC prova *que ele funciona em todo o espaço de falhas*.

---

## 2. Escopo

### 2.1. Dentro do escopo

- Cobertura de todas as células `(layer × ErrorKind × outcome={recovered, escalated})`.
- Ciclo de vida de concorrência: contenção de lock, takeover de lock stale, SIGINT mid-loop.
- Persistência e replay: contadores de `agent_failures` sobrevivem a restart, idempotência de fix.
- Todos os ramos do diagnoser: hits determinísticos, fallback LLM, budget esgotado, resposta malformada.
- Script `scripts/inject_fault.py` estendido com o quinto kind (`unknown`).
- Harness compartilhado (`tests/integration/fault_campaign/conftest.py`) reutilizado por toda a campanha.

### 2.2. Fora do escopo

- Novas features do agente (fix strategies, novos ErrorKinds, wiring de entrypoints reais).
- Benchmarking de performance (lane separada PERF).
- Integração com sistemas externos (Slack, PagerDuty, Prometheus).
- Mudanças em código de produção além de `scripts/inject_fault.py`.

---

## 3. Requisitos funcionais

| ID | Requisito |
|---|---|
| FIC-RF-01 | Todo par `(layer, ErrorKind)` com outcome `recovered` tem ≥1 teste E2E que injeta a falha, executa `run_once`, e asserta fix aplicado + `runs.status=COMPLETED`. |
| FIC-RF-02 | Todo par `(layer, ErrorKind)` com outcome `escalated` tem ≥1 teste que força falha persistente, esgota budget, e asserta `agent_failures.escalated=1`. |
| FIC-RF-03 | O ciclo de vida de budget é testado por kind: exatamente `retry_budget` linhas em `agent_failures` + 1 escalação. |
| FIC-RF-04 | Isolamento por batch: falha em `batch_A` não impede conclusão de `batch_B` (verificado em cada bundle). |
| FIC-RF-05 | Concorrência: segundo runner lança `AgentBusyError`; takeover de lock stale ocorre em ≤1 tentativa. |
| FIC-RF-06 | `SIGINT` mid-loop sela `agent_runs.status=INTERRUPTED` e emite evento `loop_stopped`. |
| FIC-RF-07 | Persistência: `agent_failures` counters são preservados entre instâncias de `ManifestDB` apontando para o mesmo arquivo SQLite. |
| FIC-RF-08 | Diagnoser com fake LLM: call count verificável; budget zero → `UNKNOWN` sem chamada; resposta malformada → `UNKNOWN`. |

---

## 4. Contratos de dados (referência)

FIC não altera tabelas. Referência apenas:

- `agent_runs(agent_run_id, started_at, ended_at, status, batches_processed, failures_recovered, escalations)` — F4.2.
- `agent_failures(failure_id, agent_run_id, batch_id, layer, error_class, attempts, last_fix_kind, escalated, last_error_msg, ts)` — F4.2.
- `logs/agent.jsonl` — append-only JSONL com 9 eventos canônicos — F4.13.

---

## 5. Não-funcionais

- **NFR-FIC-01** Determinismo: todos os testes usam seed fixo (`random.Random(2026)` onde aplicável) e `fixed_clock` do `_logging` module.
- **NFR-FIC-02** Sem chamadas LLM reais: todo teste usa `fake_llm_client` do harness; `ANTHROPIC_API_KEY` não precisa estar presente.
- **NFR-FIC-03** Wallclock total da campanha `pytest -m fault_campaign` ≤ 60 segundos.
- **NFR-FIC-04** Sem novas dependências (NFR-05 de F4 herdado). Usa stdlib + polars + click + pytest + structlog + anthropic.
- **NFR-FIC-05** Cada teste é isolado: usa `tmp_path` ou `ManifestDB(":memory:")` — sem estado compartilhado entre testes.

---

## 6. Critérios de aceitação

- Cada célula da `matrix.md` tem `Status = ✅ passing`.
- `pytest -m fault_campaign -q` verde em ≤60s wall.
- `ruff check tests/integration/fault_campaign/` limpo.
- Marcador `fault_campaign` registrado em `pyproject.toml`.
- Sem chamadas LLM reais (verificado pelo contador `.calls` do `fake_llm_client`).
- `inject_fault.py --kind unknown` corrompe o arquivo e força `ErrorKind.UNKNOWN` via classificador.
