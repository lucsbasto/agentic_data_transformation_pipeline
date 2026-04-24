# F4 — Spec: Agent core (loop + auto-correção)

> Status: **draft** — aguarda assinatura do usuário antes de avançar para `design.md`.
> Origem: PRD §7 + §8 + §10 + §11 + ADR-003/004/005/006. Roadmap M3.
> Dependências: F1 (manifest + LLMClient), F2 (Silver), F3 (Gold).

---

## 1. Visão

O agente é o **orquestrador autônomo** do pipeline. Não é um notebook nem um script de uma execução; é um processo que:

1. **Observa** a Bronze (crescimento de fonte / novo `batch_id`).
2. **Planeja** o próximo passo incremental (ingest → silver → gold).
3. **Executa** cada etapa chamando os entrypoints já existentes (F1/F2/F3).
4. **Diagnostica** falhas e tenta **auto-corrigir** dentro de um *retry budget*.
5. **Escalona** para o operador (log estruturado + alerta) quando o budget esgota.
6. **Mantém-se vivo** em loop com `sleep(interval)` configurável, ou roda em *single-shot*.

F4 entrega o **núcleo do agente** (módulo `pipeline.agent`); F5 entrega CLI `watch` + observabilidade pública. F4 deve ser usável por F5 sem reescrita.

---

## 2. Escopo

### 2.1. Dentro do escopo

- Módulo `src/pipeline/agent/` com:
  - `loop.py` — laço principal (`run_once`, `run_forever(interval)`).
  - `observer.py` — detecta delta na Bronze (novos `batch_id` na fonte vs `manifest.runs`).
  - `planner.py` — decide quais layers (`silver`, `gold`) precisam rodar para cada `batch_id` pendente.
  - `executor.py` — invoca entrypoints existentes (`pipeline.bronze.ingest`, `pipeline.silver.transform`, `pipeline.gold.transform`) com retry budget.
  - `diagnoser.py` — classifica falha (transiente vs permanente) e propõe `Fix`.
  - `escalator.py` — emite alerta estruturado quando `Fix` falha N vezes.
  - `state.py` — persiste contadores de retry e estado do loop em `manifest.db` (tabela nova `agent_runs`).
- Tabelas de manifest novas:
  - `agent_runs(agent_run_id, started_at, ended_at, status, batches_processed, failures_recovered, escalations)`.
  - `agent_failures(failure_id, agent_run_id, batch_id, layer, error_class, attempts, last_fix_kind, escalated, ts)`.
- Loop de auto-correção determinístico para **3 classes** de falha cobertas pelo PRD §7.1:
  - **schema_drift** (coluna nova/faltante na Bronze) → ajuste de parser + re-run.
  - **regex_break** (regex de Silver não casa novo formato) → regenera padrão via LLM (`LLMClient.diagnose`) + valida fixture mínima + re-run.
  - **partition_missing** (parquet ausente) → recria diretório/`batch_id` no manifest + re-run.
  - **out_of_range** (valor numérico fora de range) → roteia registro para `bronze.rejected` (quarentena já existente em F1) + re-run.
- *Retry budget* = **3** tentativas por `(batch_id, layer, error_class)` antes de escalonar (ADR-006).
- Escalação: log estruturado JSON + linha em `agent_failures.escalated=1`. **Sem** integração com PagerDuty/Slack (fora de escopo; F5 trata).
- CLI mínima de F4: `python -m pipeline agent run-once` e `python -m pipeline agent run-forever --interval 60`. CLI completa fica em F5.
- Demo de injeção sintética de falha (`scripts/inject_fault.py`) — 1 fixture por classe de erro acima.

### 2.2. Fora do escopo

- CLI `watch` rica, dashboards, métricas Prometheus → **F5**.
- Auto-correção fora das 4 classes acima (ex.: timeout LLM persistente, OOM Polars).
- Paralelismo entre runs do agente (ADR-004 fixa thread única).
- Streaming / event-driven triggers (PRD §3 fora de escopo).
- Mudança de retry budget em runtime (configurável via env apenas).
- Auto-correção que envolva DDL externo (SQLite manifest pode evoluir; nada além).

---

## 3. Requisitos funcionais

| ID | Requisito |
|---|---|
| F4-RF-01 | `pipeline.agent.loop.run_once()` lê estado da Bronze, identifica `batch_id`s pendentes (presentes em fonte, ausentes ou `FAILED` em `manifest.runs`) e dispara a sequência `bronze → silver → gold` para cada um. |
| F4-RF-02 | `run_forever(interval, max_iters=None)` executa `run_once` em loop com `time.sleep(interval)`. `max_iters` permite teste determinístico. |
| F4-RF-03 | Antes de cada `batch_id`, o planner consulta `manifest.runs` para determinar quais layers já têm `status='COMPLETED'`; layers já concluídas são puladas (idempotência). |
| F4-RF-04 | Cada execução de layer é envelopada por `executor.run_with_recovery(layer, batch_id)`, que captura exceções, classifica via `diagnoser.classify(exc) -> ErrorKind`, aplica `Fix`, e re-tenta até `retry_budget` (default 3). |
| F4-RF-05 | `ErrorKind ∈ {schema_drift, regex_break, partition_missing, out_of_range, unknown}`. `unknown` **não** é auto-corrigível: escalona imediatamente. |
| F4-RF-06 | Cada tentativa registra linha em `agent_failures` com `attempts` incrementado. Estouro de budget seta `escalated=1` e marca `runs.status='FAILED'` para o `(batch_id, layer)` afetado. |
| F4-RF-07 | Alerta de escalação é log JSON estruturado em `logs/agent.jsonl` com chaves `event='escalation'`, `batch_id`, `layer`, `error_class`, `last_error_msg`, `suggested_fix`. Stdout recebe linha humana resumida. |
| F4-RF-08 | Falha em `(batch_id_A, layer_X)` **não derruba** processamento de `batch_id_B` (independência por batch). |
| F4-RF-09 | Loop é interrompível por `SIGINT`/`SIGTERM` sem perda de estado: `agent_runs.status` vira `INTERRUPTED`, layer em curso recebe `runs.status='IN_PROGRESS'` recuperável na próxima rodada. |
| F4-RF-10 | `LLMClient.diagnose(error_ctx) -> Fix` é a única chamada LLM do F4; é *cacheada* (mesmo hash de `error_ctx` → mesmo `Fix`). Custo limitado por `agent.diagnose_budget` (default 10 chamadas/`run_once`). |
| F4-RF-11 | Idempotência absoluta: rodar `run_once` N vezes sobre Bronze inalterada não cria novas linhas em `agent_failures` nem reprocessa layers `COMPLETED`. |
| F4-RF-12 | Demo `scripts/inject_fault.py --kind {schema_drift,regex_break,partition_missing,out_of_range}` modifica fixture controlada e roda `agent run-once`; resultado esperado: 1 falha registrada + 1 fix aplicado + layer concluída. |

---

## 4. Contratos de dados

### 4.1. `manifest.agent_runs`

| coluna | tipo | nota |
|---|---|---|
| `agent_run_id` | TEXT PK | UUID |
| `started_at` | TEXT | ISO-8601 UTC |
| `ended_at` | TEXT NULL | NULL enquanto `IN_PROGRESS` |
| `status` | TEXT | `IN_PROGRESS` / `COMPLETED` / `INTERRUPTED` / `FAILED` |
| `batches_processed` | INT | count distinct `batch_id` tocados |
| `failures_recovered` | INT | tentativas com `Fix` aplicado com sucesso |
| `escalations` | INT | linhas em `agent_failures` com `escalated=1` deste run |

### 4.2. `manifest.agent_failures`

| coluna | tipo | nota |
|---|---|---|
| `failure_id` | TEXT PK | UUID |
| `agent_run_id` | TEXT FK → `agent_runs` | |
| `batch_id` | TEXT | |
| `layer` | TEXT | `bronze` / `silver` / `gold` |
| `error_class` | TEXT | `ErrorKind` enum |
| `attempts` | INT | 1..N |
| `last_fix_kind` | TEXT NULL | nome do `Fix` aplicado |
| `escalated` | INT | 0/1 |
| `last_error_msg` | TEXT | sanitizado, ≤512 chars |
| `ts` | TEXT | ISO-8601 UTC |

Índices: `(agent_run_id)`, `(batch_id, layer, error_class)`.

### 4.3. `logs/agent.jsonl`

Append-only. Eventos: `loop_started`, `loop_iteration`, `batch_started`, `layer_started`, `layer_completed`, `failure_detected`, `fix_applied`, `escalation`, `loop_stopped`.

---

## 5. Não-funcionais

- **NFR-01** Latência: `run_once` em Bronze sem delta termina em ≤ 1s (só consulta manifest).
- **NFR-02** Custo LLM: ≤ 10 chamadas `diagnose` por `run_once` (budget hard).
- **NFR-03** Cobertura de testes: ≥ 90% sobre `src/pipeline/agent/**`.
- **NFR-04** Logs estruturados em JSON; sem `print()` no código de produção.
- **NFR-05** Sem dependências novas além das já em `pyproject.toml` (Polars, structlog, anthropic, pytest).
- **NFR-06** Determinismo: mesma fixture + mesma seed produz mesma sequência de eventos no log.

---

## 6. Critérios de aceitação

- `python -m pipeline agent run-once` em pipeline limpo (sem manifest) executa `bronze → silver → gold` end-to-end e exibe `agent_runs.status='COMPLETED'`.
- `python -m pipeline agent run-once` em pipeline já COMPLETED é no-op (≤1s, 0 layers reprocessadas).
- `pytest tests/unit/test_agent_*.py tests/integration/test_agent_*.py` verde com ≥90% cobertura em `src/pipeline/agent/**`.
- Cada `ErrorKind` de §3 (`schema_drift`, `regex_break`, `partition_missing`, `out_of_range`) tem ≥1 teste de injeção positivo + 1 teste de orçamento esgotado (escalonamento).
- `scripts/inject_fault.py --kind regex_break` produz log de fix aplicado, layer Silver concluída, e nenhuma linha escalonada.
- Smoke E2E: Bronze (153k linhas) → ... → Gold via agent em ≤ 15min warm-cache (mantém SLA M2).
- `ruff check` e `mypy --strict` limpos em `src/pipeline/agent/**`.

---

## 7. Decisões assinadas pelo usuário (2026-04-24)

| # | Pergunta | Decisão |
|---|---|---|
| D1 | Retry budget = 3 por `(batch_id, layer, error_class)`? | ✅ sim, confirmado. Não é global por run. |
| D2 | Lockfile `state/agent.lock` em F4? | ✅ sim, F4 entrega lockfile com PID + cleanup em SIGINT/SIGTERM + recuperação de lock stale. |
| D3 | `diagnose_budget = 10` chamadas LLM por `run_once`? | ✅ sim, default 10. Configurável via env `AGENT_DIAGNOSE_BUDGET`. |
| D4 | 4 classes de erro auto-corrigíveis (`schema_drift`, `regex_break`, `partition_missing`, `out_of_range`)? | ✅ sim, escopo fechado. Outras classes → `unknown` → escalonamento imediato. |
| D5 | `run_forever` interval default = 60s? | ✅ sim. Configurável via flag `--interval` ou env `AGENT_LOOP_INTERVAL`. |

## 8. Riscos / questões residuais (não bloqueantes)

| # | Risco | Mitigação |
|---|---|---|
| R1 | `regex_break` regenerado por LLM sem validação humana | Validação é fixture Silver passando. Human-in-the-loop fica para F4.x se necessário. |
| R2 | `out_of_range` depende de F1 expor API para mover linhas de `bronze.rejected` | Validar em design.md; se ausente, criar `pipeline.bronze.rejected.requeue(batch_id)`. |
| R3 | Cold-run SLA breach do M2 (Silver LLM ~35min) | Não bloqueante para F4. Agent não muda budget LLM. |
| R4 | Demo de injeção: `scripts/` vs `examples/` | `scripts/inject_fault.py`. M4 “Demo auto-correção” = wrapper sobre essa demo. |

Próximo artefato: `design.md` (módulos detalhados, sequência, ADRs derivados, exemplos de log).
