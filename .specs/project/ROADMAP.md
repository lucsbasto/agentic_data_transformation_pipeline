# ROADMAP

## Estado atual

- Fase: **M2 concluído — pronto para M3**
- Próxima decisão do usuário: autorizar início de F4 (agent core); revisar caveats de SLA cold-run e findings F3 no backlog.

## Milestones

### M1 — Foundation (F1)

Pipeline mínimo executável + cliente LLM com cache. Sem inteligência ainda; só infraestrutura.

| Feature | Status | Deps | Tamanho |
|---|---|---|---|
| **F1 — Bronze ingest + LLMClient base** | ✅ shipped (2026-04-23) | — | Medium |

**Critério de fim do M1:** `python -m pipeline ingest` lê o parquet, escreve Bronze particionado, registra batch_id no manifest. `LLMClient.cached_call()` funciona com cache hit/miss. Testes verdes.

**Status da entrega:** smoke run em 322 ms para 153.228 linhas (Bronze zstd 4,2 MB). 110 testes verdes, cobertura 96%, ruff + mypy strict limpos. Lane de review de 3 agentes (code-reviewer + security-reviewer + critic) + sweep final `F1.9` assinaram M1. Detalhes em `.specs/features/F1/REVIEWS/INDEX.md`.

---

### M2 — Data layer (F2 + F3)

Camadas Silver e Gold completas. Aqui mora a maior parte da lógica de IA.

| Feature | Status | Deps | Tamanho |
|---|---|---|---|
| **F2 — Silver transforms** | ✅ shipped (2026-04-24) | F1 | Large |
| **F3 — Gold analytics + personas** | ✅ shipped (2026-04-24) | F2 | Large |

**Escopo F2 congelado:** dedup + normalize + PII mask positional + lead_id HMAC-SHA256 + manifest `silver_runs` + CLI `transform-silver --batch-id`. **Fora:** extração LLM (veículo/concorrente/sinistro) deslocada para F3. Spec/design/tasks em `.specs/features/F2/`.

**Critério de fim do M2:** Bronze → Silver → Gold roda end-to-end em <15min full load. Gold tem 4 tabelas + 4 insights não-óbvios. Personas classificadas com guard-rails duros.

**Status da entrega M2:** 616 testes verdes, 95.40% cobertura, ruff + mypy strict limpos. Smoke warm-cache: ~2min end-to-end (dentro do SLA). Smoke cold-cache: ~35min (Silver LLM extract = 5000 calls, 0 hits → **SLA breach em cold run**; incremental / cached runs atendem). Gold entrega 4 tabelas (`conversation_scores=15000`, `lead_profile=14938`, `competitor_intel`, `agent_performance=20`) + `insights/summary.json` com 4 chaves (`ghosting_taxonomy`, `objections`, `disengagement_moment`, `persona_outcome_correlation`). Personas com guard-rails (R1/R2/R3) antes do LLM. Lane de review 3-agent (code-reviewer + security-reviewer + critic) em `.specs/features/F3/REVIEWS/INDEX.md`: 0 critical não-resolvidos, 1 high (budget-waste em rule-hit), 7 medium, risco de segurança LOW. Follow-up obrigatório antes de F4 consumir sinais: C1 (`_coerencia_expr` emite 1.5 fora de [0,1]) e M1 (`price_sensitivity` coluna morta).

---

### M3 — Agent autonomy (F4 + F5)

Orquestrador e CLI. Pipeline vira pipeline vivo.

| Feature | Status | Deps | Tamanho |
|---|---|---|---|
| **F4 — Agent core (loop + auto-correção)** | ⚪ blocked | F1 | Complex |
| **F5 — Observability + CLI** | ⚪ blocked | F4 | Medium |

**Critério de fim do M3:** `python -m pipeline watch` mantém pipeline vivo. Injeção de falha sintética dispara ciclo diagnose→fix→retry→escalate documentado em log estruturado.

---

### M4 — Quality & differentials (Tests, Demo, F6)

| Feature | Status | Deps | Tamanho |
|---|---|---|---|
| **Tests + CI** | ⚪ pending | F1 (parallel possible) | Medium |
| **Demo auto-correção** | ⚪ blocked | F4 | Small |
| **F6 — Databricks runner (bonus)** | ⚪ blocked | F5 | Medium |

**Critério de fim do M4:** CI verde, README permite clone→run em <10min, demo de auto-correção reproduzível, runner Databricks documentado.

---

## Mapa de dependências

```
F1 (Bronze + LLMClient)
 ├── F2 (Silver) ── F3 (Gold)
 └── F4 (Agent) ── F5 (CLI/Obs) ── F6 (Databricks bonus)
                              └── Demo
Tests/CI: paralelo com qualquer feature após F1
```

## Ordem de execução recomendada

1. **F1** — sem dependências, fundação obrigatória
2. **F2** — desbloqueia F3 e tem mais lógica de extração
3. **F3** — produto analítico final
4. **F4** — pode iniciar em paralelo com F2/F3 se houver outro agente; em modo single-agent, vai depois de F3
5. **F5** — depende de F4 estar usável
6. **Tests/CI** — escalonado: smoke tests por F1, suíte completa em F5
7. **Demo + F6** — fechamento

## Ponto de pausa pedagógica

Vou pausar para validação do usuário em:
- Após init project (agora) → você revisa ROADMAP
- Após Design de F1 → você aprova stack final (Polars vs DuckDB, Anthropic vs alternativa)
- Após Design de F2 → você decide trade-offs de custo LLM
- Após Design de F4 → você decide retry budget, política de escalonamento
- Após Tasks de F4 → você confirma ordem de execução do agente

Pausas curtas são intencionais. Decisões de IA precisam de input humano para não viralizar erro.
