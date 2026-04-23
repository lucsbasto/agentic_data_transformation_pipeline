# ROADMAP

## Estado atual

- Fase: **Phase 0 — Init**
- Próxima decisão do usuário: validar ordem de features abaixo, autorizar início de F1.

## Milestones

### M1 — Foundation (F1)

Pipeline mínimo executável + cliente LLM com cache. Sem inteligência ainda; só infraestrutura.

| Feature | Status | Deps | Tamanho |
|---|---|---|---|
| **F1 — Bronze ingest + LLMClient base** | 🔵 pending | — | Medium |

**Critério de fim do M1:** `python -m pipeline ingest` lê o parquet, escreve Bronze particionado, registra batch_id no manifest. `LLMClient.cached_call()` funciona com cache hit/miss. Testes verdes.

---

### M2 — Data layer (F2 + F3)

Camadas Silver e Gold completas. Aqui mora a maior parte da lógica de IA.

| Feature | Status | Deps | Tamanho |
|---|---|---|---|
| **F2 — Silver transforms** | ⚪ blocked | F1 | Large |
| **F3 — Gold analytics + personas** | ⚪ blocked | F2 | Large |

**Critério de fim do M2:** Bronze → Silver → Gold roda end-to-end em <15min full load. Gold tem 4 tabelas + 4 insights não-óbvios. Personas classificadas com guard-rails duros.

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
