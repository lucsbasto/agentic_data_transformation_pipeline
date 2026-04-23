# STATE

Memória persistente entre sessões. Curta e mutável. Não é log; é contexto carregado em toda sessão.

## Decisões tomadas (ADR-stubs preliminares — refinar em fase Design por feature)

- **D-001 (2026-04-22):** Engine = **Polars** (lazy API). Justificativa em PRD §16 ADR-001.
- **D-002 (2026-04-22, revisado):** LLM provider = **DashScope (Alibaba)** via endpoint Anthropic-protocol-compatible (`https://coding-intl.dashscope.aliyuncs.com/apps/anthropic`). SDK Anthropic oficial com `base_url` override. Modelo primário **`qwen3-max`** (reasoning forte p/ classificação de persona + extração analítica), fallback **`qwen3-coder-plus`** (barato, estruturado). Motivo da troca: chave disponível é DashScope, não Anthropic direto. Custo zero p/ esta task; mantém intercambialidade (se trocar p/ Anthropic direto, só muda env vars). PRD §16 ADR-002 precisa nota de rodapé.
- **D-003 (2026-04-22):** State store = **SQLite** single-file (`state/manifest.db`). PRD §16 ADR-003.
- **D-004 (2026-04-22):** Loop scheduler = **thread única + sleep configurável**. PRD §16 ADR-004.
- **D-005 (2026-04-22):** Observability = **structlog JSON + manifest por run**. PRD §16 ADR-005.
- **D-006 (2026-04-22):** Auto-correção = **retry budget 3 + escalonamento determinístico**. PRD §16 ADR-006.
- **D-007 (2026-04-22):** Modo pedagógico ativo. Cada decisão tem explicação inline para o usuário, ênfase extra em features de LLM.
- **D-008 (2026-04-22):** Repo = `git@github.com:lucsbasto/agentic_data_transformation_pipeline.git`. Branch default `main`. Inicializado na Phase 0. Raw parquet (`data/raw/conversations_bronze.parquet`, 8.7MB) commitado como test fixture; camadas Bronze/Silver/Gold derivadas são gitignored.
- **D-009 (2026-04-22):** Protocolo de commits = **Conventional Commits atômicos** via skill `/commit`. Um commit por unidade lógica (docs, chore, feat, test, fix, refactor). Nunca agrupar mudanças não relacionadas. Mensagens em EN (convenção da indústria).
- **D-010 (2026-04-22):** Skills de projeto em `.claude/skills/<name>/SKILL.md`. Carregadas automaticamente pelo Claude Code. Servem como guardrails de best-practices para Python, Polars, LLM client, agent loop e medallion layout. Evitam drift em sessões futuras.

## Blockers ativos

(nenhum)

## Lessons learned

(vazio — preencher conforme execução revela aprendizados)

## Todos pendentes

(todos de onboarding resolvidos 2026-04-22 — ver §Onboarding resolvido)

## Onboarding resolvido (2026-04-22)

- ✅ ROADMAP validado pelo usuário. Ordem M1→M2→M3→M4 confirmada.
- ✅ Parquet localizado em `data/raw/conversations_bronze.parquet` (8.7MB, já no filesystem).
- ✅ Chave API disponível em `.env` como `ANTHROPIC_API_KEY` (valor é DashScope key, SDK Anthropic reusa nome). `.env.example` criado.
- ✅ Repo GitHub inicializado agora (D-008). Remote `origin` configurado. Primeiro commit pendente.
- ✅ Modelo LLM escolhido: `qwen3-max` primário + `qwen3-coder-plus` fallback (D-002).
- ⚠️ Security: chave real foi detectada em `.env.example` durante criação; sobrescrita com placeholder. Usuário orientado a rotacionar no DashScope console se valor era produção.

## Deferred ideas

- **Vector store de personas:** embedar conversa inteira e indexar para busca semântica de leads similares. Não é requisito do teste; explorar se sobrar tempo.
- **Hot reload de regex:** agente injeta novo regex sem reiniciar processo. Bonus de robustez. Adiar.
- **Multi-agent (split observação e execução):** overkill para volume atual. Adiar.
- **Dashboard mínimo (Streamlit) lendo Gold:** explicitamente fora de escopo do PRD. Não fazer.

## Preferências do usuário

- Modo Caveman ativo (lite OK em prosa pedagógica, normal em código/decisões técnicas).
- Pedagogia obrigatória: explicar cada move + por quê, ênfase em features de LLM.
- Pausas para confirmação em decisões de alto impacto (custo LLM, agent loop, retry policy).
- Language: **English for all new artifacts** (docs, specs, prose, code, comments, commits). Changed 2026-04-22 by explicit user request. Existing PT-BR content (PRD.md, earlier STATE entries, earlier commits) is left as-is; only new writing goes in EN.

## Sessão atual

- Iniciada: 2026-04-22 (continuada em 2026-04-23).
- Foco: **M1 shipped.** Phase 0 (init) → F1 foundation completa.
- Marcos da sessão:
  - ROADMAP validado.
  - Git repo inicializado + remote configurado.
  - `.gitignore` + `.env.example` criados.
  - STATE.md revisado (D-002 repontado pra DashScope; D-008/D-009/D-010 adicionados).
  - Skills de projeto criadas em `.claude/skills/` (python, polars, llm-client, agent-loop, medallion).
  - F1.1 → F1.7 shipped: pyproject/uv bootstrap, settings+logging+errors+paths, Bronze+manifest schemas, ManifestDB with crash recovery, ingest pipeline (scan/transform/write), click CLI, LLMCache, LLMClient w/ retries+fallback.
  - F1.8 shipped: full README quickstart + real-parquet smoke run recorded (322 ms for 153,228 rows).
  - F1.9 shipped: holistic M1-close review (reviewer-pipeline + critic) both verdict SHIP M1. ROADMAP marks F1 shipped.
  - Three-agent review lane per task (code-reviewer + security-reviewer + critic); every wave's findings triaged in `.specs/features/F1/REVIEWS/INDEX.md`.
  - 110 tests green, coverage 96%, ruff + mypy strict clean.

## F1.8 smoke run (real parquet, 2026-04-23)

Measured against `data/raw/conversations_bronze.parquet` (8.7 MB, 153,228 rows):

- Command: `uv run python -m pipeline ingest`.
- First run:
  - `run_id=d581b35692b4`
  - `batch_id=d287cbb50cc3`
  - `source_hash=b2a6dacd57158a6d0f19a3edcce25c3a6b8cab52afa419fd52934ec4a0b196eb`
  - `rows_written=153228`
  - `duration_ms=322` (scan → Enum cast → lineage cols → zstd parquet write → re-scan roundtrip → manifest COMPLETED).
  - Output: `data/bronze/batch_id=d287cbb50cc3/part-0.parquet`, **4.2 MB** (~52% compression ratio vs raw source).
- Second run (idempotency):
  - short-circuited via `ingest.skip.already_completed`; no new Bronze write, no manifest mutation.
- Budget for full load (M2 target): <15 min. Bronze alone at 322 ms is comfortably inside the envelope — the budget headroom goes to the Silver LLM enrichment in F2/F3.

## M1 — close (2026-04-23)

F1 shipped. Evidence:

- ROADMAP M1 exit criteria all met (see `.specs/features/F1/REVIEWS/F1.9-m1-close.md`).
- Smoke run against the real 153,228-row parquet: 322 ms end-to-end; Bronze partition 4.2 MB zstd.
- 110 tests green, coverage 96%, ruff + mypy strict clean.
- Review trail: 6 waves of three-agent reviews (code-reviewer + security-reviewer + critic) + the F1.9 holistic close, all archived under `.specs/features/F1/REVIEWS/`.

### Carry-forward items for M2 / F2

The three seams most likely to tear when Silver lands:

1. **`LLMClient.cached_call` signature** will need a request-object refactor before the first Silver call — Silver wants structured output / batch / streaming beyond the current `system/user/model/max_tokens/temperature` surface.
2. **Lineage-column helper** — `transform_to_bronze` adds `batch_id + ingested_at + source_file_hash` inline. Extract into a shared helper before `transform_to_silver` copies the pattern.
3. **`ManifestDB.runs` table writers** — DDL exists, no API. Silver run tracking needs `insert_run` / `mark_run_completed` / `mark_run_failed`.

### Invariants pinned (ADR-only change)

1. Bronze `pl.Schema` + Enum closed sets (`src/pipeline/schemas/bronze.py`).
2. `batches` table DDL (`src/pipeline/schemas/manifest.py`) — Silver/Gold depend on the `batch_id` FK.
3. `compute_cache_key` hash formula (`src/pipeline/llm/cache.py`) — any change invalidates every cached LLM response; version the algorithm if altered.

### De-risk-F2 prototype (recommended first F2 move)

~100-line `SilverOrchestrator`: read one Bronze partition, send 5 conversations through `LLMClient.cached_call` against real `qwen3-max`, write one Silver parquet. Forces concrete answers on Enum round-trip, prompt template shape, SQLite WAL contention between `ManifestDB` and `LLMCache`, `runs`-table writers, and the real `qwen3-max` latency number — before any full Silver code lands.

- Next: start F2 Design when ready. Natural pause point here.
