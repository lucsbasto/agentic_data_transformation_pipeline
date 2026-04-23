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
- Idioma de docs/specs: PT-BR (PRD está em PT-BR).
- Idioma de código/comentários: EN (convenção padrão da indústria; deixa código portável).

## Sessão atual

- Iniciada: 2026-04-22
- Foco: Phase 0 (init) → preparar F1.
- Marcos da sessão:
  - ROADMAP validado.
  - Git repo inicializado + remote configurado.
  - `.gitignore` + `.env.example` criados.
  - STATE.md revisado (D-002 repontado pra DashScope; D-008/D-009/D-010 adicionados).
  - Skills de projeto criadas em `.claude/skills/` (python, polars, llm-client, agent-loop, medallion).
- Próximo: atomic commits → iniciar F1 Design.
- Tasks ativos: ver TaskList do orquestrador.
