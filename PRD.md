# PRD — Pipeline de Transformação Agêntica de Dados

**Produto:** Agente autônomo que constrói, opera e mantém um pipeline de dados em arquitetura medalhão (Bronze → Silver → Gold) sobre conversas transacionais de WhatsApp de uma operação de venda de seguro automotivo.

**Autor:** Lucas Bastos
**Data:** 2026-04-21
**Status:** Draft v2 — base para Spec-Driven Development
**Repositório:** público no GitHub (a ser criado)
**Origem:** este PRD operacionaliza o desafio descrito em `Teste Técnico de Data & AI Engineering.md` (fonte de verdade para escopo e critérios de avaliação).

---

## 1. Contexto e Problema

A operação comercial utiliza WhatsApp como canal principal de prospecção e fechamento de seguro auto, com vendedores humanos negociando diretamente com leads. Os dados gerados (~15k conversas, 120k–150k mensagens no mês de fev/2026) são transacionais, não estruturados, contêm imperfeições reais (nomes inconsistentes, dados pessoais em texto livre, duplicidade de status, transcrições de áudio com ruído, menções a concorrentes em linguagem informal, etc.) e crescem continuamente.

Hoje não existe camada analítica persistente: análises são pontuais, não reproduzíveis e não escalam com o crescimento da fonte. Falta infraestrutura de dados viva capaz de classificar leads, segmentar audiências, detectar personas e extrair sinais de negociação sem intervenção humana a cada ciclo.

> **Distinção crítica do teste técnico:** o entregável **não é uma análise pontual**. É **infraestrutura de dados persistente**, equivalente a um job orquestrado em Databricks: o pipeline existe, é versionado, observável, e a Gold é mantida viva pelo agente conforme novos dados chegam à Bronze.

## 2. Objetivo

Entregar um **agente de IA** que **cria e gerencia um pipeline de dados persistente**, não apenas executa análises pontuais. O agente deve:

1. Construir o pipeline em código Python, em 3 camadas (Bronze, Silver, Gold).
2. Operar de forma autônoma: detectar falhas, alertar, propor ou aplicar correções.
3. Manter o pipeline vivo: atualizar automaticamente a Gold quando a Bronze crescer.
4. Produzir classificações e insights analíticos na Gold (além dos exemplos óbvios do enunciado).

## 3. Fora de Escopo

- Dashboards, BI ou interfaces de visualização finais.
- Ingestão em tempo real (streaming). Atualização é incremental por lotes disparada por crescimento da fonte.
- Modelos de ML treinados do zero; uso de LLMs e bibliotecas prontas é permitido.
- Integração com CRM real da operação de vendas.

## 4. Usuários e Stakeholders

- **Avaliador técnico** (primário): valida engenharia de dados, autonomia do agente e qualidade de código.
- **Engenheiro de dados / Analista** (persona futura): consome a Gold para segmentação e estratégia comercial.
- **Operador do pipeline**: recebe alertas quando o agente não consegue se auto-corrigir.

## 5. Fonte de Dados

- **Arquivo:** `conversations_bronze.parquet` (~9 MB, ~15k conversas, 120k–150k mensagens, 01/02/2026 a 28/02/2026).
- **Download oficial:** [Google Drive — conversations_bronze.parquet](https://drive.google.com/file/d/1rt-bLmt3ccR0_BL424WcAGpUMh14PKiO/view?usp=sharing).
- **Dicionário de dados:** [Dicionário de Dados — Teste Técnico de Data & AI Engineering](https://docs.google.com/document/d/1QO2NohJ3BJOMmQiQBtK1lGyPJxfZ9Dpii6Y4oO0maLM/) (referência canônica de tipos e semântica de cada coluna).
- **Grão:** 1 linha = 1 mensagem dentro de uma conversa.
- **Schema resumido:** `message_id`, `conversation_id`, `timestamp`, `sender_phone`, `sender_name`, `message_body`, `campaign_id`, `agent_id`, `direction` (inbound/outbound), `message_type` (text/image/audio/document/sticker), `status` (sent/delivered/read/failed), `channel`, `conversation_outcome` (venda_fechada, perdido_preco, perdido_concorrente, ghosting, desistencia_lead, proposta_enviada, em_negociacao), `metadata` (JSON com `device`, `city`, `state`, `response_time_sec`, `is_business_hours`, `lead_source`).
- **Imperfeições conhecidas:** nomes inconsistentes de leads; CPF/CEP/e-mail/telefone/placa no texto livre; duplicidade `sent`+`delivered`; transcrições de áudio com erros; `message_body` vazio em stickers/imagens; dados de veículo não padronizados; menções a concorrentes (Porto Seguro, Azul, Bradesco, SulAmérica, Liberty, Allianz) em linguagem informal; histórico de sinistros em texto natural.
- **Distribuição de conversas:** ~35% bounce (2–4 msgs), ~30% curtas (5–10), ~25% médias (11–20), ~10% longas (21–30+).
- **LGPD:** dados pessoais (nomes, e-mails, CPF, telefone, placa) devem ser mascarados preservando as dimensões originais.

## 6. Arquitetura do Pipeline

### 6.1. Camada Bronze (raw preservado)
- Ingestão idempotente do parquet fonte sem alterar conteúdo.
- Adiciona colunas técnicas: `ingested_at`, `source_file_hash`, `batch_id`.
- Particionamento por `date(timestamp)`.
- Contrato: append-only. Retenção total.

### 6.2. Camada Silver (limpo, normalizado, um registro por `{conversation_id, message_id}` deduplicado)
Transformações obrigatórias:
- **Deduplicação** de eventos `sent`/`delivered` mantendo evento mais recente por mensagem.
- **Normalização de `sender_name`** do lead: trim, case, remoção de acentos, reconciliação por `sender_phone` (chave estável) — resolve "Ana Paula Ribeiro" / "ana paula" / "Ana P." como mesmo lead.
- **Extração estruturada via regex + LLM** de: e-mail, CPF, CEP, telefone, placa, marca/modelo/ano do veículo, valor atual pago em concorrente, concorrente mencionado, histórico de sinistros.
- **Mascaramento LGPD** preservando dimensões: `a****@g****.com`, `***.***.***-**`, placa `***1D**`, etc.
- **Parse de `metadata`** JSON em colunas tipadas.
- **Limpeza de transcrições de áudio**: marca nível de confiança quando detecta ruído.
- **Tratamento de vazios**: stickers/imagens sem caption marcadas com flag `has_content=false`.
- **Chave de lead**: `lead_id = hash(sender_phone_normalizado)` — permite múltiplas conversas do mesmo lead.

### 6.3. Camada Gold (analítica, agregada por conversa e por lead)
Tabelas:
- **`gold.conversation_features`** — 1 linha por `conversation_id`: duração, nº mensagens por direção, tempo médio de resposta do lead, tempo até primeira resposta, nº de msgs fora do horário comercial, flag `mencionou_concorrente`, concorrente citado, veículo extraído, faixa de valor atual, `conversation_outcome`.
- **`gold.lead_profile`** — 1 linha por `lead_id`: nº de conversas, taxa de fechamento, provedor de e-mail dominante, UF/cidade, perfil de engajamento, persona classificada via LLM (ex.: *pesquisador de preço*, *comprador racional*, *negociador agressivo*, *indeciso*, *comprador rápido*, *refém de concorrente*), sensibilidade a preço, score de intenção de compra (0–100).
- **`gold.agent_performance`** — 1 linha por `agent_id`: conversas atendidas, taxa de fechamento, tempo médio de resposta, mix de outcomes, personas com que mais converte.
- **`gold.competitor_intel`** — menções por concorrente, faixa de preço média citada, taxa de perda para cada um, correlação com região.

Insights a produzir (além dos exemplos óbvios do enunciado, que devem ser evitados como foco principal):
- Taxonomia de **motivos de ghosting** inferida via LLM sobre última janela da conversa.
- Detecção de **objeções recorrentes** (preço, cobertura, franquia, assistência 24h).
- **Momento da conversa onde o lead desengaja** (após cotação, após proposta, etc.).
- **Correlação perfil × outcome** por cluster de persona.

## 7. Agente — Requisitos de Autonomia

O agente é o orquestrador. Não é um notebook que roda análise uma vez.

### 7.1. Capacidades
- **Criar** DAG do pipeline (Bronze → Silver → Gold) programaticamente na primeira execução.
- **Observar** a fonte: detectar crescimento da Bronze (novos `batch_id` ou aumento de linhas/hash do arquivo).
- **Executar incrementalmente**: reprocessa só conversas afetadas; Gold é recalculada apenas para `lead_id`/`conversation_id` impactados.
- **Validar** cada camada com contratos de dados (schema, nulos, ranges, unicidade de chaves, outliers).
- **Auto-corrigir** falhas comuns:
  - schema drift → ajusta parser e registra mudança;
  - regex quebrado após novo padrão de dado → regenera regex com auxílio de LLM e re-testa;
  - particionamento ausente → recria;
  - valores fora de range → quarentena em `bronze.rejected`.
- **Alertar** o operador (log estruturado + stdout/arquivo) quando não for possível auto-corrigir, com diagnóstico e sugestão.
- **Idempotência**: rodar N vezes sobre mesma Bronze produz mesma Gold.

### 7.2. Loop do agente (conceitual)
```
while True:
    state = observe(bronze_source)
    if state.changed:
        plan = agent.plan_incremental(state.delta)
        for step in plan:
            try: execute(step)
            except Failure as f:
                fix = agent.diagnose_and_fix(f)
                if fix.applied: retry(step)
                else: alert(operator, f, fix.suggestion); break
        validate(silver); validate(gold)
    sleep(interval)
```

### 7.3. LLM como ferramenta do agente
- Extração de entidades em texto livre (veículo, concorrente, sinistro).
- Classificação de persona e sentimento.
- Diagnóstico de falha e sugestão de correção de código/regex.
- Cache de chamadas por hash de input para idempotência e custo.

## 8. Requisitos Funcionais

- **RF-01** — Ingestão Bronze idempotente a partir do parquet fonte, preservando schema original.
- **RF-02** — Silver com lead reconciliado por telefone e dados pessoais mascarados.
- **RF-03** — Extração estruturada de entidades do `message_body` (regex + LLM).
- **RF-04** — Gold com as 4 tabelas analíticas descritas em 6.3.
- **RF-05** — Atualização incremental automática da Gold quando Bronze cresce.
- **RF-06** — Agente detecta, classifica e tenta corrigir falhas; gera alerta estruturado quando falha.
- **RF-07** — Logs de execução por `batch_id` com métricas (linhas lidas, rejeitadas, latência por etapa).
- **RF-08** — Quarentena de registros inválidos sem parar o pipeline.
- **RF-09** — CLI única para inicializar, rodar uma vez e rodar em loop.
- **RF-10** — Testes automatizados de cada transformação e do contrato de cada camada.

## 9. Requisitos Não-Funcionais

- **RNF-01** Python puro (≥3.11). Stack sugerida: `polars` ou `duckdb` + `pyarrow`, `pydantic` para contratos, `pytest`, cliente LLM.
- **RNF-02** Diferencial: rodar também em **Databricks free tier** (mesmo código, runner alternativo).
- **RNF-03** Reprodutibilidade: `uv`/`poetry` com lockfile, `.env.example`, `README` com passo-a-passo.
- **RNF-04** Observabilidade: logging estruturado (JSON) e `run_manifest.json` por execução.
- **RNF-05** Custo: cache de LLM e limites configuráveis de tokens/chamadas por batch.
- **RNF-06** Tempo de execução do full load abaixo de 15 min em máquina local razoável.
- **RNF-07** Segurança: sem credenciais em repositório; dados pessoais mascarados antes de sair da Silver.

## 10. Critérios de Aceitação

- [ ] Repositório público no GitHub com README, licença e instruções executáveis.
- [ ] `python -m pipeline init` cria estrutura das 3 camadas.
- [ ] `python -m pipeline run` processa a Bronze existente e popula Silver e Gold.
- [ ] `python -m pipeline watch` mantém pipeline vivo: ao adicionar novas linhas à Bronze, Gold é atualizada sem reprocessar tudo.
- [ ] Injeção proposital de falha (ex.: schema alterado, regex quebrada) dispara ciclo de diagnóstico + correção do agente, documentado em log.
- [ ] Gold contém as 4 tabelas com ao menos 3 insights fora dos exemplos do enunciado.
- [ ] Dados pessoais aparecem mascarados em Silver e Gold com dimensões preservadas.
- [ ] Suite de testes (`pytest`) passa em CI (GitHub Actions).
- [ ] Tempo de full load documentado; tempo de incremental em delta de ≤1k mensagens ≤ 30s.
- [ ] Documento explicando decisões de arquitetura do agente, taxonomia de personas e estratégia de auto-correção.

## 11. Riscos e Mitigações

| Risco | Mitigação |
|---|---|
| Custo de LLM explode em 150k mensagens | Cache determinístico, batching, extração regex antes, amostragem para classificação de persona por lead e não por mensagem |
| Regex falha em padrões novos | Agente regenera regex via LLM com testes unitários de regressão |
| Reconciliação de lead errada por telefone sujo | Normalização de telefone + fallback por similaridade de nome |
| LGPD | Mascaramento determinístico antes de qualquer saída persistida fora da Bronze |
| Loop de auto-correção infinito | Limite de tentativas por falha + escalonamento para alerta humano |

## 12. Entregáveis

1. Repositório GitHub público.
2. Código do pipeline + agente.
3. Suíte de testes e CI.
4. `README.md` com arquitetura, como rodar local e (diferencial) como rodar no Databricks free tier.
5. Este PRD versionado na raiz.
6. Documento de decisões de design do agente (auto-correção, personas, insights).
7. Demonstração reproduzível de falha → auto-correção (script + log).

## 13. Métricas de Sucesso da Entrega

- Avaliador consegue rodar o projeto em menos de 10 minutos a partir do clone.
- Ao injetar uma falha sintética, agente detecta, corrige ou alerta em menos de 2 minutos.
- Gold produz insights acionáveis distintos dos exemplos do enunciado.
- Código passa linter, type-check e testes sem warnings críticos.

## 14. Próximos Passos (Spec-Driven Development)

Este PRD vira entrada do workflow `/tlc-spec-driven`:
1. **Specify** — spec formal por módulo (ingest, silver, gold, agent, observability).
2. **Design** — ADRs para: escolha de engine (polars vs duckdb), contrato de camadas, loop do agente, estratégia de LLM.
3. **Tasks** — quebra atômica por módulo com critérios de verificação.
4. **Execute** — implementação incremental com testes por task.

---

## 15. Mapeamento ao Teste Técnico

### 15.1. Requisitos Obrigatórios (rastreabilidade)

| Requisito do teste | Onde é atendido neste PRD |
|---|---|
| Python puro | RNF-01 |
| AI Agent cria E gerencia pipeline (auto-correção em falha) | §7 (todo) + RF-06 |
| Pipeline vivo: Gold atualiza quando Bronze cresce | §7.1 + RF-05 + critério de aceitação `watch` |
| 3 camadas Bronze → Silver → Gold com transformações claras | §6.1, §6.2, §6.3 |
| Repositório público no GitHub | Cabeçalho + critério de aceitação |
| Mascaramento de dados sensíveis preservando dimensões | §5 (LGPD) + §6.2 + RNF-07 |

### 15.2. Diferenciais (espaço para criatividade) — como serão entregues

| # | Diferencial do teste | Estratégia neste projeto |
|---|---|---|
| 1 | Pipelines em Databricks (free tier atende) | RNF-02: runner alternativo, mesmo código roda local e em job Databricks. Documentado no README. |
| 2 | Tipos de classificação e insights na Gold | §6.3 entrega 4 tabelas + 4 insights não-óbvios (ghosting, objeções, momento de desengajamento, perfil×outcome). |
| 3 | Abordagem de limpeza e transformação | §6.2: dedup `sent`/`delivered`, normalização de nome via `sender_phone`, regex+LLM para entidades, parse tipado de `metadata`, tratamento de transcrição ruidosa, flag `has_content`. |
| 4 | Criatividade em agrupamentos e segmentações | `gold.lead_profile` com personas via LLM (pesquisador de preço, refém de concorrente, comprador rápido, etc.) + sensibilidade a preço + score de intenção 0–100. |
| 5 | Robustez do mecanismo de auto-correção | §7.1 (auto-corrigir schema drift, regex quebrado, partição ausente, quarentena) + §7.2 loop conceitual + §11 (limite de tentativas, escalonamento). Demo reproduzível listada em §12. |
| 6 | Fugir dos exemplos de insights do enunciado | §6.3 declara explicitamente que os exemplos "óbvios" devem ser evitados como foco principal. Entregáveis priorizam taxonomia de ghosting, objeções, perfil de desengajamento, correlação persona×outcome. |

### 15.3. Critérios de Avaliação do Teste — como o projeto responde

| Eixo de avaliação | Resposta deste PRD |
|---|---|
| **Engenharia de dados e criatividade analítica** — pipeline robusto, camadas claras, distinção entre análise pontual e infraestrutura | §1 reforça distinção; §6 separa contratos por camada; §6.3 + §15.2 #2,#4,#6 entregam insights criativos. |
| **Capacidade de construir agentes** — autonomia, auto-correção, processo contínuo | §7 inteiro: capacidades, loop, LLM como ferramenta; §10 inclui demo de injeção de falha; §12 entrega script reproduzível. |
| **Qualidade de código** — limpo, estruturado, documentado, boas práticas | RNF-01/03/04, RF-10 (testes), critério de aceitação inclui CI verde, lint, type-check; §12 entrega README + documento de decisões. |

### 15.4. Insights "óbvios" do enunciado — tratamento

O enunciado lista como exemplos: provedores de e-mail mais usados, classificação de personas, segmentação por audiência, análise de sentimento. Estes serão entregues **como subproduto** (presentes em `gold.lead_profile`), mas **não são o foco principal** da Gold. O foco principal são os insights listados em §6.3 (ghosting, objeções, desengajamento, correlação persona×outcome) — alinhado ao diferencial 6.

---

## 16. Decisões Arquiteturais Preliminares (ADR-stubs)

Decisões abaixo são **preliminares**: serão promovidas a ADRs formais na fase **Design** do `/tlc-spec-driven`. Servem como ponto de partida com rationale explícito; podem ser revertidas se a fase Design provar contrário.

### ADR-001 — Engine de transformação: **Polars**
- **Decisão:** Polars (lazy API) como engine principal para Bronze→Silver→Gold.
- **Por quê:** lazy evaluation reduz materialização desnecessária em pipeline de 150k linhas; ~2–5x mais rápido que pandas em joins/groupby; API expressiva e tipada; integração nativa com PyArrow (parquet); footprint de memória menor que DuckDB para datasets <1 GB.
- **Trade-off:** DuckDB ganha em queries SQL ad-hoc e em interoperabilidade com BI. Pipeline aqui é cadeia de transformações deterministas, não exploração SQL → Polars vence.
- **Reversão se:** Gold precisar virar serviço SQL consultável externamente, ou volume crescer >10x → migrar para DuckDB (mesma origem PyArrow, custo de migração baixo).

### ADR-002 — Provedor de LLM: **Anthropic Claude Haiku 4.5**
- **Decisão:** Anthropic Claude Haiku 4.5 como modelo padrão; Sonnet 4.6 como fallback para casos ambíguos detectados pela validação.
- **Por quê:** melhor relação custo/qualidade para extração estruturada em PT-BR; suporta JSON mode determinístico; prompt caching nativo (>5min TTL) reduz custo em batch repetido; SDK oficial Python estável.
- **Custo estimado (full load):** 150k msgs × ~600 tokens in / ~150 tokens out × preço Haiku → ~**$0.30** sem cache. Com cache de prompt + amostragem de persona por lead (~15k chamadas, não por msg) + extração regex-first → **<$0.10 por full load**.
- **Trade-off:** vendor lock-in. Mitigado por interface `LLMClient` abstrata (`extract(text, schema) -> dict`, `classify(text, taxonomy) -> str`, `diagnose(error_ctx) -> Fix`). Trocar provedor = trocar 1 implementação.
- **Reversão se:** custo explodir, latência inviabilizar incremental, ou cliente exigir on-prem → trocar para modelo local (Qwen3-Coder, Llama 3.3) via mesma interface.

### ADR-003 — State store do agente: **SQLite (`state/manifest.db`)**
- **Decisão:** SQLite single-file para manifesto de runs, hash de batches, registro de tentativas de auto-correção, fila de quarentena.
- **Por quê:** zero infraestrutura, idempotência garantida por PRIMARY KEY, queryável por SQL, embarcado no Python stdlib; DuckDB consegue ler arquivos SQLite se engine mudar.
- **Trade-off:** single-writer; não escala para múltiplos workers paralelos. Não é requisito do teste.
- **Reversão se:** múltiplos agentes concorrentes → trocar para Postgres/DuckDB.

### ADR-004 — Loop do agente: **thread única + intervalo configurável**
- **Decisão:** loop síncrono com `time.sleep(interval)` configurável (default 60s), executável também em modo single-shot via CLI.
- **Por quê:** Python puro, sem cron/Airflow/Prefect; alinhado ao requisito de simplicidade reproduzível.
- **Trade-off:** sem paralelismo entre runs. Aceitável: cada run é incremental e curto.
- **Reversão se:** projeto for promovido a produção → trocar para Prefect/Dagster/Databricks Jobs.

### ADR-005 — Observabilidade: **structlog JSON + manifesto por run**
- **Decisão:** logging estruturado JSON via `structlog` para stdout e arquivo `logs/run_{batch_id}.jsonl`. Manifesto por execução em `state/runs/{batch_id}.json` com métricas (linhas lidas, rejeitadas, latência por etapa, chamadas LLM, custo estimado).
- **Por quê:** parseável por qualquer ferramenta downstream (Grafana, Datadog, simples `jq`); sem dependência externa pesada.
- **Trade-off:** sem tracing distribuído. Não é necessário para single-process.

### ADR-006 — Auto-correção: **retry budget = 3, escalonamento determinístico**
- **Decisão:** cada falha tem orçamento máximo de 3 tentativas de auto-correção pelo agente. Após esgotar, gera alerta estruturado + para a etapa, mas não derruba o pipeline (próximas etapas independentes continuam).
- **Por quê:** evita loop infinito (risco listado em §11); 3 tentativas cobrem 90%+ dos casos transientes; mantém o pipeline vivo mesmo com falha localizada.
- **Trade-off:** falha persistente em 1 partição pode atrasar Gold dela até intervenção humana. Aceitável — alternativa é falha silenciosa, pior.

---

## 17. Taxonomia Formal de Personas

Cada lead recebe **uma** persona dominante na `gold.lead_profile` baseada em critérios mensuráveis. LLM classifica usando os sinais textuais como evidência; critérios numéricos servem como guard-rails (LLM não pode contradizer critérios duros).

| Persona | Critérios mensuráveis (duros) | Sinais textuais (LLM detecta) |
|---|---|---|
| **Pesquisador de preço** | ≥5 msgs mencionando valor; menciona ≥2 concorrentes; taxa de fechamento histórica <20% | "quanto fica", "tô vendo na Porto", "Azul me deu X" |
| **Comprador racional** | Pergunta sobre cobertura/franquia/assistência; tempo de resposta médio <5min; ≥1 dado pessoal fornecido | "qual a franquia", "cobre vidros", "tem carro reserva" |
| **Negociador agressivo** | ≥2 pedidos de desconto; menciona sair pelo menos 1x | "tá caro", "consigo melhor em outro lugar", "me dá um desconto" |
| **Indeciso** | Conversa >15 msgs sem outcome; ≥1 gap >24h entre msgs; múltiplas reaberturas | "vou pensar", "depois te aviso", "preciso ver com a esposa" |
| **Comprador rápido** | ≤10 msgs até `venda_fechada`; tempo total <2h; ≤1 objeção | "fechou", "manda boleto", "tá bom assim" |
| **Refém de concorrente** | Cita valor pago atualmente; queixa explícita de seguradora atual; menciona renovação | "Porto subiu 30%", "tô fugindo da SulAmérica", "minha renovação veio absurda" |
| **Bouncer** | 2–4 msgs total; sem resposta nas últimas 48h após cotação | (silêncio após proposta) |
| **Caçador de informação** | Não fornece nenhum dado pessoal/veículo; pergunta genérica | "só queria saber", "é uma dúvida geral", "pra um amigo" |

### 17.1. Score de intenção de compra (0–100)

Composição linear ponderada, calculada deterministicamente em Python (não LLM):

```
score =  25 * fornecimento_dados_pessoais (0-1)
      +  20 * velocidade_resposta_normalizada (0-1)
      +  15 * presença_termo_fechamento (0-1)
      +  15 * número_perguntas_técnicas_normalizado (0-1)
      +  10 * ausência_palavras_evasivas (0-1)
      +  10 * coerência_outcome_histórico_persona (0-1)
      +   5 * janela_horária_comercial (0-1)
```

Persona é categórica, score é contínuo. Os dois juntos permitem segmentação dupla (ex.: "Indeciso com score >70" = leads de alto valor que precisam de follow-up; "Pesquisador de preço com score <30" = não vale insistir).

---

## 18. Exemplos de Prompts LLM

Prompts canônicos abaixo são **versões iniciais**; serão refinados na fase Tasks com testes de regressão por amostra anotada.

### 18.1. Extração de entidades estruturadas

```
Você extrai entidades de mensagens de WhatsApp entre vendedor de seguro auto e lead.
Retorne APENAS JSON válido conforme o schema. Use null para campos ausentes.

Schema:
{
  "veiculo": {"marca": str|null, "modelo": str|null, "ano": int|null},
  "concorrente_mencionado": str|null,    // ex: "Porto Seguro", "Azul"
  "valor_pago_atual_brl": float|null,
  "sinistro_historico": bool|null,
  "intencao_compra": "alta"|"media"|"baixa"|null
}

Mensagem:
"""
{message_body}
"""

JSON:
```

### 18.2. Classificação de persona (por lead, agregando conversa)

```
Você classifica leads de seguro auto em UMA persona dominante.

Personas válidas (escolha exatamente uma):
- pesquisador_de_preco
- comprador_racional
- negociador_agressivo
- indeciso
- comprador_rapido
- refem_de_concorrente
- bouncer
- cacador_de_informacao

Regras duras (NÃO podem ser violadas):
- Se conversa tem ≤4 msgs E sem outcome → "bouncer".
- Se outcome="venda_fechada" E msgs ≤10 → "comprador_rapido".
- Se lead nunca forneceu dado pessoal → "cacador_de_informacao".

Conversa (mensagens em ordem cronológica):
{conversation_text}

Métricas pré-computadas:
- num_msgs: {n}
- outcome: {outcome}
- mencionou_concorrente: {bool}
- forneceu_dado_pessoal: {bool}

Responda APENAS com a chave da persona, sem explicação.
Persona:
```

### 18.3. Diagnóstico de falha + sugestão de correção

```
Você é um agente de auto-correção de pipeline de dados em Polars.

Falha detectada na etapa: {step_name}
Tipo: {error_type}
Mensagem: {error_message}
Stack trace (últimas 20 linhas):
{stack_trace}

Contrato esperado da etapa:
{step_contract}

Amostra dos dados de entrada (5 linhas):
{sample_input}

Tarefa:
1. Identifique a causa raiz em 1 frase.
2. Classifique: SCHEMA_DRIFT | REGEX_BREAK | NULL_OVERFLOW | TYPE_MISMATCH | UNKNOWN.
3. Proponha patch em Python (Polars) que aplica a correção. Patch deve ser idempotente e não-destrutivo.
4. Defina critério de teste: SQL/Polars expression que retorna True se o patch funcionou.

Responda APENAS JSON:
{
  "causa_raiz": str,
  "categoria": str,
  "patch_python": str,
  "teste_validacao": str,
  "confianca": 0.0-1.0
}
```

### 18.4. Cache e governança

- Todas as chamadas LLM passam por `LLMClient.cached_call(prompt, schema)` → hash SHA-256(prompt + schema_version) como chave em SQLite.
- Cache hit = custo zero, latência <5ms.
- Versão do prompt em constante `PROMPT_VERSION`; bump invalida cache da família afetada.
- Limite global por batch (configurável): `MAX_LLM_CALLS_PER_BATCH=5000`, `MAX_USD_PER_BATCH=2.00`. Estourou → agente pausa e alerta.
