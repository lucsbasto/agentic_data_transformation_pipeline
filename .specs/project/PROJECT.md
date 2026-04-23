# PROJECT — Pipeline de Transformação Agêntica de Dados

## Visão

Agente autônomo em Python que constrói, opera e mantém um pipeline de dados em arquitetura medalhão (Bronze → Silver → Gold) sobre conversas transacionais de WhatsApp de uma operação de venda de seguro auto.

O agente **não é uma análise pontual**. É infraestrutura de dados persistente, equivalente a um job orquestrado em Databricks: existe, é versionado, observável, e mantém a Gold viva conforme novos dados chegam à Bronze.

## Objetivos de longo prazo

1. **Pipeline em código** — 3 camadas em Python puro, contratos de dados explícitos por camada.
2. **Autonomia operacional** — detecção, classificação e auto-correção de falhas sem intervenção humana para casos comuns.
3. **Pipeline vivo** — Gold atualiza automaticamente quando Bronze cresce, sem reprocessar tudo.
4. **Insights criativos** — Gold entrega análises não-óbvias (taxonomia de ghosting, objeções, momentos de desengajamento, correlação persona×outcome) além dos exemplos do enunciado.
5. **Reproduzibilidade total** — `git clone` → `uv sync` → `python -m pipeline run` em <10min.

## Princípios de design

- **Determinismo onde possível, LLM onde necessário.** Regex e regras antes de chamar modelo.
- **Idempotência absoluta.** Rodar N vezes sobre mesma Bronze produz mesma Gold.
- **Custo controlado.** Cache de LLM por hash, limites por batch, amostragem por lead (não por mensagem).
- **Falha visível, nunca silenciosa.** Quarentena para registros inválidos, alerta estruturado quando auto-correção esgota tentativas.
- **Guard-rails > confiança no LLM.** Regras duras filtram alucinação antes de output ir pra Silver/Gold.

## Restrições

- Python ≥3.11.
- Sem streaming. Atualização em lotes disparada por crescimento da fonte.
- Sem treinamento de ML do zero. LLMs e bibliotecas prontas permitidas.
- Sem dashboards/BI. Saída final é Gold parquet/SQLite.
- LGPD: dados pessoais mascarados antes de sair da Bronze, dimensões originais preservadas.

## Stakeholders

- **Avaliador técnico** (primário) — valida engenharia de dados, autonomia do agente, qualidade de código.
- **Engenheiro de dados / Analista** (persona futura) — consome Gold para segmentação comercial.
- **Operador do pipeline** — recebe alertas quando agente não auto-corrige.

## Origem

PRD canônico em `/PRD.md` (raiz do projeto). Teste técnico de origem em `/Teste Técnico de Data & AI Engineering.md`.
