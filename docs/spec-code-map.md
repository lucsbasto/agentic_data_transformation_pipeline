# Spec → Code Map

For every line of `Teste Técnico de Data & AI Engineering.md`, the file (and where possible the line) where the implementation lives. Use this to answer "where in the code do I see X?" without thinking.

The numbers reference the spec headings directly. Open the spec file in one tab, this map in another.

---

## Mandatory requirements

### 1. "Python puro: pipeline implementado em código Python"

| Element | File |
|---|---|
| Package layout | `src/pipeline/` (single package) |
| Entry point | `src/pipeline/__main__.py:50` (`if __name__ == "__main__": main()`) |
| Sub-commands wiring | `src/pipeline/__main__.py:31-34` (Click `add_command`) |
| Build config (Python only, no bash glue) | `pyproject.toml`, `uv.lock` |

### 2. "AI Agent: cria E gerencia o pipeline (auto-correção em caso de falha)"

| Element | File |
|---|---|
| Agent loop entry | `src/pipeline/agent/loop.py:59` (`run_once`) |
| Continuous mode | `src/pipeline/agent/loop.py:174` (`run_forever`) |
| Observe step | `src/pipeline/agent/observer.py` (search `def scan`) |
| Plan step | `src/pipeline/agent/planner.py` (search `def plan`) |
| Execute + retry/recovery | `src/pipeline/agent/executor.py` (search `run_with_recovery`) |
| Error classification | `src/pipeline/agent/diagnoser.py` (deterministic patterns + LLM diagnose with cap) |
| Fix dispatcher | `src/pipeline/agent/runners.py` (search `make_fix_builder`) |
| Escalation | `src/pipeline/agent/escalator.py` (writes `logs/agent-escalations.jsonl`) |
| Retry budget | `src/pipeline/agent/executor.py` (`DEFAULT_RETRY_BUDGET = 3`) |
| Diagnose budget | `src/pipeline/cli/agent.py:46` (`DEFAULT_DIAGNOSE_BUDGET = 10`) |
| Lock (single-writer guard) | `src/pipeline/agent/lock.py` |

### 3. "Pipeline vivo: atualização automática da Gold quando a fonte cresce"

| Element | File |
|---|---|
| Loop interval default 60s | `src/pipeline/agent/loop.py:50` (`DEFAULT_LOOP_INTERVAL_S`) |
| Continuous loop body | `src/pipeline/agent/loop.py:174-219` (`run_forever`) |
| Observer detects new batches | `src/pipeline/agent/observer.py` (filesystem walk + manifest join) |
| Manifest skips COMPLETED layers | `src/pipeline/agent/planner.py` |
| Idempotent atomic writes | `src/pipeline/gold/writer.py`, `src/pipeline/silver/writer.py` (temp file + `os.rename`) |
| CLI for daemon mode | `src/pipeline/cli/agent.py:213` (`@agent.command("run-forever")`) |
| Docker compose for prod-mode | `compose.yaml`, `Dockerfile` |

### 4. "3 camadas: Bronze → Silver → Gold com transformações claras"

| Layer | Driver file | Schema file | Output |
|---|---|---|---|
| Bronze | `src/pipeline/ingest/ingest.py` | `src/pipeline/schemas/bronze.py` | `data/bronze/batch_id=<id>/part-*.parquet` |
| Silver | `src/pipeline/silver/transform.py` | `src/pipeline/schemas/silver.py` | `data/silver/batch_id=<id>/part-*.parquet` |
| Gold | `src/pipeline/gold/transform.py` (`transform_gold`) | `src/pipeline/schemas/gold.py` | 4 parquets + 1 JSON under `data/gold/<table>/batch_id=<id>/...` |
| Schema drift guards | `src/pipeline/schemas/gold.py:assert_lead_profile_schema` (line ~257) and siblings | | (raises `SchemaDriftError` on mismatch) |

### 5. "Repositório público no GitHub"

| Element | Where |
|---|---|
| Remote URL | `git@github.com:lucsbasto/agentic_data_transformation_pipeline.git` |
| Confirm | `git remote -v` from repo root |
| README explains how to run | `README.md` |
| Spec-driven feature lanes | `.specs/features/F1/`, `F2/`, `F3/`, `F4/`, `F5/`, `F7/`, `FIC/` |

---

## Bronze layer requirements

### "Todas as conversas transacionais no formato original"

| Element | File |
|---|---|
| Reader | `src/pipeline/ingest/ingest.py` (`pl.scan_parquet`) |
| Content-hash dedup | `src/pipeline/ingest/ingest.py` (search `content_hash`) |
| Schema | `src/pipeline/schemas/bronze.py` (matches source columns 1:1) |
| Writer | `src/pipeline/ingest/ingest.py` (atomic rename) |

### "Coluna de mensagem com dados não estruturados"

The `message_body` column is preserved unchanged. See `src/pipeline/schemas/bronze.py` for declared type.

---

## Silver layer requirements

### "Dados limpos e organizados por usuário/lead"

| Element | File |
|---|---|
| Per-message dedup | `src/pipeline/silver/dedup.py` |
| Type normalization (timestamp, casing) | `src/pipeline/silver/normalize.py` |
| Per-lead reconciliation | `src/pipeline/silver/reconcile.py` |
| Audio transcript handling | `src/pipeline/silver/audio.py` |
| Quarantine for malformed rows | `src/pipeline/silver/quarantine.py` |
| Atomic writer | `src/pipeline/silver/writer.py` |

### "Extração de informações relevantes (e-mails, dados de contato)"

| Entity | File:Line |
|---|---|
| Email domain regex | `src/pipeline/silver/regex.py` (search `EMAIL_RE`) |
| Email extractor (Polars expr) | `src/pipeline/silver/extract.py:97` (`extract_email_domain`) and `:119` (`extract_email_domain_expr`) |
| CPF regex + flag | `src/pipeline/silver/regex.py` + `src/pipeline/silver/extract.py` (search `has_cpf`) |
| Phone mention regex | `src/pipeline/silver/extract.py` (search `has_phone_mention`) |
| Plate (Brazilian car plate) regex | `src/pipeline/silver/regex.py` (search `PLATE_RE`) |
| CEP prefix | `src/pipeline/silver/extract.py` (search `cep_prefix`) |
| Vehicle marca/modelo/ano | `src/pipeline/silver/extract.py` (search `veiculo_`) |
| Competitor mention | `src/pipeline/silver/extract.py` (search `concorrente_mencionado`) |
| LLM extraction (regex fallback) | `src/pipeline/silver/llm_extract.py` |

### "Dados sensíveis (nomes, e-mails) mascarados com as mesmas dimensões"

| PII class | File:Line |
|---|---|
| Email mask | `src/pipeline/silver/pii.py:99` (`_mask_email`) — keeps first letter + first letter of host + full TLD |
| CPF mask | `src/pipeline/silver/pii.py:111` (`_mask_cpf`) — fixed template `***.***.***-**` |
| Phone mask | `src/pipeline/silver/pii.py:130` (`_mask_phone`) — keeps last 2 digits |
| CEP mask | `src/pipeline/silver/pii.py` (search `_mask_cep`) — fixed template `*****-***` |
| Master masker (applied to `message_body`) | `src/pipeline/silver/pii.py:193-199` (subn calls in priority order) |
| Phone-only convenience | `src/pipeline/silver/pii.py:207` (`mask_phone_only`) |
| Tests for length preservation | `tests/unit/test_silver_pii.py` |

---

## Gold layer requirements

### "Tabela analítica com classificações e agrupamentos"

| Output table | Builder file |
|---|---|
| `gold.conversation_scores` | `src/pipeline/gold/conversation_scores.py` (`build_conversation_scores`) |
| `gold.lead_profile` | `src/pipeline/gold/lead_profile.py` (`build_lead_profile_skeleton`) |
| `gold.agent_performance` | `src/pipeline/gold/agent_performance.py` |
| `gold.competitor_intel` | `src/pipeline/gold/competitor_intel.py` |
| Insights JSON | `src/pipeline/gold/insights.py` (`build_insights`) |
| Orchestrator | `src/pipeline/gold/transform.py:127` (`transform_gold`) |
| Atomic writers | `src/pipeline/gold/writer.py` |

### "Criação de audiências e personas"

| Element | File:Line |
|---|---|
| 8 personas (closed enum) | `src/pipeline/schemas/gold.py:54-66` (`PERSONA_VALUES`) |
| Per-lead aggregator | `src/pipeline/gold/persona.py:201` (`aggregate_leads`) |
| Hard rules (R1/R2/R3) | `src/pipeline/gold/persona.py:153` (`evaluate_rules`) |
| LLM classifier (combined prompt) | `src/pipeline/gold/persona.py` (search `_classify_with_llm`) |
| LLM concurrency lane (semaphore + budget) | `src/pipeline/gold/concurrency.py` |
| LLM client (DashScope via Anthropic SDK) | `src/pipeline/llm/client.py` |
| Response cache | `src/pipeline/llm/cache.py` (SHA-256 key) |

### "Esta camada atualiza automaticamente quando novos dados chegam na Bronze"

Same wiring as Mandatory §3 — the agent loop's continuous mode watches Bronze and re-runs Silver+Gold for new batches without manual intervention. See `src/pipeline/agent/loop.py:174` (`run_forever`) plus the observer/planner pair.

### "Provedores de e-mail mais usados pelos leads" (insight example)

| Element | File:Line |
|---|---|
| Email domain extracted in Silver | `src/pipeline/silver/extract.py:119` (`extract_email_domain_expr`) |
| Aggregated in Gold | `src/pipeline/gold/lead_profile.py` (search `dominant_email_domain`) |
| Surfaced in insights JSON | `src/pipeline/gold/insights.py` (search `email_provider`) |

### "Classificação de personas/perfis de usuários"

Already covered above (`PERSONA_VALUES` + `evaluate_rules` + LLM classifier).

### "Segmentação por audiência"

| Element | File:Line |
|---|---|
| Engagement profile bucket (hot/warm/cold) | `src/pipeline/gold/lead_profile.py:148` (`_add_engagement_profile`) |
| Engagement enum | `src/pipeline/schemas/gold.py:69` (`ENGAGEMENT_PROFILE_VALUES`) |
| Price sensitivity (low/medium/high) | `src/pipeline/gold/transform.py` (search `price_sensitivity_expr`) |
| Intent score | `src/pipeline/gold/intent_score.py` (`compute_intent_score`) |

### "Análise de sentimento do cliente"

| Element | File:Line |
|---|---|
| Sentiment enum (4 labels) | `src/pipeline/schemas/gold.py:75-86` (`SENTIMENT_VALUES`) |
| Sentiment columns in lead_profile | `src/pipeline/schemas/gold.py:144-145` (`sentiment`, `sentiment_confidence`) |
| Hard rules SR1/SR2/SR3 | `src/pipeline/gold/sentiment.py:70` (`evaluate_sentiment_rules`) |
| Combined LLM prompt (persona + sentiment) | `src/pipeline/gold/persona.py:333` (`SYSTEM_PROMPT` v3) |
| JSON parser (markdown-fence tolerant) | `src/pipeline/gold/persona.py` (search `parse_classifier_reply`) |
| Telemetry | `src/pipeline/gold/transform.py` (search `_sentiment_telemetry`) |
| Tests | `tests/unit/test_gold_sentiment.py` (25 tests) |
| Backfill runbook | `docs/runbooks/f5-sentiment-backfill.md` |
| Design doc | `.specs/features/F5/DESIGN.md` |

---

## Differential requirements (creativity space)

### "Destaque para pipelines que utilizam o Databricks"

**Skipped** — optional. Rationale in `docs/learn-design-decisions.md` Part 9.

The Gold transform is engine-portable: `pl.LazyFrame` expressions map cleanly to Spark dataframes if a future migration is needed.

### "Tipos de classificação e insights gerados"

| Insight | File |
|---|---|
| Email provider distribution | `src/pipeline/gold/insights.py` |
| Top objection categories | `src/pipeline/gold/insights.py` (search `objection`) |
| Ghosting taxonomy | `src/pipeline/gold/insights.py` (search `ghosting`) |
| Competitor intel | `src/pipeline/gold/competitor_intel.py` |
| Conversation scoring (multi-dim) | `src/pipeline/gold/conversation_scores.py` |

### "Abordagem de limpeza e transformação"

Bronze→Silver chain: dedup → normalize → extract → mask → reconcile. Each step is its own module under `src/pipeline/silver/` so the chain is readable and testable.

### "Criatividade nos agrupamentos e segmentações"

| Element | File |
|---|---|
| 8-label persona taxonomy (PRD §18.2) | `src/pipeline/schemas/gold.py:54` |
| Hot/warm/cold engagement | `src/pipeline/gold/lead_profile.py:148` |
| Price sensitivity by haggling-phrase count | `src/pipeline/gold/transform.py` |
| Intent score (9-input weighted) | `src/pipeline/gold/intent_score.py` |
| Sentiment 4-label (incl. "misto") | `src/pipeline/gold/sentiment.py` |
| Per-conversation scoring (response time, off-hours, competitor) | `src/pipeline/gold/conversation_scores.py` |

### "Robustez do mecanismo de auto-correção"

| Element | File:Line |
|---|---|
| 2-stage error classification (regex + LLM diagnose with cap) | `src/pipeline/agent/diagnoser.py` |
| 9 deterministic `ErrorKind`s | `src/pipeline/agent/types.py` (search `class ErrorKind`) |
| Fix recipes per kind | `src/pipeline/agent/runners.py` (search `make_fix_builder`) |
| Bounded retry (3 attempts) | `src/pipeline/agent/executor.py` (`DEFAULT_RETRY_BUDGET`) |
| Bounded diagnose LLM (10 calls per `run_once`) | `src/pipeline/cli/agent.py:46` |
| Escalation (JSONL + manifest flip) | `src/pipeline/agent/escalator.py` |
| Fault injection campaign (FIC) test lane | `.specs/features/FIC/` + `tests/integration/test_fic_*.py` |
| F7 secret redaction in logs | `src/pipeline/logging.py` (structlog processor) |

### "É recomendado fugir dos exemplos de insights"

Insights present in this repo that go beyond the spec's example list:
- 8-label persona taxonomy (vs simple "personas" example).
- Hot/warm/cold engagement bucketing.
- Price sensitivity scoring driven by haggling-phrase count.
- 9-input intent score.
- Per-agent performance with `top_persona_converted` per agent.
- Competitor intel with average quoted price + loss rate.
- Conversation health scores (off-hours rate, response latency).

---

## Avaliação criteria

### "Engenharia de dados e criatividade analítica"

| Criterion | Evidence |
|---|---|
| Robust pipeline with clear layer transformations | Medallion architecture, schema drift guards per layer, atomic writers |
| Difference between pontual analysis vs infrastructure | Agent loop + manifest + idempotent re-runs (this is *not* a one-off script) |
| Insights extracted | See "Tipos de classificação e insights" table above |
| Classification + segmentation | See "Criatividade nos agrupamentos" table above |

### "Capacidade de construir agentes"

| Criterion | Evidence |
|---|---|
| Agent autonomy | `agent/loop.py:run_forever` — no human in the loop in steady state |
| Self-correction | `agent/executor.py:run_with_recovery` — classify/fix/retry/escalate |
| Manages pipeline as continuous process | Manifest tracks per-batch per-layer status; future runs skip COMPLETED |

### "Qualidade de código"

| Criterion | Evidence |
|---|---|
| Clean, well-structured | Modules per concern; Polars expressions kept pure; functional core / imperative shell |
| Documented | Module docstrings; `.specs/features/` per feature; `docs/runbooks/`; learn-* doc series |
| Best practices | mypy strict; ruff; pytest 870+ unit + 9 integration; structlog; pydantic Settings; closed-set enums; atomic writes; dependency injection |

---

## Quick lookup — by question

> "Where is the masking?"
`src/pipeline/silver/pii.py` lines 99 (email), 111 (CPF), 130 (phone).

> "Where is sentiment?"
`src/pipeline/gold/sentiment.py` (rules) and `src/pipeline/gold/persona.py:333` (combined LLM prompt).

> "Where is the agent's main loop?"
`src/pipeline/agent/loop.py:59` (`run_once`) and `:174` (`run_forever`).

> "Where is retry/recovery?"
`src/pipeline/agent/executor.py` (search `run_with_recovery`).

> "Where is the LLM cache?"
`src/pipeline/llm/cache.py` (SQLite, SHA-256 keys).

> "Where are the tests?"
`tests/unit/` (870+), `tests/integration/` (9 e2e), `tests/property/` (Hypothesis).

> "How do I run it end-to-end?"
`uv run python -m pipeline agent run-once`. See `docs/learn-agent-flow.md` for the full call chain.

> "How do I add a feature?"
Create `.specs/features/Fn/DESIGN.md` + `tasks.md`, mirror the F5 pattern. See `.specs/features/F5/` as a template.

> "Where is the schema drift guard?"
`src/pipeline/schemas/gold.py:_assert_schema_match` and the per-table `assert_*_schema` wrappers (e.g. `assert_lead_profile_schema` line 242).

> "How do I read a Gold parquet?"
```python
import polars as pl
pl.read_parquet('data/gold/lead_profile/batch_id=<id>/part-*.parquet')
```

> "Where do escalations go?"
`logs/agent-escalations.jsonl` plus a row in `state/manifest.db` flipped to `ESCALATED`.
