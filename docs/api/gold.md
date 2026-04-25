# Gold Module API

This module contains the Silver -> Gold orchestrator that aggregates per-lead profiles, classifies personas via LLM, scores conversations and agent performance, and produces four analytical parquet tables plus insights JSON.

For a walkthrough of the complete Gold flow including CLI entry points, see [gold-flow.md](../gold-flow.md).

## Types

### `PersonaClassifier`

Type alias for the persona classification callable.

```python
PersonaClassifier = Callable[[list[LeadAggregate], datetime], dict[str, PersonaResult]]
```

Receives the aggregated leads and the `batch_latest_timestamp` anchor (for R1 staleness check) and returns a `lead_id -> PersonaResult` map covering every input aggregate.

Tests inject a synchronous fake; production wires `pipeline.gold.concurrency.classify_all`. The classifier is dependency-injected so unit tests avoid spinning up the real LLM thread pool.

## Entrypoint

### `transform_gold(silver_lf, *, batch_id, gold_root, persona_classifier=None, settings=None) -> GoldTransformResult`

Run the Gold lane end-to-end for one Silver batch.

**Parameters:**
- `silver_lf: pl.LazyFrame` — Input frame with schema matching `pipeline.schemas.silver.SILVER_SCHEMA`
- `batch_id: str` — Batch identifier for manifest and output filenames
- `gold_root: Path` — Root directory where all four parquet tables and insights JSON are written
- `persona_classifier: PersonaClassifier | None` — Callable for LLM-based persona classification. Pass `None` for production (uses default `classify_all` implementation). Tests pass a synchronous fake to avoid LLM calls
- `settings: Settings | None` — Optional pipeline settings; uses defaults if `None`

**Returns:** `GoldTransformResult` dataclass carrying output file paths and row counts.

**Sequence:**
1. Cache the input Silver LazyFrame
2. Build `conversation_scores` table from Silver aggregations
3. Build initial `lead_profile` skeleton (pre-LLM)
4. Aggregate Silver into per-lead dimensions; materialize list of `LeadAggregate` objects
5. Run persona classification lane on the aggregated leads via `persona_classifier`
6. Compute `intent_score` from persona + lead features
7. Fill `lead_profile` with persona and intent results
8. Rebuild `agent_performance` with populated `lead_personas` frame so `top_persona_converted` reflects LLM output
9. Build `competitor_intel` table from aggregations
10. Build insights JSON summary
11. Persist all outputs via atomic writers; return result carrying paths and row counts

**Side effects:** Writes four parquet files and one insights JSON file to `gold_root`. No side effects on input frame or external state.

## Result Type

### `GoldTransformResult`

Outcome of one Gold transform run.

**Fields:**
- `batch_id: str` — Batch identifier (for logging/manifest)
- `conversation_scores_path: Path` — Parquet file path
- `lead_profile_path: Path` — Parquet file path
- `agent_performance_path: Path` — Parquet file path
- `competitor_intel_path: Path` — Parquet file path
- `insights_path: Path` — JSON file path
- `conversation_scores_rows: int` — Row count in conversation_scores table
- `lead_profile_rows: int` — Row count in lead_profile table
- `agent_performance_rows: int` — Row count in agent_performance table
- `competitor_intel_rows: int` — Row count in competitor_intel table

**Property:**
- `rows_out: int` — Sum of all four table row counts (for manifest F3-RF-13 reporting). Insights JSON is not counted.

## Output Schemas

### Conversation Scores: `GOLD_CONVERSATION_SCORES_SCHEMA`

One row per unique conversation in the Silver batch.

See `pipeline.schemas.gold.GOLD_CONVERSATION_SCORES_SCHEMA` for column details.

### Lead Profile: `GOLD_LEAD_PROFILE_SCHEMA`

One row per unique lead (identified by `lead_id`) in the Silver batch.

Carries aggregated conversation counts, dominant metadata (email domain, state), and the LLM-classified persona plus intent score (PRD §17, §17.1).

See `pipeline.schemas.gold.GOLD_LEAD_PROFILE_SCHEMA` for column details.

### Agent Performance: `GOLD_AGENT_PERFORMANCE_SCHEMA`

One row per unique agent in the Silver batch.

Aggregates conversation counts, conversion metrics, and outcome distribution. Rebuilt after persona classification so `top_persona_converted` reflects LLM output (Design §7.1).

See `pipeline.schemas.gold.GOLD_AGENT_PERFORMANCE_SCHEMA` for column details.

### Competitor Intel: `GOLD_COMPETITOR_INTEL_SCHEMA`

One row per normalized competitor mention in the Silver batch.

Aggregates lead counts, conversation counts, and outcome distribution for each competitor.

See `pipeline.schemas.gold.GOLD_COMPETITOR_INTEL_SCHEMA` for column details.

### Insights JSON

One file per batch carrying aggregate statistics and metadata:
- Total leads, conversations, agents
- Persona distribution
- Price sensitivity distribution
- Top competitors by mention count
- Batch timestamp and wall clock

## Enums

**PERSONA_VALUES** (8 labels, PRD §18.2)
- `pesquisador_de_preco`
- `comprador_racional`
- `negociador_agressivo`
- `indeciso`
- `comprador_rapido`
- `refem_de_concorrente`
- `bouncer`
- `cacador_de_informacao`

**PRICE_SENSITIVITY_VALUES**
- `low`
- `medium`
- `high`

**ENGAGEMENT_PROFILE_VALUES**
- `hot`
- `warm`
- `cold`

## Helper Functions

The following builders are called internally by `transform_gold` but are re-exported for unit testing and custom analysis:

**`build_conversation_scores(silver_lf) -> LazyFrame`**

Build the conversation_scores table using phrase-matching rules and conversation aggregations.

**`build_lead_profile_skeleton(silver_lf) -> LazyFrame`**

Build the initial lead_profile table before persona classification and intent scoring.

**`build_agent_performance(silver_lf, lead_personas_lf) -> LazyFrame`**

Build the agent_performance table. Requires `lead_personas_lf` (the lead_profile frame after persona + intent fill) so `top_persona_converted` reflects the LLM output.

**`build_competitor_intel(silver_lf) -> LazyFrame`**

Build the competitor_intel table from conversation_outcome aggregations.

**`build_insights(silver_lf, lead_profile_lf) -> dict`**

Build the insights JSON summary.

**`aggregate_leads(silver_lf) -> list[LeadAggregate]`**

Aggregate Silver frame into a list of `LeadAggregate` objects for persona classification. Each aggregate carries all dimensions needed by the LLM classifier (inbound text, conversation count, closed count, etc.).

**`compute_intent_score(lead_profile_lf) -> LazyFrame`**

Compute the `intent_score` column from persona and conversation features.

## Regex Phrase Groups

For conversation scoring and phrase counting:

**`CLOSING_PHRASES`** — Expressions indicating deal closure (e.g., "closing", "agreed").

**`EVASIVE_PHRASES`** — Expressions indicating evasion (e.g., "I'll think about it").

**`HAGGLING_PHRASES`** — Expressions indicating price negotiation (e.g., "can you do better").

**`TECHNICAL_QUESTION_PHRASES`** — Expressions indicating product/technical questions.

**`count_phrase_hits(text, phrases) -> int`** — Count occurrences of phrase group in text.

## Errors Raised

See [errors.md](errors.md) for the full error taxonomy.

- **`PipelineError`** — Generic base for Gold-layer errors. Raised when persona classification fails, intent score computation fails, or writer errors occur.

## Cross-References

- **Flow walkthrough:** [gold-flow.md](../gold-flow.md) — Complete step-by-step narrative from Silver read to output write
- **Agent recovery:** See [agent.md](../agent.md) for how the agent handles Gold errors and re-runs
- **Schemas:** `pipeline.schemas.gold` — All four table schemas plus enum values
- **Persona module:** `pipeline.gold.persona` — `LeadAggregate` and `PersonaResult` types
- **Writers:** `pipeline.gold.writer` — Atomic write functions for all four tables and insights JSON
- **Concurrency:** `pipeline.gold.concurrency.classify_all` — Default LLM persona classifier (async bridge)
