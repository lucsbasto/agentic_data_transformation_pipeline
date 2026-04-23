---
name: medallion-data-layout
description: Medallion architecture (Bronze/Silver/Gold) conventions for this pipeline. Defines layer contracts, paths, schemas, partitioning, and what logic belongs where. Use when adding a transform, deciding where a column belongs, or reviewing a layer boundary. Trigger keywords: bronze, silver, gold, medallion, layer, partition, schema, data contract.
---

# Medallion data layout

Binding contracts for Bronze → Silver → Gold (per PRD §data layout).

## 1. Paths

```
data/
  raw/                             # immutable source drop
    conversations_bronze.parquet   # provided input (committed)
  bronze/batch_id=<id>/part-*.parquet
  silver/<table>/part-*.parquet
  gold/<table>/part-*.parquet
state/
  manifest.db                      # SQLite — batches, runs, retries, diagnoses
```

## 2. Layer contracts

### Bronze — "landed raw, typed"

- 1:1 with source rows. No filtering, no joins.
- Cast strings to their true types (timestamps, ints).
- Add: `batch_id`, `ingested_at`, `source_file_hash`.
- Partition by `batch_id`.
- **Never** apply business logic here.

### Silver — "cleaned, enriched, stable keys"

- Deduplication.
- Schema normalization (rename cols to canonical names).
- Derived cols from cheap expressions (e.g., `msg_word_count`).
- Parse conversation structure (turns, roles).
- LLM-powered enrichment: persona classification, intent tags, summary.
- Key discipline: every row has a stable primary key.
- One Silver table per logical entity: `conversations`, `messages`, `participants`.

### Gold — "analytics-ready, denormalized"

- Aggregations, joins across Silver.
- The 4 analytical tables per PRD (kept concrete):
  - `gold.persona_summary` — persona × metrics.
  - `gold.conversation_outcomes` — per-conversation outcome + drivers.
  - `gold.daily_activity` — time series.
  - `gold.insight_board` — curated non-obvious findings (LLM-generated w/ guardrails).
- Optimized for read (wide, pre-joined, compressed).

## 3. Where does logic go?

| Kind of logic | Layer |
|---|---|
| Type casting | Bronze |
| Dedup, rename | Silver |
| Join between entities | Silver (keys) or Gold (wide) |
| Aggregation | Gold |
| LLM classification | Silver |
| LLM narrative / insight | Gold |
| Regex cleanup | Silver |
| Filtering out test rows | Silver |
| Ordering / ranking | Gold |

**Rule:** if you need a layer's output for the computation, the computation lives in the next layer up.

## 4. Schemas

- Each layer has a Python schema declaration in `pipeline/schemas/<layer>.py`.
- Schema changes require a STATE.md note (decision log entry).
- Schema enforcement: assert at write, not at read. Reads trust the layer.

## 5. Partitioning

- Bronze: `batch_id` only. Small per-batch files OK.
- Silver: no partitioning by default; partition on time key if table > 10M rows.
- Gold: no partitioning. Gold tables are small, wide, read whole.

## 6. Compression + stats

- All parquet writes: `compression="zstd"`, `statistics=True`.
- Row group size: default. Revisit only if profiling says so.

## 7. Idempotency

- Bronze: overwrite partition by `batch_id`. Same input → same output.
- Silver: full-layer rewrite per run. Cheap at this scale; keeps logic simple.
- Gold: full rewrite per run.

## 8. Lineage

- Every Silver/Gold row traces to source via `batch_id` (from Bronze) and `run_id` (recorded in manifest).
- Schema includes `run_id` column on Silver + Gold. Enables replay + debug.

## 9. LLM outputs in the data layer

- LLM-produced columns are **never** the sole key. Always paired with a deterministic key + a confidence score + a `prompt_version` column.
- Low-confidence outputs (`< threshold`) land as `null` + a logged escalation, not as bad data.

## 10. Anti-patterns (do not)

- No business logic in Bronze.
- No aggregation in Silver.
- No joins against Bronze from Gold (always go through Silver).
- No undocumented new table. Add schema file + STATE.md note first.
- No mixing analytical rows with escalation rows. Escalations go to manifest, not Gold.
