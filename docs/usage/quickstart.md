# Quickstart: End-to-End Pipeline

Get the pipeline running from `git clone` to a completed agent loop in under 10 minutes.

## Prerequisites

- Python 3.12+
- `uv` package manager ([install](https://docs.astral.sh/uv/))
- DashScope API key (sign up at [Alibaba Cloud](https://dashscope.aliyun.com/))
- Git

## Setup

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd "Pipeline de Transformação Agêntica de Dados"
   ```

2. Sync dependencies and create virtual environment:
   ```bash
   uv sync
   ```

3. Copy the environment template:
   ```bash
   cp .env.example .env
   ```

4. Edit `.env` and fill in your DashScope API key:
   ```bash
   # In .env, update this line:
   ANTHROPIC_API_KEY=sk-your-dashscope-key-here
   ```

## Run the Pipeline

The pipeline transforms data through four layers: Bronze (ingest) → Silver (enrich) → Gold (classify) → Agent (self-heal).

### 1. Bronze: Ingest raw data

```bash
uv run python -m pipeline ingest
```

Expected output:
```
ingested 153228 rows into data/bronze/batch_id=<12hex>/part-0.parquet (batch=<id>, <ms> ms)
```

This creates `state/manifest.db` and writes the Bronze partition. Running again is idempotent—it detects the batch by content hash and skips re-ingestion.

Note the `batch_id` from the output; you'll use it in the next steps.

### 2. Silver: Enrich with LLM-based entity extraction

Replace `<batch_id>` with the ID from step 1:

```bash
uv run python -m pipeline silver --batch-id <batch_id>
```

Expected output (varies by batch size):
```
silver run completed: 153228 rows → enriched with entity fields
```

This reads the Bronze partition, applies LLM extraction to identify entities (company, contact, product), deduplicates rows, and writes the Silver partition to `data/silver/batch_id=<batch_id>/`.

### 3. Gold: Classify with persona extraction

```bash
uv run python -m pipeline gold --batch-id <batch_id>
```

Expected output:
```
gold run completed: created 4 tables + insights JSON
```

This reads the Silver partition, extracts persona insights via LLM, and writes four parquet tables (leads, companies, interactions, enrichment) plus an insights summary to `data/gold/batch_id=<batch_id>/`.

### 4. Agent: Self-healing loop (single iteration)

```bash
uv run python -m pipeline agent run-once
```

Expected output:
```
agent run completed: status=COMPLETED
```

This loop observes pending batches, diagnoses data quality issues, and coordinates fixes. On the golden path with no errors detected, it exits with status `COMPLETED`.

## Verify Success

Query the manifest to confirm all layers completed:

```bash
sqlite3 state/manifest.db "SELECT batch_id, layer, status FROM runs ORDER BY started_at DESC LIMIT 10;"
```

You should see four rows (one per layer) with `status='COMPLETED'`:

```
batch_id|layer|status
<id>    |bronze|COMPLETED
<id>    |silver|COMPLETED
<id>    |gold|COMPLETED
<id>    |agent|COMPLETED
```

Check output directories:

- Bronze: `data/bronze/batch_id=<id>/part-0.parquet`
- Silver: `data/silver/batch_id=<id>/part-0.parquet`
- Gold: `data/gold/batch_id=<id>/` (4 tables + insights.json)

## Next Steps

- **Command reference**: See [docs/usage/cli.md](../usage/cli.md) for all CLI flags and options.
- **Configuration**: See [docs/usage/configuration.md](../usage/configuration.md) for environment variables and tuning.
- **Agent flow**: See [docs/agent-flow.md](../agent-flow.md) for self-healing loop internals.
- **Troubleshooting**: See [docs/troubleshooting.md](../troubleshooting.md) for common errors and fixes.
