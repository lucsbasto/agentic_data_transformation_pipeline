# F5 — Sentiment Column Backfill Runbook

## Context

F5 added two columns to `gold.lead_profile`:
- `sentiment` — `pl.Enum("positivo", "neutro", "negativo", "misto")`
- `sentiment_confidence` — `pl.Float64`

`assert_lead_profile_schema` rejects any parquet missing these
columns, so existing pre-F5 batches **will fail drift validation**
on next access until they are re-built.

The combined-prompt change also bumps `PROMPT_VERSION_PERSONA` from
`v2` to `v3`. The LLM cache key (`src/pipeline/llm/cache.py:77-88`)
includes the system prompt text, so the bump invalidates **every
cached entry**. The first post-deploy batch is therefore a full
LLM re-run — there is no cache savings for backfill.

## Cost estimate

For a backfill run over the test dataset (`conversations_bronze.parquet`,
≈15k conversations):

- Cache hits: 0 (prompt version flush)
- Hard-rule hits: ~30-50% of leads (R1/R2/R3 cover the common cases)
- LLM-dispatched leads: ~50-70% × 15k = ~7.5k–10.5k calls
- Per call: ~500 input tokens + ~30 output tokens

Apply `pipeline_llm_max_calls_per_batch` cap to limit blast radius
per batch. Run sequentially over batches if the daily budget is tight.

## Procedure

### Option A — Re-run every batch (recommended)

```bash
pipeline manifest list --layer silver --status completed | \
  awk '{ print $1 }' | \
  xargs -I{} pipeline gold run-once --batch-id {}
```

Atomic rename in `src/pipeline/gold/writer.py` makes re-runs idempotent.
Schema-drift assertion will pass after each successful run.

### Option B — Null-fill migration (only if Option A is impractical)

A one-shot script that appends typed-null `sentiment` /
`sentiment_confidence` columns to existing parquets. **Not recommended**
because the data quality is degraded — every lead lands as `null`
sentiment instead of a real classification.

## Verification

After backfill:

```bash
# Schema sanity (run on each migrated batch)
python -c "
import polars as pl
from pipeline.schemas.gold import assert_lead_profile_schema
df = pl.read_parquet('<gold_root>/lead_profile/<batch_id>.parquet')
assert_lead_profile_schema(df)
print('schema OK')
print(df.select('sentiment').to_series().value_counts())
"
```

The structlog event `gold.sentiment.batch_complete` reports per-batch
distributions:

```json
{
  "leads": 1234,
  "sentiment_source_mix": {"rule": 600, "llm": 500, "llm_fallback": 134},
  "sentiment_label_mix": {"positivo": 480, "neutro": 410, "negativo": 290, "misto": 54}
}
```

## Calibration gate

Post-deploy first-batch threshold: `misto ≤ 25%` of LLM-classified leads.
If `sentiment_label_mix.misto / (leads - sentiment_source_mix.rule) > 0.25`,
file a follow-up issue to iterate on the prompt's `misto` definition.
This is a soft gate — does not block the release.

## Rollback

If F5 introduces a regression:

1. `git revert` the F5 schema commit. Pipeline returns to v2 prompt.
2. The drift guard now rejects post-revert reads of F5-era parquets.
3. Re-run gold once on each affected batch under the reverted code,
   or restore the previous parquets from the prior backup.

The prompt-version bump is the single coupling point — reverting it
also reverts the cache invalidation. Cached v2 entries return.
