# F2 — Design: Silver transforms

> Companion to `spec.md`. Decisions are fixed there; this file describes *how* the code is laid out and why.

## 1. Module layout

```
src/pipeline/
├── schemas/
│   ├── bronze.py          # F1 — untouched
│   └── silver.py          # NEW — SILVER_SCHEMA + METADATA_STRUCT (pl.Schema, pl.Struct)
├── silver/                # NEW package
│   ├── __init__.py        # public API re-exports
│   ├── reader.py          # scan Bronze partition, shape-check
│   ├── dedup.py           # pure: (conversation_id, message_id) dedup with priority
│   ├── normalize.py       # pure: name normalize, metadata JSON parse, timestamp parse
│   ├── lead.py            # pure: phone digits → HMAC → lead_id (uses Settings secret)
│   ├── pii.py             # pure: regex patterns + positional maskers (email/CPF/phone/CEP/plate)
│   ├── transform.py       # orchestrator: chains dedup→normalize→mask→lead into lazy plan
│   └── writer.py          # partitioned parquet write under data/silver/batch_id=<bid>/
├── state/
│   └── manifest.py        # EXTEND — add silver_runs table + insert/mark_completed/mark_failed/get/delete helpers
├── cli/
│   └── silver.py          # NEW — click command `transform-silver --batch-id`
└── __main__.py            # EXTEND — register the new click command
```

Tests:

```
tests/
├── unit/
│   ├── test_silver_dedup.py
│   ├── test_silver_normalize.py
│   ├── test_silver_lead.py
│   ├── test_silver_pii.py
│   ├── test_silver_reader.py
│   ├── test_silver_writer.py
│   ├── test_silver_transform.py
│   └── test_manifest_silver.py        # new rows only
└── integration/
    └── test_silver_e2e.py             # Bronze → Silver end-to-end via CLI runner
```

## 2. Data flow

```
CLI (silver.py)
  │  --batch-id <bid>
  ▼
ManifestDB.open()
  │  short-circuit if silver_runs[bid].COMPLETED
  │  else delete_silver_run(bid) if stale/failed
  │  insert_silver_run(bid, IN_PROGRESS)
  ▼
reader.scan_silver_input(bronze_root, bid)      → LazyFrame (Bronze schema)
  ▼
transform.build_silver_plan(lf, settings, now)  → LazyFrame (Silver schema)
  │  ├── normalize.parse_timestamp
  │  ├── normalize.parse_metadata
  │  ├── normalize.normalize_name
  │  ├── lead.attach_lead_id
  │  ├── pii.mask_message_body
  │  ├── pii.mask_sender_phone
  │  ├── dedup.apply_priority_dedup
  │  └── add transformed_at constant
  ▼
transform.collect_silver(plan)                  → DataFrame (streaming collect)
  ▼
writer.write_silver(df, silver_root, bid)       → SilverWriteResult(rows_written, path)
  ▼
manifest.mark_silver_completed(...)             → status=COMPLETED
  │
  ▼
click.echo + structured log
```

Every non-I/O step (everything between `reader` and `writer`) is a pure function over `pl.LazyFrame` or plain Python. I/O stays at the edges.

## 3. Module contracts

### `schemas/silver.py`

```python
METADATA_STRUCT = pl.Struct({
    "device": pl.String,
    "city": pl.String,
    "state": pl.String,
    "response_time_sec": pl.Int32,
    "is_business_hours": pl.Boolean,
    "lead_source": pl.String,
})

SILVER_SCHEMA = pl.Schema({
    "message_id": pl.String,
    "conversation_id": pl.String,
    "timestamp": pl.Datetime("us", "UTC"),
    "sender_phone_masked": pl.String,
    "sender_name_normalized": pl.String,
    "lead_id": pl.String,
    "message_body_masked": pl.String,
    "has_content": pl.Boolean,
    "campaign_id": pl.String,
    "agent_id": pl.String,
    "direction": pl.String,
    "message_type": pl.String,
    "status": pl.String,
    "channel": pl.String,
    "conversation_outcome": pl.String,
    "metadata": METADATA_STRUCT,
    "ingested_at": pl.Datetime("us", "UTC"),
    "silver_batch_id": pl.String,
    "source_file_hash": pl.String,
    "transformed_at": pl.Datetime("us", "UTC"),
})
```

### `silver/pii.py` — masking rules

Each masker is a pair (compiled `re.Pattern`, replacement function).

- **Email** — pattern `\b([A-Za-z0-9._%+-])([A-Za-z0-9._%+-]*)@([A-Za-z0-9-])([A-Za-z0-9.-]*)\.([A-Za-z]{2,})\b`; replacement `{g1}{'*' * max(4, len(g2))}@{g3}{'*' * max(4, len(g4))}.{g5}`.
- **CPF** — pattern `\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b`; replacement constant `***.***.***-**`.
- **Phone (BR)** — pattern `\+?55?\s*\(?\d{2}\)?\s*9?\d{4}-?\d{4}`; replacement keeps last 2 digits: `+55 (XX) *****-**<dd>`.
- **CEP** — pattern `\b\d{5}-?\d{3}\b`; replacement constant `*****-***`.
- **Plate** — patterns for old `[A-Z]{3}-?\d{4}` and Mercosul `[A-Z]{3}\d[A-Z]\d{2}`; replacement keeps the letter at position 4 (for Mercosul) and the digit at position 4 (for old), rest masked.

`sender_phone_masked` uses the same phone masker.

All maskers are pure Python callables wrapped by a tiny `polars` wrapper (`pl.Expr.map_batches` over `pl.String`) — Polars does not ship a native regex-replace with callback, so the lazy plan contains one `map_batches` per regex group. Acceptable cost: fixture of 153k rows completes well under the RNF budget.

### `silver/lead.py`

```python
def lead_id_expr(phone_col: str, secret: bytes) -> pl.Expr:
    # LEARN: pure Polars expression — HMAC must be computed per row. We
    # go through map_elements over the digits string, which is slow but
    # tiny input per row. Could be optimized later with a Rust UDF; not
    # needed at the current scale.
```

### `silver/dedup.py`

Strategy: add a numeric `_status_rank` column, `group_by(['conversation_id', 'message_id']).agg(pl.all().sort_by(['_status_rank', 'timestamp'], descending=[True, True]).first())`. Drop `_status_rank` before writing.

### `state/manifest.py` — delta

New DDL:

```sql
CREATE TABLE IF NOT EXISTS silver_runs (
    batch_id         TEXT PRIMARY KEY,
    bronze_batch_id  TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED','FAILED')),
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    duration_ms      INTEGER,
    rows_read        INTEGER,
    rows_written     INTEGER,
    rows_deduped     INTEGER,
    silver_path      TEXT,
    error_type       TEXT,
    error_message    TEXT,
    FOREIGN KEY (bronze_batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_silver_runs_bronze ON silver_runs(bronze_batch_id);
```

New API on `ManifestDB`:

- `insert_silver_run(batch_id, bronze_batch_id, started_at)`
- `mark_silver_completed(batch_id, rows_read, rows_written, rows_deduped, silver_path, finished_at, duration_ms)`
- `mark_silver_failed(batch_id, finished_at, duration_ms, error_type, error_message)`
- `get_silver_run(batch_id) -> SilverRunRow | None`
- `is_silver_completed(batch_id) -> bool`
- `delete_silver_run(batch_id) -> bool`
- `reset_stale_silver()` — sweep `IN_PROGRESS` at `ensure_schema` time (same pattern as batches).

Note: `batch_id` of a silver run IS the bronze batch_id (1:1 relation). Keeping two columns (`batch_id` PK, `bronze_batch_id` FK) lets us evolve to multi-Silver-run-per-Bronze later without a schema rewrite — the column stays, we just generate a new PK.

### `cli/silver.py`

Mirror of `cli/ingest.py`. Same defensive `_safe_resolve`, same `run_id` context binding, same try/except/finally split, same typed-error-vs-unexpected two-branch rescue. Differences:

- Reads `--batch-id` (required) instead of `--source`.
- Builds `bronze_root / f"batch_id={bid}"` and fails fast if missing.
- Uses `insert_silver_run` / `mark_silver_completed` / `mark_silver_failed` instead of the batch equivalents.

### `__main__.py`

One line added: `cli.add_command(silver_cmd)` after the existing `ingest_cmd` registration.

## 4. Teachability (LEARN comments)

The user is learning Python + Polars + LLM-engineering. Every new module follows the F1 convention:

- Top-of-file docstring: why this module exists in plain language.
- `# LEARN:` comments at every non-obvious Polars expression, stdlib choice, or Python idiom.
- Prefer a short, slower expression with a LEARN note over a cryptic fast one.

Examples we will write:

- Why `map_batches` instead of `str.replace_all` for the PII regexes.
- Why `HMAC-SHA256` instead of plain `SHA256`.
- Why we convert to UTC explicitly instead of `.dt.replace_time_zone("UTC")` without reading (hint: one is assume, one is convert).
- Why the metadata struct has a fixed schema instead of `json_decode` inferring (hint: schema drift safety).
- Why dedup uses `group_by → first` with a rank column instead of `unique(subset=...)` (hint: `unique` doesn't let us pick *which* row wins).

## 5. Risks + mitigations

| Risk | Mitigation |
|---|---|
| `map_batches` for PII regexes is slow | Measure on the fixture first; fallback is `pl.Expr.str.replace_all` with back-reference if we can live with less positional nuance. RNF-07 sets the budget (5 s). |
| `metadata` JSON has unexpected keys | `json_decode` with a fixed `METADATA_STRUCT` drops extras silently but logs a sample when first seen. |
| Dedup tie-break when `timestamp` values are equal after parse | Secondary tie-break on Bronze file order — already stable because we `scan_parquet` in sorted order. Documented. |
| `PIPELINE_LEAD_SECRET` missing in CI | Settings loader raises at start; CI provides a dummy secret for the test job (fixture-level secret in `conftest.py`). |

## 6. Verification plan

Covered in `tasks.md` per task, but at the feature level:

- Unit tests for every pure module, including property tests where cheap (e.g., masker output length equals input length for CPF / CEP / plate).
- Integration test: run the CLI end-to-end on a tiny 50-row fixture, assert (a) Silver parquet exists, (b) row count matches post-dedup expectation, (c) no raw PII remains, (d) manifest row is COMPLETED.
- Smoke run: `pipeline transform-silver --batch-id <real-bid>` on the 153k-row fixture, timing captured and compared against RNF-07.

---

Next: `tasks.md` (atomic steps + dependencies + verification per step).
