# Silver flow — Bronze → Silver, step by step

Audience: engineers new to the codebase. The PRD (§6.2) specifies *what*
Silver must produce; this doc walks through *how* the code actually
builds it, file by file, from the moment the CLI is invoked to the
moment the parquet lands on disk and the manifest row is committed.

Every section ends with the exact file/function you can jump to if you
want to see the implementation. Keep this doc in sync when the
sequence changes — reviewers rely on it to sanity-check PRs.

---

## 0. Entry point: `python -m pipeline silver`

File: `src/pipeline/__main__.py` → `cli.add_command(silver)`

Click dispatches `silver` to `pipeline.cli.silver:silver`. The command
accepts three flags:

| Flag            | Default                    | Purpose                                      |
| --------------- | -------------------------- | -------------------------------------------- |
| `--batch-id`    | required                   | Bronze batch to transform                    |
| `--bronze-root` | `data/bronze/`             | Root dir holding `batch_id=<id>/` partitions |
| `--silver-root` | `data/silver/`             | Where to write the Silver partition          |

The runtime loads `Settings` once at entry, binds a fresh correlation
id (`run_id`), and clears any stale structlog context from a previous
command running in the same process.

File: `src/pipeline/cli/silver.py:silver`

---

## 1. Preflight & idempotency

Before any parquet is read, the CLI makes three cheap checks and bails
out fast if any of them fails. Order is deliberate: filesystem first
(cheapest), then manifest (DB hit), then short-circuit if already done.

1. **Path hygiene** — `_safe_resolve` rejects `..` segments in
   `--bronze-root` and `--silver-root`. `_validate_batch_id` rejects
   anything that looks like a path fragment. These prevent an operator
   from walking outside the project tree or forging a partition name.
2. **Bronze partition exists** — Silver reads from
   `bronze_root/batch_id=<id>/part-0.parquet`. If that file does not
   exist, raise `SilverError` immediately; no manifest row is opened.
3. **Manifest alignment** —
   - `manifest.get_batch(batch_id)` must return a `COMPLETED` row.
   - `manifest.is_run_completed(batch_id, 'silver')` short-circuits a
     repeat run (idempotency: re-running is free).
   - `manifest.delete_runs_for(batch_id, 'silver')` wipes stale
     `IN_PROGRESS` / `FAILED` runs so the new `insert_run` does not
     collide on the primary key.

File: `src/pipeline/cli/silver.py:_run_silver`

---

## 2. Open a `runs` row

A fresh `run_id` (12-char UUID hex) is inserted as
`IN_PROGRESS`. `started_at` is UTC wall clock; `started_wall` captures
`time.monotonic()` so the duration stopwatch survives any clock shift.

File: `src/pipeline/state/manifest.py:insert_run`

---

## 3. Lazy scan of Bronze

`pl.scan_parquet(bronze_part)` returns a `LazyFrame` — a *plan*, not a
materialized table. Every helper below takes and returns a `LazyFrame`
so Polars can fuse the whole chain into a single physical pass.

We also capture `rows_in = lf.select(pl.len()).collect().item()`
up-front so the manifest row can record the input count even when the
transform later collapses rows.

File: `src/pipeline/cli/silver.py:_run_silver` (section "try:")

---

## 4. Dedup — one row per `(conversation_id, message_id)`

The first transform is always dedup. Bronze preserves every WhatsApp
event (`sent`, `delivered`, `read`), so the same message may appear
two or three times. Silver guarantees one row per
`(conversation_id, message_id)`.

Rule:
- Sort by **status priority desc** (`read` > `delivered` > `sent`) →
  **timestamp desc** → **batch_id desc**.
- Keep the first row per dedup key.

Why priority before timestamp: `read` always happens chronologically
after `delivered`, but timestamps can tie (same-second events) while
the priority order is an ordered contract the source always respects.
Batch_id is a pure determinism tiebreak — same Bronze twice always
produces the same winner.

File: `src/pipeline/silver/dedup.py:dedup_events`

---

## 5. Per-row transforms — one `select` pass

After dedup, a single `.select([...])` call fuses every per-row
transform into one Polars pass. Order of expressions inside the
`select` does not affect output; Polars plans them together.

### 5a. Timestamp tagging

Bronze stores `timestamp` as naive `Datetime('us')`. The source parquet
has no timezone metadata and the data dictionary documents the values
as UTC, so we *tag* (not convert) with `dt.replace_time_zone('UTC')`.

File: `src/pipeline/silver/normalize.py:parse_timestamp_utc`

### 5b. `sender_phone_masked`

`pii.mask_phone_only` applies the positional phone regex and replaces
every match with `+55 (**) *****-**<last2>`. The raw `sender_phone`
column does **not** survive to Silver.

File: `src/pipeline/silver/pii.py:mask_phone_only`,
`src/pipeline/silver/transform.py:_mask_phone_expr`

### 5c. `sender_name_normalized` (per-row only; reconciliation is step 6)

`normalize.normalize_name_expr`: `.strip()` → NFKD accent decomposition
→ strip combining marks → `.casefold()`. Converts
`"Ana Paula Ribeiro"`, `"ANA PAULA"`, and `"Aná Paula"` into the same
canonical form.

File: `src/pipeline/silver/normalize.py:normalize_name_expr`

### 5d. `lead_id` (HMAC-SHA256 over normalized phone)

`lead.derive_lead_id_expr` calls `normalize_phone_digits` then
`hmac.new(secret, phone, 'sha256').hexdigest()[:16]`. The 16-hex (64-
bit) truncation keeps the parquet compact while staying well below
birthday-collision probability at the ~10M-lead scale.

Key property: the id is **non-reversible** without
`PIPELINE_LEAD_SECRET`. An attacker can brute-force every 11-digit
phone against plain SHA-256 in hours on a GPU; adding HMAC makes the
attack target the secret instead.

File: `src/pipeline/silver/lead.py:derive_lead_id_expr`

### 5e. `message_body_masked`

`pii.mask_all_pii` runs every regex (email → CPF → CEP → phone → plate)
in a safe order and returns `(masked_text, counts)`. The transform
keeps only the masked text.

File: `src/pipeline/silver/pii.py:mask_all_pii`,
`src/pipeline/silver/transform.py:_mask_body_expr`

### 5f-bis. Extract analytical dimensions (regex lane)

Five columns are derived from the *raw* `message_body` in the same
`select` pass — Polars fuses every regex walk into a single scan per
row:

| Column             | Shape        | What                                     |
| ------------------ | ------------ | ---------------------------------------- |
| `email_domain`     | `String`     | Lower-cased host + TLD (`"gmail.com"`).   |
| `has_cpf`          | `Boolean`    | True when a formatted CPF is present.     |
| `cep_prefix`       | `String`     | First 5 digits of the earliest CEP.       |
| `has_phone_mention`| `Boolean`    | True when any phone-shaped run is found.  |
| `plate_format`     | `String`     | `"mercosul"` / `"old"` / `null`.          |

Every column here is *non-identifying* by construction — domain
without local part, region prefix without street, format class
without plate characters. The boolean flags carry presence without
leaking digits. The LLM extraction lane (veículo, concorrente, valor
atual, histórico de sinistros — PRD §6.2) lands in a later F2 slice.

File: `src/pipeline/silver/extract.py`,
`src/pipeline/silver/transform.py` (the five extract expressions in
the main `select`).

### 5g. `has_content` boolean

True when `message_body` is non-null and contains at least one
non-whitespace character. PRD §6.2 specifies stickers/images without
captions as the main `false` signal; we generalize to "empty body"
because `message_type` already encodes the media-vs-text distinction
and a null-body text message is also uninspectable.

File: `src/pipeline/silver/transform.py:_has_content_expr`

### 5h. Parse `metadata` JSON

`normalize.parse_metadata_expr` decodes each JSON string into the
typed `METADATA_STRUCT` (device, city, state, response_time_sec,
is_business_hours, lead_source). Malformed JSON → null (not raise);
the orchestrator aggregates the null count for a log field.

File: `src/pipeline/silver/normalize.py:parse_metadata_expr`

### 5i. Passthrough + lineage constants

- Passthrough from Bronze: `message_id`, `conversation_id`,
  `campaign_id`, `agent_id`, `direction`, `message_type`, `status`,
  `channel`, `conversation_outcome`, `ingested_at`, `source_file_hash`.
- New constants: `silver_batch_id` (= Bronze `batch_id`; Silver is 1:1
  with the Bronze batch it consumes), `transformed_at`
  (`datetime.now(tz=UTC)` stamped at CLI invocation).

File: `src/pipeline/silver/transform.py:silver_transform`

---

## 6. Lead-level name reconciliation

After `lead_id` exists for every row, `reconcile_name_by_lead` picks
**one canonical name per lead** and broadcasts it back onto every row
from that lead. Rule:

1. Among non-null names per `lead_id`, pick the **most frequent**.
2. Tie → **longest** name (`"ana paula ribeiro"` beats `"ana paula"`).
3. Tie → **lexicographically smallest** (pure determinism).

The implementation is a `group_by` + sort + `join` so row count is
preserved (critical — Silver contract is *one row per dedup key*).

File: `src/pipeline/silver/reconcile.py:reconcile_name_by_lead`

---

## 7. Pin column order to `SILVER_SCHEMA`

The final step in the LazyFrame plan is
`.select(list(SILVER_COLUMNS))`. Parquet files are column-ordered; any
drift here would force downstream consumers to reorder on read.

File: `src/pipeline/schemas/silver.py` for the authoritative schema.

---

## 8. Collect and assert schema

`silver_lf.collect()` materializes the frame. Immediately after,
`assert_silver_schema(df)` compares `df.schema` against
`SILVER_SCHEMA`. A mismatch raises `SchemaDriftError` with a
per-column diff — belt-and-braces check in case a future refactor
slips a column add/drop past type-checking.

File: `src/pipeline/silver/transform.py:assert_silver_schema`

---

## 9. Atomic write

`write_silver(df, silver_root, batch_id)`:

1. Asserts schema once more (in case caller passed a hand-crafted df).
2. Creates `silver_root/.tmp-batch_id=<id>/` next to the final dir
   (same filesystem, so `rename` is atomic).
3. Writes `part-0.parquet` (zstd compressed, with row-group statistics).
4. If a final dir exists (re-run scenario) → `shutil.rmtree`. Then
   `tmp_dir.replace(final_dir)` — this is the atomic swap.
5. On any failure, `shutil.rmtree(tmp_dir, ignore_errors=True)` so
   repeat runs start clean.
6. Re-scans the written file and asserts the row count matches
   `df.height` — catches a write that silently truncated.

Output path: `silver_root/batch_id=<id>/part-0.parquet`.

File: `src/pipeline/silver/writer.py:write_silver`

---

## 10. Commit manifest run

`mark_run_completed` flips the `IN_PROGRESS` row to `COMPLETED` and
records:

- `rows_in` — row count of the Bronze scan.
- `rows_out` — row count of the Silver parquet.
- `rows_deduped` — `rows_in - rows_out`.
- `output_path` — absolute path to the written Silver file.
- `duration_ms` — `time.monotonic()` delta.
- `finished_at` — UTC wall clock (ISO-8601).

On any raised `PipelineError` in the try-block, `mark_run_failed` is
called instead with the error class name and message. A non-pipeline
exception (raw `RuntimeError`, etc.) is wrapped in `SilverError` so
callers see a single error hierarchy.

File: `src/pipeline/state/manifest.py:mark_run_completed`,
`mark_run_failed`.

---

## 11. Operator-visible output

`click.echo(...)` prints a one-liner with `rows_out`, `rows_deduped`,
`output_path`, `run_id`, and `duration_ms`. The final `structlog` event
is `silver.complete` with the same fields as structured JSON.

---

## Glossary of files touched

| Module                             | Role                                     |
| ---------------------------------- | ---------------------------------------- |
| `pipeline/cli/silver.py`           | Click command + orchestration            |
| `pipeline/silver/transform.py`     | Compose all per-row transforms           |
| `pipeline/silver/dedup.py`         | Dedup rule                               |
| `pipeline/silver/normalize.py`     | Timestamp, metadata, name normalizers    |
| `pipeline/silver/pii.py`           | Positional PII maskers                   |
| `pipeline/silver/extract.py`       | Regex entity extraction (domain/region/format) |
| `pipeline/silver/lead.py`          | Phone normalization + HMAC-SHA256 lead id |
| `pipeline/silver/reconcile.py`     | Canonical name per lead                  |
| `pipeline/silver/writer.py`        | Atomic parquet writer                    |
| `pipeline/schemas/silver.py`       | Authoritative Silver schema              |
| `pipeline/schemas/manifest.py`     | `runs` table DDL + status constants      |
| `pipeline/state/manifest.py`       | Manifest DB API (insert_run, mark_*)     |

---

## Test map

| Test file                                          | Covers                               |
| -------------------------------------------------- | ------------------------------------ |
| `tests/unit/test_silver_dedup.py`                  | Priority, timestamp, batch_id ties   |
| `tests/unit/test_silver_normalize.py`              | Timestamp tag, metadata, name norm   |
| `tests/unit/test_silver_pii.py`                    | Every positional masker              |
| `tests/unit/test_silver_extract.py`                | Every entity extractor               |
| `tests/unit/test_silver_lead.py`                   | Phone normalization, HMAC, idempotence |
| `tests/unit/test_silver_reconcile.py`              | Canonical-name pick rule             |
| `tests/unit/test_silver_transform.py`              | Full transform wiring + schema round-trip |
| `tests/integration/test_cli_silver_e2e.py`         | End-to-end CLI → parquet → manifest   |
