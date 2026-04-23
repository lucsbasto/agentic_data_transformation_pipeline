# F2 — Spec: Silver transforms (dedup + normalize + PII mask)

> Status: draft (approved 2026-04-23)
> Depends on: F1 shipped
> Size: Large
> Follow-up: F3 takes LLM-based entity extraction (vehicle, competitor, accident history). F2 scope is deterministic only.

## 1. Intent

Transform the Bronze layer (raw-preserved, string-typed parquet partitioned by `batch_id`) into a Silver layer that is:

- **Deduplicated** on `(conversation_id, message_id)` — Bronze carries a `sent` + `delivered` duplicate per message.
- **Typed** — timestamps parsed to `Datetime[µs, UTC]`, booleans real, `metadata` JSON parsed to a struct.
- **Normalized** on `sender_name` — trim, case-fold, strip accents, reconciled per `sender_phone`.
- **Masked for LGPD** — email, CPF, phone, CEP, license plate masked positionally (dimensions preserved).
- **Keyed by stable lead** — `lead_id = HMAC-SHA256(secret, normalized_phone)` (truncated to 16 hex chars).
- **Registered in manifest** — every Silver transform run is tracked with its own row, FK'd to the Bronze batch it consumed.

Out of scope for F2: LLM-based entity extraction (vehicle, competitor, accident text). Deferred to F3 — keeps LLM cost off the default Bronze→Silver path and keeps F2 deterministic / replayable / testable without network.

## 2. Source contract (input)

F2 reads **`data/bronze/batch_id=<bid>/part-*.parquet`** — a directory produced by F1 `pipeline ingest`. Schema mirrors `src/pipeline/schemas/bronze.py`. Every column is present, no nulls (enforced by F1). `timestamp` is `pl.String` in `YYYY-MM-DD HH:MM:SS` format, no timezone. `metadata` is a JSON string with `device`, `city`, `state`, `response_time_sec`, `is_business_hours`, `lead_source`.

F2 will **not** re-validate the Bronze schema beyond what Polars enforces on read — F1 owns that contract.

## 3. Functional requirements

| ID | Requirement |
|---|---|
| F2-RF-01 | Given a Bronze `batch_id`, Silver pipeline reads the partition, applies all transforms, writes `data/silver/batch_id=<bid>/part-*.parquet`, and records a row in the manifest `silver_runs` table. |
| F2-RF-02 | Silver is deduped on `(conversation_id, message_id)` keeping the highest-priority row. Priority: `status` ordinal (`read`=3, `delivered`=2, `sent`=1, else 0) **descending**, then `timestamp` **descending** as tie-break. |
| F2-RF-03 | `timestamp` parsed to `Datetime[µs, UTC]`. Source is naive local; F2 treats it as UTC-naive and attaches UTC explicitly (documented assumption — source dictionary has no TZ info). |
| F2-RF-04 | `metadata` (JSON string) parsed to a typed struct with fields `device:str`, `city:str`, `state:str`, `response_time_sec:Int32`, `is_business_hours:Boolean`, `lead_source:str`. Null if the JSON is unparseable (logged, counted). |
| F2-RF-05 | `sender_name_normalized` = trim + case-fold + NFKD-strip-accents of `sender_name`. Same `sender_phone` across multiple rows yields the **most frequent** normalized name for that phone (ties broken by first occurrence by timestamp). |
| F2-RF-06 | `lead_id` = first 16 hex chars of `HMAC-SHA256(PIPELINE_LEAD_SECRET, normalized_phone)`. `normalized_phone` = keep digits only from `sender_phone`. Deterministic per run. |
| F2-RF-07 | PII masking, positional (dimensions preserved): email → `a****@g****.com`; CPF → `***.***.***-**`; phone → `+55 (**) *****-**XX` (last 2 digits kept); CEP → `*****-***`; plate → `***1D**` (new Mercosul) / `***1D23` (old) — generic rule: keep positional letters-vs-digits template, mask alphanum content. Applied to `message_body` and to any phone/email/cpf in `sender_name`. |
| F2-RF-08 | `has_content` boolean: `false` when `message_body` is empty/whitespace OR `message_type` in {`image`, `audio`, `video`, `sticker`, `document`} AND `message_body` length < 8 chars. True otherwise. |
| F2-RF-09 | Manifest: new table `silver_runs(batch_id TEXT PK, bronze_batch_id TEXT FK → batches.batch_id, status, started_at, finished_at, duration_ms, rows_read, rows_written, rows_deduped, silver_path, error_type, error_message)`. ON DELETE CASCADE from `batches`. |
| F2-RF-10 | Idempotency: if `silver_runs.batch_id` is already `COMPLETED` for the target Bronze batch, CLI short-circuits with a skip log line. Stale `IN_PROGRESS` or `FAILED` rows are deleted and retried (F1 pattern). |
| F2-RF-11 | CLI: `python -m pipeline transform-silver --batch-id <bid>`. `--batch-id` is required. No `--latest` flag in F2 (defer to F5). |
| F2-RF-12 | Every Silver run logs structured events: `silver.start`, `silver.batch.opened`, `silver.dedup.done` (with `rows_before`, `rows_after`), `silver.mask.done` (with `masked_emails`, `masked_cpfs`, ... counts), `silver.complete` / `silver.failed`. `run_id` + `batch_id` bound to context for the whole run. |

## 4. Non-functional requirements

| ID | Requirement |
|---|---|
| F2-RNF-01 | Polars lazy-first. `scan_parquet` → lazy transforms → `collect(streaming=True)` for the write. No pandas anywhere. |
| F2-RNF-02 | Hashing + masking are pure functions, no I/O. Tested as such. |
| F2-RNF-03 | `PIPELINE_LEAD_SECRET` env var required (no silent fallback). Settings loader raises if missing. |
| F2-RNF-04 | All PII regexes compiled once at import, case-insensitive where meaningful. Unit-tested against a fixture of Brazilian formats. |
| F2-RNF-05 | Ruff + mypy strict clean. `from __future__ import annotations` at top of every module. |
| F2-RNF-06 | Test coverage ≥ 90% on `src/pipeline/silver/**` and new `schemas/silver.py`. |
| F2-RNF-07 | End-to-end on the 153k-row fixture: < 5 seconds wall clock on a modern laptop, Silver parquet < 10 MB zstd. |

## 5. Decisions (from gray-area discussion on 2026-04-23)

| # | Question | Decision | Why |
|---|---|---|---|
| D1 | F2 scope: LLM extraction in or out? | **Out** — deterministic only. LLM goes to F3. | Cost/latency off default path; F2 replayable without network; smaller blast radius. |
| D2 | PII masking style | Positional, dimensions preserved (`a****@g****.com`, `***.***.***-**`). | Matches PRD §5 example exactly. Hashing destroys dimensions needed downstream (e.g., provider-domain stats). |
| D3 | `lead_id` construction | HMAC-SHA256 with env secret; 16 hex chars. | Not reversible without secret. Truncation keeps row size small; 16 hex = 64-bit space — enough for ~15k leads. |
| D4 | Dedup tie-break | `status` priority `read>delivered>sent>other`, then `timestamp` DESC. | `read` is the latest lifecycle event for a delivered message. Matches PRD "evento mais recente". |
| D5 | Silver partitioning | `data/silver/batch_id=<bid>/` (mirror Bronze). | Keeps lineage 1:1 with Bronze batch. Date-partitioning is a Gold/analytics concern, not Silver. |
| D6 | Manifest layout | New table `silver_runs`, FK to `batches`. | Extensible to `gold_runs` in F3. Multiple Silver rebuilds per Bronze batch stay separable. Keeps `batches` append-only and stable. |
| D7 | Re-run on COMPLETED Silver | Skip with log + CLI echo. | Consistent with F1 batch idempotency. Explicit `--force` flag deferred to F5. |
| D8 | `--batch-id` contract | Required. | No magic defaults in F2. `--latest` lands in F5 alongside the watch loop. |
| D9 | Audio transcription confidence | Deferred to F3 (needs LLM/heuristic noise scoring). F2 only sets `has_content` boolean. | Keeps F2 deterministic. Confidence model belongs near entity extraction. |

## 6. Data contract (output)

Silver parquet schema (`src/pipeline/schemas/silver.py`):

| Column | Type | Source | Notes |
|---|---|---|---|
| `message_id` | String | Bronze | Dedup key part. |
| `conversation_id` | String | Bronze | Dedup key part. |
| `timestamp` | Datetime[µs, UTC] | parsed from Bronze string | UTC assumed (documented). |
| `sender_phone_masked` | String | derived | `+55 (11) *****-12` style (keep last 2). |
| `sender_name_normalized` | String | derived | Reconciled per phone. |
| `lead_id` | String | derived | HMAC-SHA256(secret, digits(sender_phone))[:16]. |
| `message_body_masked` | String | derived | Positional masking of all PII matches. |
| `has_content` | Boolean | derived | See F2-RF-08. |
| `campaign_id` | String | Bronze | passthrough. |
| `agent_id` | String | Bronze | passthrough. |
| `direction` | Enum(inbound,outbound) | Bronze | narrowed. |
| `message_type` | String | Bronze | passthrough (8 known values). |
| `status` | Enum(sent,delivered,read,failed) | Bronze | narrowed. |
| `channel` | String | Bronze | passthrough (constant `whatsapp`). |
| `conversation_outcome` | String | Bronze | passthrough. |
| `metadata` | Struct(...) | parsed | See F2-RF-04. |
| `ingested_at` | Datetime[µs, UTC] | Bronze | passthrough. |
| `silver_batch_id` | String | constant per run | = Bronze batch_id (1:1). |
| `source_file_hash` | String | Bronze | passthrough. |
| `transformed_at` | Datetime[µs, UTC] | constant per run | Silver transform wall-clock. |

## 7. Acceptance criteria

1. `python -m pipeline transform-silver --batch-id <bid>` turns a Bronze partition into a Silver partition in a single command.
2. Running it twice in a row on the same `batch_id` prints the skip message and performs no writes.
3. The Silver parquet round-trips through `pl.read_parquet` with the schema declared in `schemas/silver.py`.
4. Dedup reduces the fixture 153,228 rows to the expected count of unique `(conversation_id, message_id)` pairs; the delta equals `rows_deduped` in the manifest.
5. No raw CPF / email / phone / CEP / plate appears in any `message_body_masked` value, verified by regex scan over the written parquet in an integration test.
6. `lead_id` is stable across runs for the same phone, and two different phones never collide in the fixture.
7. Manifest `silver_runs` row exists with status `COMPLETED`, `bronze_batch_id` matching, non-zero `duration_ms`, `rows_written > 0`.
8. `ruff check`, `mypy --strict src tests`, `pytest -q` all green. Coverage ≥ 90% on new code.

## 8. Open questions (none blocking)

- Do we need to handle plate formats other than Brazilian? **No** — source is exclusively Brazilian.
- Should `sender_name_normalized` attempt to split into first/last name? **No** — reconciliation is the goal, not parsing.
- Should we store the masking regex match counts on the manifest row itself? **No** — they live in the structured log (`silver.mask.done`) and can be re-aggregated from logs. Keeps manifest narrow.

---

Next: `design.md` (component breakdown + module layout + module contracts), then `tasks.md` (atomic steps + verification).
