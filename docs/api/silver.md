# Silver Module API

This module contains the Bronze -> Silver orchestrator that deduplicates, normalizes, masks PII, and derives stable lead identifiers from raw conversation data.

For a walkthrough of the complete Silver flow including CLI entry points, see [silver-flow.md](../silver-flow.md).

## Entrypoint

### `silver_transform(lf, *, secret, silver_batch_id, transformed_at) -> LazyFrame`

Build the Silver LazyFrame from a Bronze LazyFrame. This is a pure function with no I/O.

**Parameters:**
- `lf: pl.LazyFrame` — Input frame with schema matching `pipeline.schemas.bronze.BRONZE_SCHEMA`
- `secret: str` — Raw HMAC key for deriving stable `lead_id` from phone numbers. Pass unwrapped `SecretStr.get_secret_value()` at composition boundary
- `silver_batch_id: str` — Value for the `silver_batch_id` column. Typically equal to Bronze's `batch_id`
- `transformed_at: datetime` — UTC-aware datetime for the `transformed_at` column. Pass `datetime.now(tz=UTC)` at the CLI layer

**Returns:** `pl.LazyFrame` with schema exactly matching `SILVER_SCHEMA` (column order preserved).

**Side effects:** None. The CLI layer owns the Bronze read and Silver write via `pipeline.cli.silver`.

**Sequence:**
1. Dedup on `(conversation_id, message_id)` via `silver/dedup.py`
2. Per-row transforms (normalize, mask PII, extract dimensions) fused into a single `select` pass
3. Lead-level name reconciliation via `silver/reconcile.py` (runs after `lead_id` is available)
4. Final column order pin to `SILVER_SCHEMA` (Parquet is order-sensitive)

### `assert_silver_schema(df) -> None`

Validate that a DataFrame's schema matches `SILVER_SCHEMA` exactly.

**Parameters:**
- `df: pl.DataFrame` — Frame to validate

**Raises:** `SchemaDriftError` with details if any mismatch (missing column, wrong dtype, extra columns).

## Schemas

### Input Schema

See `pipeline.schemas.bronze.BRONZE_SCHEMA` — Silver accepts the typed Bronze parquet directly.

### Output Schema: `SILVER_SCHEMA`

| Column | Type | Notes |
|--------|------|-------|
| `message_id` | `String` | Dedup grain key (non-null enforced in quarantine) |
| `conversation_id` | `String` | Dedup grain key (non-null enforced in quarantine) |
| `timestamp` | `Datetime[µs, UTC]` | Parsed from naive Bronze datetime; tagged as UTC per PRD assumption |
| `sender_phone_masked` | `String` | Positional masking; last 2 digits preserved for analytics |
| `sender_name_normalized` | `String` | Trimmed, case-folded, accent-stripped, reconciled per phone |
| `lead_id` | `String` | Truncated HMAC-SHA256 hex string; stable per phone, non-reversible |
| `message_body_masked` | `String` | Email/CPF/phone/CEP/plate masked in place with positional rules |
| `has_content` | `Boolean` | True if `message_body` has non-whitespace text (excludes stickers) |
| `email_domain` | `String` | Extracted domain bucket (e.g., "gmail.com"); local part dropped |
| `has_cpf` | `Boolean` | Presence flag; no CPF value kept |
| `cep_prefix` | `String` | First 5 digits (region); street suffix dropped |
| `has_phone_mention` | `Boolean` | Presence flag; no phone value kept |
| `plate_format` | `String` | Mercosul vs. old format; no plate characters kept |
| `audio_confidence` | `String` | "high" / "low" for audio rows; `null` for other message types |
| `veiculo_marca` | `String` | LLM-extracted vehicle brand; seeded as `null` by transform, filled by F2 extraction lane |
| `veiculo_modelo` | `String` | LLM-extracted vehicle model; seeded as `null` |
| `veiculo_ano` | `Int32` | LLM-extracted vehicle year; seeded as `null` |
| `concorrente_mencionado` | `String` | LLM-extracted competitor mention; seeded as `null` |
| `valor_pago_atual_brl` | `Float64` | LLM-extracted current price paid; seeded as `null` |
| `sinistro_historico` | `Boolean` | LLM-extracted claim history; seeded as `null` |
| `campaign_id` | `String` | Passed through from Bronze unchanged |
| `agent_id` | `String` | Passed through from Bronze unchanged |
| `direction` | `Enum(DIRECTION_VALUES)` | Closed-set enum; reused from Bronze (inbound/outbound) |
| `message_type` | `Enum(MESSAGE_TYPE_VALUES)` | Closed-set enum; reused from Bronze (text/audio/sticker/etc.) |
| `status` | `Enum(STATUS_VALUES)` | Closed-set enum; reused from Bronze |
| `channel` | `String` | Passed through from Bronze unchanged |
| `conversation_outcome` | `String` | Passed through from Bronze unchanged |
| `metadata` | `Struct` | Parsed from JSON: `{device, city, state, response_time_sec, is_business_hours, lead_source}` |
| `ingested_at` | `Datetime[µs, UTC]` | Flows through from Bronze; wall clock of Bronze ingest |
| `silver_batch_id` | `String` | Batch identifier; typically equals Bronze `batch_id` |
| `source_file_hash` | `String` | Passed through from Bronze unchanged |
| `transformed_at` | `Datetime[µs, UTC]` | NEW in Silver; wall clock of this Silver run |

### Quarantine Schema: `REJECTED_SCHEMA`

Rows missing `message_id` or `conversation_id` are rejected. The quarantine parquet carries all Bronze columns plus two additional columns:

| Column | Type | Notes |
|--------|------|-------|
| `reject_reason` | `String` | One of: `"null_message_id"`, `"null_conversation_id"` |
| `rejected_at` | `Datetime[µs, UTC]` | Wall clock of the rejection decision |

All Bronze columns are also present to give operators full context for triaging. Quarantine files live at `{silver_root}/batch_id={id}/rejected/part-0.parquet`.

## Public Helpers

### PII Masking Functions

**`mask_all_pii(text: str) -> tuple[str, dict]`**

Mask email, CPF, phone, CEP, and license plate in a single pass using positional replacement rules. Returns the masked string and a counts dict for logging.

**`mask_phone_only(text: str) -> str`**

Mask only phone numbers using positional replacement (keeps last 2 digits).

Both functions are re-exported from `pipeline.silver.pii` and wrapped by Polars expressions in `silver_transform` so they integrate seamlessly into the LazyFrame pipeline.

### Quarantine Partitioning

**`partition_rows(lf, *, rejected_at) -> tuple[LazyFrame, LazyFrame]`**

Split an input LazyFrame into `(valid, rejected)` branches.

**Parameters:**
- `lf: pl.LazyFrame` — Frame with Bronze schema
- `rejected_at: datetime` — UTC wall clock for rejected rows

**Returns:** Tuple of `(valid_lf, rejected_lf)`. The `rejected_lf` has schema `REJECTED_SCHEMA`. Pure function; no I/O.

## Errors Raised

See [errors.md](errors.md) for the full error taxonomy.

- **`SchemaDriftError`** — Raised by `assert_silver_schema` when input DataFrame schema diverges from `SILVER_SCHEMA` (missing columns, type mismatches, extras). Callers should inspect the error message for the specific mismatch.
- **`SilverError`** — Generic base for Silver-layer errors. Raised by the CLI layer during write (`pipeline.silver.writer`).

## Cross-References

- **Flow walkthrough:** [silver-flow.md](../silver-flow.md) — Complete step-by-step narrative from Bronze read to Parquet write
- **Agent recovery:** See [agent.md](../agent.md) for how the agent handles Silver errors and quarantine partitions
- **Schemas:** `pipeline.schemas.silver.SILVER_SCHEMA` (column order), `pipeline.schemas.silver.SILVER_COLUMNS` (tuple of names)
- **PII module:** `pipeline.silver.pii` — Regex patterns and masking logic
- **Quarantine module:** `pipeline.silver.quarantine` — Rejection reasons and schema
