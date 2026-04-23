# F1 review lane — pending fixes plan

Scope: every finding in `REVIEWS/INDEX.md` + `F1.9-critic.md` that is **not yet reflected in code**, classified by when to fix. M1 already shipped (ROADMAP M1 exit criteria met, 110 tests green, smoke run 322 ms). None of these block M1.

## Tier A — fix before starting F2 (cheap hardening, ~½ day total)

These close real gaps that F2 will hit immediately. Land as atomic conventional commits.

### A1. SQLite `busy_timeout` on both connections — F1.9 critic #2

`src/pipeline/state/manifest.py:85-89` and `src/pipeline/llm/cache.py:85-89`: after `sqlite3.connect(...)` add `conn.execute("PRAGMA busy_timeout = 5000;")`. Default is 0 ms → `SQLITE_BUSY` under F2's concurrent writer loop.

**Verify:** add `test_busy_timeout_set` that reads `PRAGMA busy_timeout` and asserts `5000`.

### A2. `runs.batch_id` FK → `ON DELETE CASCADE` — F1.9 critic #5

`src/pipeline/schemas/manifest.py:47`: change `REFERENCES batches(batch_id)` to `REFERENCES batches(batch_id) ON DELETE CASCADE`. Free to change now (`runs` empty). Paired with `delete_batch` retry path, which today would throw `IntegrityError` the first time Silver writes a run and Bronze retries.

**Verify:** extend `test_manifest.py::test_delete_batch_cascades_to_runs` (insert batch + stub run, delete batch, assert run gone).

### A3. CLI path traversal guard — F1.6 security #4

`src/pipeline/cli/ingest.py` `ingest(...)`: `source = source.resolve(); bronze_root = bronze_root.resolve()` right after parameter binding, then reuse the helper from `settings.py:56-68` (`_reject_path_traversal`) to reject `..` in the resolved form. Click's `exists=True` on `--source` is not a traversal guard.

**Verify:** `test_cli_rejects_traversal` via CliRunner with `--bronze-root ../../etc`.

### A4. Broad-exception wrap in `_run_ingest` — F1.6 critic #5

`src/pipeline/cli/ingest.py:135` currently `except PipelineError`. Add a second `except Exception as exc:` branch that calls `manifest.mark_failed(error_type=type(exc).__name__, error_message=str(exc))` and re-raises wrapped in `PipelineError`. Prevents orphan `IN_PROGRESS` rows on `PolarsError` / `OSError`.

**Verify:** `test_cli_marks_failed_on_unexpected_exception` by monkeypatching `transform_to_bronze` to raise `RuntimeError`.

### A5. `_first_text_block` length cap — F1.7 security #6

`src/pipeline/llm/client.py:332` helper: after extracting `text`, cap to `settings.pipeline_llm_response_cap` (new setting, default 200 000 chars) and raise `LLMCallError` if exceeded. Prevents a hostile upstream from poisoning cache rows beyond the SQLite page cache.

**Verify:** `test_client_rejects_oversize_response`.

### A6. Document 48-bit `batch_id` collision bound — Pre-F1.6 measurement

`.specs/features/F1/DESIGN.md §4`: add a paragraph explaining `sha256[:12]` → 48 bits → birthday bound ~2²⁴ unique batches (~16M) is comfortable for M1/M2 volume; bump to `[:16]` if pipeline ever crosses that.

**Verify:** doc-only, no test.

## Tier B — fix as F2's first commit (before any Silver transform)

These are the three carry-forward items F1.9 flagged. Do them in F2 ordering, not now.

### B1. `runs` CRUD on `ManifestDB` — F1.9 critic #1

Add to `src/pipeline/state/manifest.py`:

- `insert_run(run_id, batch_id, layer, started_at) -> None`
- `mark_run_completed(run_id, finished_at, rows_in, rows_out) -> None`
- `mark_run_failed(run_id, finished_at, error_type, error_message) -> None`
- `get_runs_for_batch(batch_id) -> list[RunRow]`

Reuse the `_transaction` + `_update_status` patterns; add a `RunRow` dataclass mirroring `BatchRow`. Tests parallel to `test_manifest.py`'s batch flow.

### B2. Lineage-column helper — F1.9 M1-close #2

Extract the `batch_id + ingested_at + source_file_hash` literal injection in `src/pipeline/ingest/transform.py:55-61` into `pipeline.ingest.lineage.add_lineage_columns(lf, *, batch_id, ingested_at, source_hash)`. `transform_to_silver` will reuse unchanged.

### B3. `cached_call` request-object — F1.9 M1-close #1

Before the first Silver LLM call, refactor `cached_call(system=, user=, model=, ...)` into `cached_call(request: LLMRequest)` where `LLMRequest` is a frozen dataclass. Opens a clean path for structured output / tool-use / `prompt_version` (Tier C) without breaking the F1 caller surface (keep a thin kwargs-forwarding shim for one release).

### B4. `prompt_version` field in cache key — F1.9 critic #4

In `compute_cache_key` (after B3 lands), add `prompt_version: str = "v1"`. Without it, iterating the Silver persona prompt replays stale cache rows silently. **Pinned invariant warning:** this changes the hash; treat as an ADR-worthy change (version the algorithm, rename function to `compute_cache_key_v2`, keep v1 readable for legacy rows, invalidate on miss).

## Tier C — deferred to M3 / F4 or declined

No action; recorded so the reviewer trail stays complete.

| Finding | Reason | Revisit trigger |
|---|---|---|
| F1.5 #1/#3 — file-level atomic rename | Acceptable for single-writer M1/M2 | Multi-process F4 agent loop |
| F1.5 #2 — `n_rows` cap on `scan_parquet` | Source is repo-internal | Ingest ever accepts untrusted input |
| F1.6 #2 — `_run_ingest` split | Linear orchestration reads cleaner in one place | F4 agent loop reuses parts |
| F1.6 #3 — `standalone_mode=True` | Explicit > implicit for operator entry | Never |
| F1.6 #6 — `delete_batch` audit history | `runs` table will carry per-attempt history (B1) | After B1 lands |
| F1.7 #5 — HMAC on cache rows | DB is local-only, `pipeline_state_db` blocks `..` | Cache ever stored on shared FS |
| F1.7 #8 — 4-attempt budget surfaced to operator | Documented in docstring; F4 cost log will surface | F4 agent loop |
| F1.7 #9 — Fallback cached under primary key | Intended (replay what caller asked for) | Operator reports a replay surprise |
| F1.7 #11 — `FakeAnthropic` kwargs validation | Protocol catches first layer; F1.8 smoke pins SDK contract | SDK signature churn in F4 |
| F1.9 #3 — `cached_call` async/batched throughput | Implementation-side; public signature unchanged | F2 measured latency > M2 envelope |

## Execution order (recommended)

1. A2 → A1 → A3 → A4 → A5 → A6 (6 atomic commits; each has a test).
2. Re-run `uv run pytest` + `ruff check .` + `mypy src` after each.
3. Open F2 design doc only after Tier A is green.
4. Tier B items land inside F2's first 3 commits, in the order B1 → B2 → B3 → B4.

## Out of scope for this plan

- Any refactor that is not listed above (keeps the diff auditable against `REVIEWS/INDEX.md`).
- Deleting / rewriting the `REVIEWS/*.md` trail — it stays as historical evidence.
