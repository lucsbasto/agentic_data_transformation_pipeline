# F1 review lane — index

Each implementation task is reviewed by three agents in parallel:
- `oh-my-claudecode:code-reviewer` — style, logic defects, SOLID, naming.
- `oh-my-claudecode:security-reviewer` — secrets, injection, path handling, deps.
- `oh-my-claudecode:critic` — adversarial: over-engineering, hidden assumptions, test smells.

Reviews live alongside this file and are summarized here.

## F1.1–F1.4 (first wave, 2026-04-22)

Scope: `3398dbc..382776c` (4 atomic commits).

| Source | Report |
|---|---|
| code-reviewer | [F1.1-F1.4-code-review.md](./F1.1-F1.4-code-review.md) |
| security-reviewer | [F1.1-F1.4-security.md](./F1.1-F1.4-security.md) |
| critic | [F1.1-F1.4-critic.md](./F1.1-F1.4-critic.md) |

### Consolidated severity and disposition

| # | Severity | Source | Finding | Disposition |
|---|---|---|---|---|
| 1 | Critical | critic | `ManifestDB` has no crash recovery — orphaned `IN_PROGRESS` rows block re-runs with `IntegrityError` | **Fixed in e24d1e4** |
| 2 | Medium | code-reviewer | `_update_status` uses `COALESCE(?, error_type)` — cannot clear stale errors when a FAILED batch transitions to COMPLETED | **Fixed in e24d1e4** |
| 3 | Medium | security | `bind_context(**kwargs)` can accidentally bind secrets; structlog has no redaction processor | **Fixed in 277d79a** |
| 4 | Medium | security | `_update_status` accepts freeform `status`; CHECK constraint catches it but leaks raw `sqlite3.IntegrityError` | **Fixed in e24d1e4** |
| 5 | Low | critic | `runs.status` has no CHECK constraint; `runs` table has no indexes | **Fixed in e24d1e4** |
| 6 | Low | security | `pipeline_state_db` relative path not guarded against `..` segments | **Fixed in 277d79a** |
| 7 | Low | code-reviewer | `for k in row.keys()` trips `ruff SIM118` | **Fixed in e24d1e4** |
| 8 | Low | critic | Tautology tests; `test_enums_reject_unknown_value` uses bare `except Exception` | **Fixed in 61989c1** (enum-rejection routed through `collect_bronze`/`SchemaDriftError`) |
| 9 | Nitpick | critic | `pl.Enum` closures leave zero growth room for new WhatsApp message types | **Deferred** — enum-as-drift-sentinel is intentional; revisit if drift fires in M2 |
| 10 | Nitpick | critic | "Self-healing" claim has zero implementation seeds in M1 | **Accepted** — claim belongs to F4 (M3) |

## F1.5 (Bronze ingest pipeline, 2026-04-22)

Scope: commit `61989c1`.

| Source | Report |
|---|---|
| code-reviewer | [F1.5-code-review.md](./F1.5-code-review.md) |
| security-reviewer | [F1.5-security.md](./F1.5-security.md) |
| critic | [F1.5-critic.md](./F1.5-critic.md) |

### Consolidated severity and disposition

| # | Severity | Source | Finding | Disposition |
|---|---|---|---|---|
| 1 | Medium | code-reviewer | `writer.py:64-66` non-atomic directory replace: `shutil.rmtree(final_dir)` then `tmp_dir.replace(final_dir)` loses both copies on a crash between the two lines | **Deferred to M3** — acceptable for single-process M1; document in F4 before multi-writer work |
| 2 | Low | security | No `n_rows` cap on parquet ingestion → OOM on malicious input | **Deferred** — source files are repo-internal; add `max_rows` config when ingest opens to untrusted sources |
| 3 | Low | security | Theoretical TOCTOU between `rmtree` and `replace` if multiple processes ever share `bronze_root` | **Deferred to M3** — paired with #1 |
| 4 | Later-pain | critic | `wrap_cast_error` is exported but only called from a test — dead in production until wired | **Fixed** — replaced with `collect_bronze(lf)` helper used by the integration flow and all future collect sites |
| 5 | Later-pain | critic | Retry-after-FAILED collides on PK; `reset_stale` only sweeps `IN_PROGRESS` | **Fixed** — added `ManifestDB.delete_batch(batch_id)` and wired the retry helper to drop a prior non-COMPLETED row before re-insert; covered by `test_ingest_retries_after_failed_row` |
| 6 | Nitpick | critic | Commit message says 48 rows; fixture is 96 | **Accepted** — future commit messages measure the generated fixture before claiming a count |
| 7 | Confirmed | critic | Polars Enum cast raises `InvalidOperationError` on unknown value (no silent null) — good | — |
| 8 | Contract flip | critic vs measurement | Critic assumed parquet round-trip drops Enum metadata; measurement shows Polars 1.x **preserves** Enum | **Pinned** — `test_write_bronze_round_trip_preserves_enum_dtype` locks the positive contract so Silver can rely on Enum dtype without re-casting |

### Pre-F1.6 measurements still owed

- [ ] Measure 153K-row zstd write time on the real parquet.
- [ ] Document 48-bit `batch_id` collision bound in the DESIGN.md.
- [ ] Smoke-test `collect_bronze` against the real parquet with a poisoned row.
- [ ] File-level atomic-rename prototype for multi-process F4 (from finding #1).

### What the reviewers flagged as well-done

- Lazy-first Polars usage (`scan_parquet` everywhere, single `.collect()` at the sink).
- Deterministic `batch_id` derivation hashed from content + mtime.
- Schema assertion at write time (`assert_bronze_schema`).
- Comprehensive enum-rejection regression test.
- All SQL parameter-bound; `SecretStr`/redaction processor upstream.

## F1.6 (CLI + LLMClient stub, 2026-04-22)

Scope: commits `2c3d2f2` and `f4fe25d`.

| Source | Report |
|---|---|
| code-reviewer | [F1.6-code-review.md](./F1.6-code-review.md) |
| security-reviewer | [F1.6-security.md](./F1.6-security.md) |
| critic | [F1.6-critic.md](./F1.6-critic.md) |

### Consolidated severity and disposition

| # | Severity | Source | Finding | Disposition |
|---|---|---|---|---|
| 1 | Medium | code-reviewer | `LLMResponse` docstring said `cache_hit` was deferred to F1.7 but the field was already present | **Fixed** — rewrote docstring to name the F1.7 additions (`retry_count`, `cost_usd_estimate`, `actual_model_used`, `latency_ms`) and added `kw_only=True` so those additions do not break existing callers |
| 2 | Medium | code-reviewer | `_run_ingest` at 87 lines exceeds the ~30-line skill target | **Accepted** — linear orchestration is cleanest to read in one place for M1; split when F4's agent loop reuses parts of it |
| 3 | Low | code-reviewer | `standalone_mode=True` in `cli(...)` call is the click default | **Accepted** — explicit is louder than implicit for the one-person-team operator entry |
| 4 | Low | security | `--bronze-root` accepts `..` segments and is not canonicalized before `mkdir` | **Deferred** — operator-controlled path in M1 scope; revisit if the CLI is ever exposed to untrusted operators |
| 5 | Later-pain | critic | Non-`PipelineError` exceptions (PolarsError, OSError) bypass `mark_failed`, leaving orphan `IN_PROGRESS` rows and surfacing raw tracebacks | **Deferred** — `reset_stale` handles the orphan on next open; a broad `try/except Exception` around the ingest step can wait until we observe a real non-PipelineError in production logs |
| 6 | Later-pain | critic | `delete_batch` drops the FAILED row's audit metadata instead of keeping history | **Deferred to M3** — `runs` table (already declared) will carry per-attempt history when the agent loop writes there |
| 7 | Later-pain | critic | `LLMResponse` positional construction will break when F1.7 adds fields | **Fixed** — dataclass now `kw_only=True` |
| 8 | Nitpick | critic | TOCTOU race on `is_batch_completed` → `insert_batch` under concurrent processes | **Accepted** — SQLite PK + `reset_stale` makes the second process crash loudly rather than corrupt; M1 is single-operator anyway |
| 9 | Nitpick | critic | Real 153K-row parquet never exercised before F1.8 | **Scheduled** — F1.8 smoke run executes the CLI against `data/raw/conversations_bronze.parquet` and records timing in STATE.md |

## F1.7 (LLMCache + LLMClient, 2026-04-22 → 2026-04-23)

Scope: commits `a5f7b00` (cache) and `515e0da` (client).

| Source | Report |
|---|---|
| code-reviewer | [F1.7-code-review.md](./F1.7-code-review.md) |
| security-reviewer | [F1.7-security.md](./F1.7-security.md) |
| critic | [F1.7-critic.md](./F1.7-critic.md) |

### Consolidated severity and disposition

| # | Severity | Source | Finding | Disposition |
|---|---|---|---|---|
| 1 | High | code-reviewer | Cache-key divergence on `-0.0` vs `0.0` — `f"{-0.0:.6f}"` produces `"-0.000000"` | **Fixed** — added `temperature + 0.0` normalization and a regression test |
| 2 | Medium | code-reviewer | `_AnthropicLike.messages: Any` defeats Protocol type checking | **Fixed** — introduced nested `_MessagesLike(Protocol)` with a typed `create()` |
| 3 | Medium | code-reviewer | `INSERT OR IGNORE` silently drops token-count corrections | **Fixed** — docstring now explicitly states the "first write wins" contract and points callers at `invalidate` to replace |
| 4 | Medium | security | `invalidate(prefix=)` does not escape LIKE metacharacters (`%`, `_`) | **Fixed** — prefix is now escaped and the LIKE uses `ESCAPE '\\'`; regression test covers `%` and `_` prefixes |
| 5 | Medium | security | Cache-as-oracle — SQLite row is trusted without an integrity check | **Accepted** — DB is local-only, `pipeline_state_db` blocks `..` traversal; HMAC hardening pass can wait |
| 6 | Low | security | `_first_text_block` returns raw provider text, no length cap or control-char strip | **Accepted** — structlog `JSONRenderer` encodes control chars when logging; cache stores raw for replay fidelity |
| 7 | Low | security | `LLMCache` opens without WAL pragma | **Fixed** — `PRAGMA journal_mode=WAL` on non-`:memory:` connections |
| 8 | Later-pain | critic | Retry budget = 3 means 4 SDK attempts (3 primary + 1 fallback); operator cost model needs to know | **Accepted** — documented in the client docstring; F4 agent-loop will surface this in the cost log |
| 9 | Later-pain | critic | Fallback response is cached under the *primary* model's key — an explicit `model=fallback` re-call would miss | **Accepted** — intended: the cache replays what the caller asked for, not what the SDK actually served |
| 10 | Later-pain | critic | Empty-text response (tool-use-only) silently cached | **Fixed** — `_one_call` now raises `LLMCallError` instead of caching an empty response; regression test added |
| 11 | Nitpick | critic | `FakeAnthropic.messages.create` accepts any kwargs — SDK signature changes would pass CI silently | **Accepted** — the Protocol catches the first layer of drift; full SDK-contract tests belong in F1.8 smoke run |

## F1.9 — M1 close (2026-04-23)

Holistic pass across the entire F1 diff (`3398dbc..a77dd76`). Reports:

| Source | Report |
|---|---|
| reviewer-pipeline (M1 close) | [F1.9-m1-close.md](./F1.9-m1-close.md) |
| critic (M1 close) | [F1.9-critic.md](./F1.9-critic.md) |

**Combined verdict:** SHIP M1. All ROADMAP M1 exit criteria met. No blocking findings.

### Carry-forward items for F2 (the three things that will trip us up first)

1. **`LLMClient.cached_call` signature expansion** — Silver will need structured output / batch / streaming. Plan a request-object refactor before the first Silver LLM call lands.
2. **Lineage-column helper** — `transform_to_bronze` duplicates `batch_id + ingested_at + source_file_hash`. Extract before writing `transform_to_silver`.
3. **`runs` table API** — DDL exists but `ManifestDB` has no writers. F2 Silver runs need `insert_run` / `mark_run_completed` / `mark_run_failed`.

### Invariants that must NOT change without a deliberate ADR

1. Bronze `pl.Schema` + Enum closed sets (`src/pipeline/schemas/bronze.py`).
2. `batches` table DDL (`src/pipeline/schemas/manifest.py`). Silver/Gold depend on the `batch_id` FK.
3. `compute_cache_key` hash algorithm (`src/pipeline/llm/cache.py`). Any change invalidates every cached LLM response; version the algorithm if ever altered.

### Single prototype to de-risk F2

Build a 100-line `SilverOrchestrator` that reads one Bronze partition, sends 5 conversations to `LLMClient.cached_call` with a real `qwen3-max` call, writes one Silver parquet. That one script forces resolution of: Enum round-trip in practice, prompt template shape, SQLite WAL contention when both `ManifestDB` and `LLMCache` hold the file, `runs` table writers, and actual `qwen3-max` latency measurement. Cheaper than discovering any of those in-flight during F2.
