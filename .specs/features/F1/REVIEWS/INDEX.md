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
