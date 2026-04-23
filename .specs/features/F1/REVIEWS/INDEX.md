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
| 1 | Critical | critic | `ManifestDB` has no crash recovery — orphaned `IN_PROGRESS` rows block re-runs with `IntegrityError` | **Fix now** (pre-F1.5) |
| 2 | Medium | code-reviewer | `_update_status` uses `COALESCE(?, error_type)` — cannot clear stale errors when a FAILED batch transitions to COMPLETED | **Fix now** |
| 3 | Medium | security | `bind_context(**kwargs)` can accidentally bind secrets; structlog has no redaction processor | **Fix now** |
| 4 | Medium | security | `_update_status` accepts freeform `status`; CHECK constraint catches it but leaks raw `sqlite3.IntegrityError` | **Fix now** |
| 5 | Low | critic | `runs.status` has no CHECK constraint; `runs` table has no indexes | **Fix now** (cheap, batched) |
| 6 | Low | security | `pipeline_state_db` relative path not guarded against `..` segments | **Fix now** (cheap, batched) |
| 7 | Low | code-reviewer | `for k in row.keys()` trips `ruff SIM118` in a file-level ignore scope (still visible in review) | **Fix now** (one-line) |
| 8 | Low | critic | Tautology tests in `test_errors.py`; `test_enums_reject_unknown_value` uses bare `except Exception` | **Fix now** (tighten assertion) |
| 9 | Nitpick | critic | `pl.Enum` closures leave zero growth room for new WhatsApp message types | **Defer** — enum-as-drift-sentinel is intentional per D-002 skill `medallion-data-layout`; revisit if drift fires in M2 |
| 10 | Nitpick | critic | "Self-healing" claim has zero implementation seeds in M1 | **Accept** — claim belongs to F4 (M3); M1 delivers only the foundation |

### Pre-F1.5 measurements requested by critic

- [ ] Verify exact Polars exception type for `pl.Enum` cast failures (decides how `SchemaDriftError` wraps it).
- [ ] Prototype atomic-rename across mount points (Bronze writer uses `tmp → final` rename; fails silently across filesystem boundaries).
- [ ] Measure 153K-row zstd write time (set expectation for M2 full-load budget < 15 min).
- [ ] Document 48-bit `batch_id` collision bound (we truncate `sha256(source_hash + mtime)` to 12 hex chars).

These become part of the F1.5 design note, not pre-work commits.

### What the reviewers flagged as well-done

- SecretStr for API key; `.env.example` placeholder; `.env` gitignored.
- All SQL uses `?` parameter binding; FK enforcement via PRAGMA.
- Transactional BEGIN/COMMIT/ROLLBACK wrapper.
- 98.67% coverage without coverage-gaming.
- `BatchRow` as frozen slots dataclass with `is_completed` / `is_failed` helpers.
