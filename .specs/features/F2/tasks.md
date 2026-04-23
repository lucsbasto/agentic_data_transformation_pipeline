# F2 — Tasks: Silver transforms

Atomic, commit-sized tasks. Each task ships as its own git commit following the F1 convention (Conventional Commits, `feat(F2)`/`docs(F2)`/`test(F2)`/`refactor(F2)`).

Legend: ✅ done · 🟡 in progress · ⚪ pending

| ID | Task | Deps | Verify | Status |
|---|---|---|---|---|
| **F2.0** | Create `.specs/features/F2/{spec,design,tasks}.md` and update ROADMAP + STATE | — | files exist; `git status` clean | ⚪ |
| **F2.1** | Add `schemas/silver.py` with `METADATA_STRUCT` and `SILVER_SCHEMA`. LEARN comments on every type choice. | F2.0 | `pytest tests/unit/test_schemas_silver.py -q` (new) passes; schema round-trips through a fabricated `pl.DataFrame` | ⚪ |
| **F2.2** | Extend `Settings` with `pipeline_lead_secret: SecretStr` (required, no default). Document env var in `.env.example`. | F2.0 | `pytest tests/unit/test_settings.py -q` passes (new case: missing secret raises) | ⚪ |
| **F2.3** | Extend `schemas/manifest.py` + `state/manifest.py`: add columns `output_path TEXT` and `rows_deduped INTEGER` to the existing `runs` table (via idempotent `ALTER TABLE` guarded by `PRAGMA table_info`). Introduce layer-parameterized helpers on `ManifestDB`: `insert_run`, `mark_run_completed`, `mark_run_failed`, `get_run`, `get_latest_run`, `is_run_completed`, `delete_runs_for`, and extend `reset_stale_runs` to sweep any layer. `RunRow` dataclass mirrors the updated table. | F2.0 | `pytest tests/unit/test_manifest_runs.py -q` covers: add-column migration is idempotent on an existing F1 DB, insert→complete→get, insert→fail→get, stale sweep per layer, cascade on parent delete, `is_run_completed(batch_id, 'silver')` returns the right bool. | ⚪ |
| **F2.4** | `silver/pii.py` — compile regexes, write pure maskers for email / CPF / phone / CEP / plate. LEARN comments explaining the positional masking choice. | F2.0 | `pytest tests/unit/test_silver_pii.py -q` — fixture table of inputs/outputs for each type, including negatives (no false positives on normal text) | ⚪ |
| **F2.5** | `silver/normalize.py` — `parse_timestamp_utc`, `parse_metadata`, `normalize_name`. LEARN comments on UTC handling and NFKD accent stripping. | F2.0 | `pytest tests/unit/test_silver_normalize.py -q` — unit cases for timezone attach, malformed metadata → null, accent/case normalization | ⚪ |
| **F2.6** | `silver/lead.py` — HMAC-based `lead_id_expr`. LEARN comments on HMAC vs hash, secret handling. | F2.2 | `pytest tests/unit/test_silver_lead.py -q` — same phone → same id, different phone → different id, id length 16, id depends on secret | ⚪ |
| **F2.7** | `silver/dedup.py` — priority-rank dedup on `(conversation_id, message_id)`. LEARN comments on why `group_by→first` beats `unique(subset=...)`. | F2.4, F2.5 | `pytest tests/unit/test_silver_dedup.py -q` — duplicate row with `read`/`delivered`/`sent` collapses to the `read` row; tie-break on timestamp works | ⚪ |
| **F2.8** | `silver/reader.py` + `silver/writer.py` + `silver/transform.py` orchestrator. Lazy plan wiring, streaming collect, partitioned write. | F2.1, F2.4-F2.7 | `pytest tests/unit/test_silver_reader.py tests/unit/test_silver_writer.py tests/unit/test_silver_transform.py -q` | ⚪ |
| **F2.9** | `cli/silver.py` — `transform-silver --batch-id` click command. Mirror F1 `cli/ingest.py` structure: safe-resolve, context bind, typed-error rescue. Register in `__main__.py`. | F2.3, F2.8 | `pytest tests/unit/test_main.py -q` covers `--help` includes the new subcommand | ⚪ |
| **F2.10** | Integration test `tests/integration/test_silver_e2e.py` — run `pipeline ingest` then `pipeline transform-silver` via click's `CliRunner` on the shared tiny fixture; assert Silver parquet written, no raw PII, manifest `silver_runs` row COMPLETED, second run is a no-op. | F2.9 | `pytest tests/integration/test_silver_e2e.py -q` passes | ⚪ |
| **F2.11** | Smoke run on 153k-row fixture. Record timing + Silver parquet size. Update `.specs/project/ROADMAP.md` with F2 shipped status. Update `MEMORY.md` hot paths if needed. | F2.10 | `time uv run pipeline transform-silver --batch-id <bid>` finishes under RNF-07 budget; write numbers into ROADMAP cell | ⚪ |
| **F2.12** | Lint / type / coverage sweep. `ruff check`, `ruff format --check`, `mypy --strict src tests`, `pytest --cov=src/pipeline/silver --cov=src/pipeline/schemas/silver --cov-fail-under=90`. Fix anything that surfaces. | F2.11 | All four commands green | ⚪ |
| **F2.13** | Multi-agent review lane (code-reviewer + security-reviewer + critic) on the F2 diff. Apply blocking findings. Archive reports under `.specs/features/F2/REVIEWS/`. | F2.12 | Each reviewer signs off or blocker closed with fixup commit | ⚪ |

## Commit map

One commit per atomic task, except where indicated below:

- F2.0 → `docs(F2): scaffold spec/design/tasks and update roadmap`
- F2.1 → `feat(F2): add silver schema and metadata struct`
- F2.2 → `feat(F2): wire PIPELINE_LEAD_SECRET through settings`
- F2.3 → `feat(F2): extend manifest runs table with output_path and rows_deduped`
- F2.4 → `feat(F2): positional PII maskers (email/cpf/phone/cep/plate)`
- F2.5 → `feat(F2): silver normalizers (timestamp/metadata/name)`
- F2.6 → `feat(F2): lead_id via hmac-sha256 polars expression`
- F2.7 → `feat(F2): priority-rank dedup on conversation+message`
- F2.8 → `feat(F2): silver reader/writer/transform orchestrator` (may split if diff > 400 LOC)
- F2.9 → `feat(F2): transform-silver CLI command`
- F2.10 → `test(F2): end-to-end silver integration test via CliRunner`
- F2.11 → `chore(F2): smoke run timings and roadmap update`
- F2.12 → `chore(F2): ruff/mypy/coverage sweep`
- F2.13 → `docs(F2): archive multi-agent review sign-off`

## Definition of done (feature-level)

- All 14 tasks complete and checked off above.
- `spec.md` acceptance criteria 1-8 all pass.
- ROADMAP shows F2 ✅ shipped with measured numbers (row count, dedup delta, duration, parquet size).
- M2 critical path to F3 is unblocked.

## Pause gates

- **After F2.0** (spec/design/tasks written) — user reviews, approves or adjusts before any code is written.
- **After F2.7** (core pure modules done, nothing I/O-touching yet) — user sees the building blocks before we wire the CLI.
- **After F2.10** (integration green) — user sees the e2e work before the sweep + review lane.
