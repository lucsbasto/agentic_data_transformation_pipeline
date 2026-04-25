# Agentic Data Transformation Pipeline

Self-healing Bronze → Silver → Gold data pipeline with an LLM-driven
agent loop. Built for the **Data & AI Engineering technical test**
(see `Teste Técnico de Data & AI Engineering.md`). Implementation is
at **milestone M3 (partial)** — F1–F4 shipped (Bronze ingest, Silver
enrichment, Gold persona classification, self-healing agent loop).
F5 (runner integration) and runner wiring remain as follow-ups.

## Stack

- **Python 3.12+** managed by [uv](https://docs.astral.sh/uv/) (single
  tool for interpreter, deps, lockfile, venv).
- **Polars** (lazy API) for every transform.
- **SQLite** (`state/manifest.db`) as the single-file state store:
  batch manifest, run tracking, LLM response cache.
- **Anthropic Python SDK** pointed at **DashScope**'s Anthropic-
  compatible endpoint (`qwen3-max` primary / `qwen3-coder-plus`
  fallback).
- **structlog** for JSON logs, **click** for the CLI, **pydantic /
  pydantic-settings** for config validation.

## Prerequisites

- `uv` ≥ 0.11. Install once:
  ```sh
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- Git + SSH access to clone. The raw source parquet
  (`data/raw/conversations_bronze.parquet`, 8.7 MB) is committed so no
  external download is needed.

## Quickstart

```sh
# 1. Sync deps + create .venv under ./.venv
uv sync

# 2. Copy the env template and fill in your DashScope API key.
#    SDK reads ANTHROPIC_API_KEY by default, so we reuse that name for
#    the DashScope key.
cp .env.example .env
$EDITOR .env

# 3. Run the Bronze ingest end-to-end against the real source parquet.
uv run python -m pipeline ingest
```

### Environment configuration

The `.env.example` template documents every required environment variable. Copy it to `.env` and fill in values:

```sh
cp .env.example .env
$EDITOR .env
```

Only `ANTHROPIC_API_KEY` (DashScope API key) is required; all other variables have sensible defaults. For full settings reference including validation rules, see `.specs/features/F7/tasks.md` (F7.1).

Expected output:
```
ingested 153228 rows into data/bronze/batch_id=<12hex>/part-0.parquet (batch=<id>, <ms> ms)
```

The first run creates `state/manifest.db` and writes the Bronze
partition. A second run short-circuits with
`batch <id> already ingested; nothing to do.` — ingest is idempotent
by content hash + mtime.

Once Bronze is populated, run Silver enrichment:
```sh
uv run python -m pipeline silver
```

Then Gold persona classification:
```sh
uv run python -m pipeline gold
```

Finally, start the self-healing agent loop (observe → diagnose → fix →
verify):
```sh
uv run python -m pipeline agent run-once
```

The agent loop scans for pending batches and drives them through all
three layers, injecting fault recovery and LLM-driven repairs.

## Commands

| Command | Purpose |
|---|---|
| `uv run python -m pipeline --help` | list subcommands |
| `uv run python -m pipeline ingest` | Bronze ingest (default source) |
| `uv run python -m pipeline ingest --source <path> --bronze-root <dir>` | point at a different file / output dir |
| `uv run pytest -q` | run the test suite (unit + integration) |
| `uv run ruff check .` | lint |
| `uv run ruff check --fix .` | lint with auto-fix |
| `uv run mypy src` | strict static typing |

CI enforces the 80% coverage floor baked into `pyproject.toml`.

## Environment variables

All required config lives in `.env`. The `.env.example` template
documents every variable; the binding ones are:

| Variable | Default | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | *(required)* | DashScope API key, consumed via the Anthropic SDK |
| `ANTHROPIC_BASE_URL` | `https://coding-intl.dashscope.aliyuncs.com/apps/anthropic` | DashScope's Anthropic-compatible endpoint |
| `LLM_MODEL_PRIMARY` | `qwen3-max` | strong-reasoning model for persona/extraction |
| `LLM_MODEL_FALLBACK` | `qwen3-coder-plus` | cheaper fallback on rate-limit or retryable failure |
| `PIPELINE_RETRY_BUDGET` | `3` | max retry attempts per LLM call before fallback |
| `PIPELINE_LOOP_SLEEP_SECONDS` | `60` | agent loop sleep (used in F4/F5) |
| `PIPELINE_STATE_DB` | `state/manifest.db` | SQLite path (relative paths resolve against repo root; `..` segments rejected) |
| `PIPELINE_LOG_LEVEL` | `INFO` | log level (`DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL`) |

## Repo layout

```
src/pipeline/
  __main__.py            # click group: python -m pipeline <sub>
  cli/ingest.py          # ingest subcommand
  ingest/                # scan → transform → write (Bronze)
    batch.py             # deterministic batch_id from source hash + mtime
    reader.py            # pl.scan_parquet + source-schema validation
    transform.py         # String → typed Bronze (+ Enums) + lineage cols
    writer.py            # atomic partition write, zstd + stats
  llm/                   # cached LLM client
    cache.py             # SQLite-backed LLMCache
    client.py            # Anthropic SDK → DashScope, retries + fallback
  schemas/
    bronze.py            # typed Bronze schema + closed-set Enums
    manifest.py          # SQL DDL for batches / runs / llm_cache
  state/manifest.py      # ManifestDB: lifecycle, stale-row recovery
  errors.py              # PipelineError hierarchy
  logging.py             # structlog JSON + secret redaction
  paths.py               # project_root resolution + layer paths
  settings.py            # pydantic-settings, strict env loading

tests/
  unit/                  # fast, per-module
  integration/           # end-to-end (marked @pytest.mark.integration)

data/
  raw/                   # committed source fixture
  bronze/ silver/ gold/  # generated layers (gitignored)

state/                   # manifest.db (gitignored)

.specs/
  project/               # PROJECT / ROADMAP / STATE (decision log)
  features/F1/           # DESIGN + REVIEWS/ for this milestone

.claude/
  skills/                # project-local best-practice skills
  agents/reviewer-pipeline.md   # read-only reviewer subagent
  settings.json          # Claude Code permission allowlist
```

## Project docs

- `PRD.md` — product requirements.
- `Teste Técnico de Data & AI Engineering.md` — original test statement.
- `Dicionário de Dados - Teste Técnico ...md` — source-column dictionary.
- `.specs/project/STATE.md` — short mutable decision log (D-001..).
- `.specs/project/ROADMAP.md` — milestones and feature order.
- `.specs/features/F1/DESIGN.md` — design of the foundation milestone.
- `.specs/features/F1/REVIEWS/` — per-task review lane
  (code-reviewer + security-reviewer + critic outputs).

## Testing philosophy

- Every public function has a test.
- Integration tests live under `tests/integration/` with a
  `pytest.mark.integration` marker; they use isolated `tmp_path`
  state, so a local run never touches the real `state/` directory.
- The LLM client tests mock the Anthropic SDK via a fake protocol
  implementation — no network, no API cost in CI.
- Coverage floor: 80% (gate configured in `pyproject.toml`).

## Status / next steps

- **M1 / F1 (foundation)**: ✅ shipped — Bronze ingest + LLMClient base.
- **M2 / F2–F3**: ✅ shipped — Silver enrichment + Gold persona classification.
- **M3 / F4**: ✅ shipped — self-healing agent loop (observe → diagnose →
  act → verify).
- **M3 / F5 (follow-up)**: 🟡 deferred — runner integration (post-M3).
- **M4**: tests/CI, auto-correction demo, optional Databricks runner.

See `.specs/project/ROADMAP.md` for the full milestone plan.
