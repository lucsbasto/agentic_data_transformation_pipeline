# F1 — Tasks: Bronze ingest + LLMClient base

## Status Legend
- ✅ Complete
- ⚪ Pending
- 🔄 In Progress

## Tasks

- [x] **F1.1** — Bootstrap pyproject.toml + package skeleton
  - Create pyproject.toml with dependencies from §8
  - Create src/pipeline/ package structure
  - Run uv lock && uv sync
  - Status: ✅

- [x] **F1.2** — Settings plumbing
  - Create src/pipeline/settings.py with pydantic-settings
  - Add .env.example with required vars from §7
  - Write tests for settings loading
  - Status: ✅

- [x] **F1.3** — Logging setup
  - Configure structlog JSON formatter
  - Add logger factory in src/pipeline/logging.py
  - Write tests for log output format
  - Status: ✅

- [x] **F1.4** — Schema definitions
  - Define BRONZE_SCHEMA in src/pipeline/schemas/bronze.py
  - Create manifest DDL in src/pipeline/schemas/manifest.py
  - Status: ✅

- [x] **F1.5** — Manifest DB implementation
  - Create src/pipeline/state/manifest.py with ManifestDB class
  - Implement batch operations (insert, update, query)
  - Write unit tests with SQLite in-memory
  - Status: ✅

- [x] **F1.6** — Ingest pipeline
  - Create src/pipeline/ingest/reader.py (scan_parquet)
  - Create src/pipeline/ingest/transform.py (string→typed casts)
  - Create src/pipeline/ingest/writer.py (partitioned write)
  - Write unit tests for each component
  - Status: ✅

- [x] **F1.7** — CLI entrypoint
  - Create src/pipeline/cli/ingest.py with Click commands
  - Create src/pipeline/__main__.py
  - Wire up settings → manifest → ingest flow
  - Status: ✅

- [x] **F1.8** — Integration test
  - Create test_ingest_end_to_end.py
  - Use tests/fixtures/tiny_conversations.parquet
  - Verify batch tracking and file output
  - Status: ✅

- [x] **F1.9** — LLM cache
  - Create src/pipeline/llm/cache.py with SQLite cache
  - Implement cache hit/miss logic
  - Write unit tests
  - Status: ✅

- [x] **F1.10** — LLM client skeleton
  - Create src/pipeline/llm/client.py with cached_call method
  - Stub for Anthropic SDK integration
  - Write basic tests
  - Status: ✅

- [x] **F1.11** — Documentation + linting
  - Create README.md with install/run instructions
  - Add ruff + mypy config to pyproject.toml
  - Verify linting passes
  - Status: ✅

- [x] **F1.12** — End-to-end smoke test
  - Run pipeline on real parquet data
  - Capture timing metrics
  - Document in STATE.md
  - Status: ✅