# Pipeline API Reference

The pipeline is organized as a modular architecture: Bronze (ingestion) -> Silver (transformation) -> Gold (analytics), plus an agent loop that orchestrates self-healing, an LLM client for agentic decisions, a state store for runtime data, and exception types for error handling.

## Modules

| Module | Purpose | Documentation |
|--------|---------|---|
| `pipeline.ingest` | Read raw data, transform to Bronze layer, write parquet. | _planned_ |
| `pipeline.silver` | Transform Bronze to Silver: dedup, normalization, PII masking, lead_id derivation. | [silver.md](silver.md) |
| `pipeline.gold` | Transform Silver to Gold: conversation scores, lead profiles, agent performance, competitor intelligence, and insights JSON. | [gold.md](gold.md) |
| `pipeline.agent` | Self-healing agent loop: observer, planner, executor, diagnoser, escalator. | [agent.md](agent.md) |
| `pipeline.llm` | Anthropic-compatible LLM client with caching and fallback retry logic. | [llm.md](llm.md) |
| `pipeline.state.manifest` | SQLite-backed runtime state store. | [manifest.md](manifest.md) |
| `pipeline.schemas` | Schema declarations for all data layers and the state store. | _planned_ |
| `pipeline.errors` | Exception hierarchy for precise error handling. | [errors.md](errors.md) |
| `pipeline.cli` | Click-based command-line interface. | [../usage/cli.md](../usage/cli.md) |

## Public API Stability

Public API consists of names explicitly re-exported from each module's `__all__` declaration. Everything else—functions, classes, and submodules prefixed with `_`—is private and subject to change without notice.

When a public symbol moves between submodules, the re-export in `__init__.py` keeps downstream callers unchanged. For example, if `pipeline.ingest.scan_source` moves to `pipeline.ingest.reader`, callers importing from `pipeline.ingest` are unaffected.

## Imports

Use top-level module imports where symbols are re-exported:

```python
# Good: stable, refactor-safe
from pipeline.ingest import scan_source, transform_to_bronze
from pipeline.agent import run_once, AgentResult
from pipeline.llm import LLMClient
from pipeline.errors import PipelineError

# Avoid: reaches into private submodules
from pipeline.agent.loop import run_once  # Use from pipeline.agent instead
from pipeline.llm.cache import LLMCache   # Use from pipeline.llm instead
```

## Version

Current version: `pipeline.__version__` (kept in sync with `pyproject.toml`).
