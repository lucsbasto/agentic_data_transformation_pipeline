---
name: python-best-practices
description: Python best practices for this repo. Use when writing, editing, or reviewing any .py file. Covers typing, project layout, tooling (ruff, mypy, pytest), logging, errors, and async patterns. Trigger keywords: python, .py, pyproject, ruff, mypy, pytest, type hint, dataclass, pydantic.
---

# Python best practices

Binding rules for `.py` code in this repo. Skip only with explicit justification in the diff.

## 1. Project layout

- Source under `src/<package_name>/` (src layout). Tests under `tests/` mirroring package.
- `pyproject.toml` is single source of truth for deps, tool config, and metadata. No `setup.py`, no `requirements.txt` (use `uv pip compile` or Poetry lock if needed).
- Package name = snake_case, matches PRD module. This repo uses `pipeline/`.
- One responsibility per module. If a file passes ~400 lines, split.

## 2. Python version + typing

- Target Python **3.12+** (use modern syntax: `match`, `|` unions, `type` statement).
- **Type hints everywhere** public. Internal helpers may skip if trivial and local.
- Prefer `from __future__ import annotations` in every module (cheaper runtime).
- Use `TypedDict`, `Protocol`, `Literal`, `Final`, `assert_never` over `Any`.
- Dataclasses (`@dataclass(slots=True, frozen=True)`) for plain records. Pydantic v2 (`BaseModel`) for external I/O validation (LLM JSON output, config files, HTTP).
- **Never** use bare `Exception:` in `except`. Catch specific types.

## 3. Standard tooling

| Tool | Role | Config location |
|---|---|---|
| `ruff` | Lint + format (replaces black, isort, flake8, pyupgrade) | `pyproject.toml [tool.ruff]` |
| `mypy` or `pyright` | Static typing (strict mode) | `pyproject.toml [tool.mypy]` |
| `pytest` | Tests | `pyproject.toml [tool.pytest.ini_options]` |
| `pytest-cov` | Coverage | same |
| `uv` | Package manager (fast, replaces pip + pip-tools) | lockfile `uv.lock` |

Ruff rules to enable at minimum: `E,F,W,I,N,UP,B,C4,SIM,RUF,ANN,PTH,PL,TRY`.

## 4. Imports

- Absolute imports only (no `from .foo import bar` across top-level modules; relative OK inside a sub-package).
- Import order handled by ruff (isort rules).
- No `from module import *`.
- Import heavy deps (polars, pandas) at module top, not inside functions, unless genuinely optional.

## 5. Naming

- `snake_case` for functions, variables, modules.
- `PascalCase` for classes.
- `UPPER_SNAKE` for module-level constants.
- Private helpers prefixed `_`.
- Boolean names start with `is_`, `has_`, `should_`.

## 6. Functions

- Short (~30 lines). Single responsibility. Pure where possible.
- Positional args ≤ 3. Rest keyword-only (use `*,`).
- Return early. Avoid nested `if`.
- No mutable default args (`def f(x=[])` → `def f(x=None)` then assign).

## 7. Errors

- Custom exception hierarchy rooted at a single `PipelineError(Exception)` per package.
- `raise FooError("msg") from original_exc` to preserve chain.
- Don't swallow. Don't log-and-ignore. Either handle (recover/fallback) or propagate.
- `try` block contains only the risky line. No giant try blocks.

## 8. Logging

- Use `structlog` (JSON-ready, bound context) — per PRD ADR-005.
- Never `print()` in library code. CLI entrypoints may print user-facing output only.
- Log at boundaries (I/O, LLM calls, retries, failures). Not inside hot loops.
- Include correlation ids (`run_id`, `batch_id`, `conversation_id`) in every log.

## 9. Testing

- `pytest` + `pytest-cov`. Minimum 80% line coverage on non-IO code.
- Fixtures in `conftest.py`. Share small fixtures; avoid god-fixtures.
- Parametrize instead of duplicating tests.
- Use `tmp_path` for filesystem, `monkeypatch` for env, `responses`/`respx` or recorded cassettes for HTTP.
- Mock the LLM client at the boundary, not internals. Test prompts = snapshot tests.

## 10. Async

- Use `asyncio` only where genuinely concurrent (parallel LLM calls, parallel reads). Single-file pipeline steps stay sync.
- Never mix sync and async in the same call stack without `anyio` or explicit bridge.
- Avoid `asyncio.run()` inside libraries; expose coroutines, let caller schedule.

## 11. Filesystem

- Always `pathlib.Path`, never `os.path`.
- All paths relative to project root via a single `settings.project_root`. No hardcoded absolute paths.

## 12. Configuration

- Env vars loaded via `pydantic-settings` (or `os.environ` + explicit validation at startup).
- Fail fast on missing/invalid config; don't paper over with defaults that hide bugs.
- `.env.example` documents every var.

## 13. Anti-patterns (do not)

- No global mutable state. Pass dependencies explicitly.
- No reaching into `_private` attributes of third-party libs.
- No `sys.path` manipulation.
- No `eval` / `exec` on untrusted input.
- No silent `except: pass`.
- No comments that repeat code. Comments explain *why*, not *what*.
