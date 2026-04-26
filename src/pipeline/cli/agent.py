"""``pipeline agent`` Click subcommand (F4.16 + WIRING-1/-2).

Exposes ``run-once`` and ``run-forever`` so operators can drive the
self-healing loop without writing any Python. Runner adapters and
the per-kind fix dispatcher live in :mod:`pipeline.agent.runners`;
this module is the thin Click surface that loads :class:`Settings`,
opens :class:`ManifestDB`, and assembles the loop's collaborators.

Subcommands:

- ``pipeline agent run-once`` — single iteration. Exit code 0 on
  ``COMPLETED``, 1 otherwise. Emits a one-line JSON summary on
  stdout.
- ``pipeline agent run-forever`` — repeats ``run-once`` every
  ``--interval`` seconds (default 60). ``--max-iters`` caps the
  loop count for tests / demos.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any, Final

import click

from pipeline.agent.diagnoser import _DiagnoseBudget
from pipeline.agent.diagnoser import classify as _classify
from pipeline.agent.escalator import DEFAULT_LOG_PATH, make_escalator
from pipeline.agent.executor import Classifier
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import (
    DEFAULT_LOOP_INTERVAL_S,
    run_forever,
    run_once,
)
from pipeline.agent.runners import make_fix_builder, make_runners_for
from pipeline.agent.types import ErrorKind, Layer
from pipeline.config import Settings as RuntimeSettings
from pipeline.settings import Settings
from pipeline.state.manifest import ManifestDB

DEFAULT_DIAGNOSE_BUDGET: Final[int] = 10
"""Per-``run_once`` LLM diagnose call cap (spec §7 D3)."""

_DEFAULT_SOURCE_ROOT: Final[Path] = Path("data/raw")
"""Source-root default. Not in :class:`pipeline.config.Settings` because
``data/raw`` is an upstream input, not a managed output path."""


def _field_default(name: str) -> Any:
    """Read a :class:`RuntimeSettings` field default WITHOUT instantiating
    (avoids the required ``DASHSCOPE_API_KEY`` validation at decoration
    time). Lets us keep ``--help`` output stable on a fresh checkout."""
    return RuntimeSettings.model_fields[name].default


def _load_runtime_settings() -> RuntimeSettings:
    """Load :class:`pipeline.config.Settings` (F7.2) with a friendly
    error when the required ``DASHSCOPE_API_KEY`` is missing.

    The CLI surface should never spew a Pydantic stack trace at an
    operator; we translate validation failure into a one-line
    ``click.UsageError`` so ``pipeline agent --help`` style discovery
    stays painless even on a fresh machine."""
    try:
        return RuntimeSettings()  # type: ignore[call-arg]
    except Exception as exc:  # pragma: no cover - exercised by test
        msg = str(exc).splitlines()[0] if str(exc) else exc.__class__.__name__
        raise click.UsageError(
            "agent runtime config invalid — set DASHSCOPE_API_KEY (and any "
            f"required AGENT_* envs) in your environment or .env file. ({msg})"
        ) from exc


def _make_default_classifier(diagnose_budget: int) -> Classifier:
    """Curry :func:`pipeline.agent.diagnoser.classify` with a fresh
    diagnose budget. ``llm_client=None`` keeps the CLI dependency
    surface narrow — stage 2 LLM fallback is disabled by default
    here; deterministic patterns still fire."""
    budget = _DiagnoseBudget(cap=diagnose_budget)

    def _curried(exc: BaseException, layer: Layer, batch_id: str) -> ErrorKind:
        return _classify(
            exc, layer=layer, batch_id=batch_id, llm_client=None, budget=budget
        )

    return _curried


@click.group("agent")
def agent() -> None:
    """Self-healing agent loop (observe → diagnose → act → verify)."""


def _common_options[F: Callable[..., Any]](fn: F) -> F:
    """Apply the shared layer-path + budget + lock flags to a Click
    command. Keeping them in one place avoids drifting defaults
    between ``run-once`` and ``run-forever``."""
    decorators = [
        click.option(
            "--source-root",
            type=click.Path(file_okay=False, path_type=Path),
            default=_DEFAULT_SOURCE_ROOT,
            show_default=True,
            help="Directory containing raw source parquet files.",
        ),
        click.option(
            "--bronze-root",
            type=click.Path(file_okay=False, path_type=Path),
            default=_field_default("BRONZE_ROOT"),
            show_default=True,
            help="Bronze partition root.",
        ),
        click.option(
            "--silver-root",
            type=click.Path(file_okay=False, path_type=Path),
            default=_field_default("SILVER_ROOT"),
            show_default=True,
            help="Silver partition root.",
        ),
        click.option(
            "--gold-root",
            type=click.Path(file_okay=False, path_type=Path),
            default=_field_default("GOLD_ROOT"),
            show_default=True,
            help="Gold table root.",
        ),
        click.option(
            "--manifest-path",
            type=click.Path(dir_okay=False, path_type=Path),
            default=_field_default("MANIFEST_PATH"),
            show_default=True,
            help="SQLite manifest path.",
        ),
        click.option(
            "--retry-budget",
            type=int,
            default=_field_default("AGENT_RETRY_BUDGET"),
            show_default=True,
            envvar="AGENT_RETRY_BUDGET",
            help="Retry budget per (batch_id, layer, error_class).",
        ),
        click.option(
            "--diagnose-budget",
            type=int,
            default=_field_default("AGENT_DIAGNOSE_BUDGET"),
            show_default=True,
            envvar="AGENT_DIAGNOSE_BUDGET",
            help="LLM diagnose call cap per run_once.",
        ),
        click.option(
            "--lock-path",
            type=click.Path(dir_okay=False, path_type=Path),
            default=_field_default("AGENT_LOCK_PATH"),
            show_default=True,
            envvar="AGENT_LOCK_PATH",
            help="Filesystem lock path.",
        ),
    ]
    for dec in reversed(decorators):
        fn = dec(fn)
    return fn


@agent.command("run-once")
@_common_options
def run_once_cmd(
    source_root: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    manifest_path: Path,
    retry_budget: int,
    diagnose_budget: int,
    lock_path: Path,
) -> None:
    """Run one iteration of the agent loop and exit.

    Assembles all collaborators (runners, classifier, escalator, lock), calls
    :func:`pipeline.agent.loop.run_once`, prints a JSON summary to stdout, and
    exits 0 on ``COMPLETED`` or 1 on any other status. (F4.16 / WIRING-1)
    """
    _load_runtime_settings()  # F7.2: friendly-fail on missing DASHSCOPE_API_KEY
    settings = Settings.load()
    runners_for = make_runners_for(
        source_root=source_root,
        bronze_root=bronze_root,
        silver_root=silver_root,
        gold_root=gold_root,
        settings=settings,
    )
    build_fix = make_fix_builder(
        source_root=source_root,
        bronze_root=bronze_root,
        silver_root=silver_root,
    )
    with ManifestDB(manifest_path) as manifest:
        result = run_once(
            manifest=manifest,
            source_root=source_root,
            silver_root=silver_root,
            gold_root=gold_root,
            runners_for=runners_for,
            classify=_make_default_classifier(diagnose_budget),
            build_fix=build_fix,
            escalate=make_escalator(log_path=DEFAULT_LOG_PATH, manifest=manifest),
            lock=AgentLock(lock_path),
            retry_budget=retry_budget,
        )
    click.echo(json.dumps(asdict(result), default=str))
    sys.exit(0 if result.status.value == "COMPLETED" else 1)


@agent.command("run-forever")
@_common_options
@click.option(
    "--interval",
    type=float,
    default=DEFAULT_LOOP_INTERVAL_S,
    show_default=True,
    envvar="AGENT_LOOP_INTERVAL",
    help="Seconds between iterations.",
)
@click.option(
    "--max-iters",
    type=int,
    default=None,
    help="Stop after this many iterations (default: run until SIGINT).",
)
def run_forever_cmd(
    source_root: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    manifest_path: Path,
    retry_budget: int,
    diagnose_budget: int,
    lock_path: Path,
    interval: float,
    max_iters: int | None,
) -> None:
    """Run the agent loop continuously until SIGINT or ``--max-iters`` is reached.

    Wraps :func:`pipeline.agent.loop.run_forever`; each iteration is one
    ``run-once`` cycle. Prints a JSON summary of iteration count and last
    status on exit. Use ``--interval`` to tune the sleep between cycles and
    ``--max-iters`` to cap runs in tests or demos. (F4.16 / WIRING-2)
    """
    _load_runtime_settings()  # F7.2: friendly-fail on missing DASHSCOPE_API_KEY
    settings = Settings.load()
    runners_for = make_runners_for(
        source_root=source_root,
        bronze_root=bronze_root,
        silver_root=silver_root,
        gold_root=gold_root,
        settings=settings,
    )
    build_fix = make_fix_builder(
        source_root=source_root,
        bronze_root=bronze_root,
        silver_root=silver_root,
    )
    with ManifestDB(manifest_path) as manifest:
        results = run_forever(
            manifest=manifest,
            source_root=source_root,
            silver_root=silver_root,
            gold_root=gold_root,
            runners_for=runners_for,
            classify=_make_default_classifier(diagnose_budget),
            build_fix=build_fix,
            escalate=make_escalator(log_path=DEFAULT_LOG_PATH, manifest=manifest),
            interval=interval,
            max_iters=max_iters,
            lock=AgentLock(lock_path),
            retry_budget=retry_budget,
        )
    click.echo(
        json.dumps(
            {
                "iterations": len(results),
                "last_status": results[-1].status.value if results else None,
            }
        )
    )
