"""``pipeline agent`` Click subcommand (F4.16).

Exposes ``run-once`` and ``run-forever`` so operators can exercise
the F4 loop without writing any Python. The runner factory is
intentionally minimal in this commit — `runners_for(batch_id)`
returns an empty mapping, so the loop walks the source / manifest
delta and emits structured events but does NOT actually invoke the
F1/F2/F3 entrypoints. Real layer wiring is a follow-up task once
the F2/F3 idempotent run helpers exist.

The `agent` group registers two commands:

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
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Final

import click

from pipeline.agent.diagnoser import _DiagnoseBudget
from pipeline.agent.diagnoser import classify as _classify
from pipeline.agent.escalator import DEFAULT_LOG_PATH, make_escalator
from pipeline.agent.executor import DEFAULT_RETRY_BUDGET, Classifier
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import (
    DEFAULT_LOOP_INTERVAL_S,
    run_forever,
    run_once,
)
from pipeline.agent.planner import LayerRunner
from pipeline.agent.types import ErrorKind, Fix, Layer
from pipeline.state.manifest import ManifestDB

DEFAULT_DIAGNOSE_BUDGET: Final[int] = 10
"""Per-``run_once`` LLM diagnose call cap (spec §7 D3)."""

_DEFAULT_SOURCE_ROOT: Final[Path] = Path("data/raw")
_DEFAULT_MANIFEST_PATH: Final[Path] = Path("state/manifest.db")


def _empty_runners(_batch_id: str) -> Mapping[Layer, LayerRunner]:
    """Placeholder runner factory — F2/F3 idempotent run helpers
    aren't part of this commit. The agent loop will still walk the
    source/manifest delta, log events, and acquire/release the lock,
    but no batch work will fire until the wiring task lands."""
    return {}


def _default_build_fix(
    _exc: BaseException,
    _kind: ErrorKind,
    _layer: Layer,
    _batch_id: str,
) -> Fix | None:
    """Placeholder fix builder — returns ``None`` for every kind so
    the executor escalates immediately. Real per-kind dispatch
    (schema_drift, regex_break, partition_missing, out_of_range)
    plugs in here once the wiring task lands."""
    return None


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


@agent.command("run-once")
@click.option(
    "--source-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=_DEFAULT_SOURCE_ROOT,
    show_default=True,
    help="Directory containing raw source parquet files.",
)
@click.option(
    "--manifest-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=_DEFAULT_MANIFEST_PATH,
    show_default=True,
    help="SQLite manifest path.",
)
@click.option(
    "--retry-budget",
    type=int,
    default=DEFAULT_RETRY_BUDGET,
    show_default=True,
    envvar="AGENT_RETRY_BUDGET",
    help="Retry budget per (batch_id, layer, error_class).",
)
@click.option(
    "--diagnose-budget",
    type=int,
    default=DEFAULT_DIAGNOSE_BUDGET,
    show_default=True,
    envvar="AGENT_DIAGNOSE_BUDGET",
    help="LLM diagnose call cap per run_once.",
)
@click.option(
    "--lock-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=Path("state/agent.lock"),
    show_default=True,
    envvar="AGENT_LOCK_PATH",
    help="Filesystem lock path.",
)
def run_once_cmd(
    source_root: Path,
    manifest_path: Path,
    retry_budget: int,
    diagnose_budget: int,
    lock_path: Path,
) -> None:
    """Run one iteration of the agent loop and exit."""
    with ManifestDB(manifest_path) as manifest:
        result = run_once(
            manifest=manifest,
            source_root=source_root,
            runners_for=_empty_runners,
            classify=_make_default_classifier(diagnose_budget),
            build_fix=_default_build_fix,
            escalate=make_escalator(log_path=DEFAULT_LOG_PATH, manifest=manifest),
            lock=AgentLock(lock_path),
            retry_budget=retry_budget,
        )
    click.echo(json.dumps(asdict(result), default=str))
    sys.exit(0 if result.status.value == "COMPLETED" else 1)


@agent.command("run-forever")
@click.option(
    "--source-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=_DEFAULT_SOURCE_ROOT,
    show_default=True,
    help="Directory containing raw source parquet files.",
)
@click.option(
    "--manifest-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=_DEFAULT_MANIFEST_PATH,
    show_default=True,
    help="SQLite manifest path.",
)
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
@click.option(
    "--retry-budget",
    type=int,
    default=DEFAULT_RETRY_BUDGET,
    show_default=True,
    envvar="AGENT_RETRY_BUDGET",
)
@click.option(
    "--diagnose-budget",
    type=int,
    default=DEFAULT_DIAGNOSE_BUDGET,
    show_default=True,
    envvar="AGENT_DIAGNOSE_BUDGET",
)
@click.option(
    "--lock-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=Path("state/agent.lock"),
    show_default=True,
    envvar="AGENT_LOCK_PATH",
)
def run_forever_cmd(
    source_root: Path,
    manifest_path: Path,
    interval: float,
    max_iters: int | None,
    retry_budget: int,
    diagnose_budget: int,
    lock_path: Path,
) -> None:
    """Run the agent loop continuously."""
    with ManifestDB(manifest_path) as manifest:
        results = run_forever(
            manifest=manifest,
            source_root=source_root,
            runners_for=_empty_runners,
            classify=_make_default_classifier(diagnose_budget),
            build_fix=_default_build_fix,
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
