"""Perf-bench CLI — dispatch a registered scenario and write JSONL.

Usage::

    python scripts/bench_agent.py \\
        --scenario warm_loop \\
        --runs 3 \\
        --out logs/perf/warm_loop.jsonl \\
        --opt batch_id=demo01 \\
        --opt layer=silver

Future PERF.* tasks register scenarios by dropping a module under
``pipeline.perf.scenarios`` that exposes a module-level ``SCENARIO``
satisfying :class:`pipeline.perf.harness.Scenario` — no edits here
are required.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from pipeline.perf.harness import (
    JsonlWriter,
    ScenarioContext,
    discover_scenarios,
)


def _parse_opts(raw: tuple[str, ...]) -> dict[str, str]:
    """Parse ``--opt KEY=VAL`` repeats into a dict.

    Rejects malformed entries (missing ``=``) by exiting with code 2.
    """
    parsed: dict[str, str] = {}
    for entry in raw:
        if "=" not in entry:
            click.echo(
                f"error: malformed --opt {entry!r}; expected KEY=VAL",
                err=True,
            )
            sys.exit(2)
        key, _, value = entry.partition("=")
        parsed[key] = value
    return parsed


@click.command()
@click.option("--scenario", "scenario_name", required=True, type=str)
@click.option("--runs", default=1, show_default=True, type=int)
@click.option(
    "--out",
    "out_path",
    required=True,
    type=click.Path(path_type=Path),
)
@click.option(
    "--opt",
    "opts",
    multiple=True,
    type=str,
    help="Repeatable KEY=VAL pairs forwarded to the scenario.",
)
@click.option(
    "--work-root",
    "work_root",
    default=None,
    type=click.Path(path_type=Path),
    help="Working directory for scenario artifacts; defaults to cwd.",
)
def main(
    scenario_name: str,
    runs: int,
    out_path: Path,
    opts: tuple[str, ...],
    work_root: Path | None,
) -> None:
    """Dispatch a perf scenario and stream its records to JSONL."""
    if runs < 1:
        click.echo("error: --runs must be >= 1", err=True)
        sys.exit(2)

    registry = discover_scenarios()
    if scenario_name not in registry:
        available = ", ".join(sorted(registry)) or "(none)"
        click.echo(
            f"error: unknown scenario {scenario_name!r}; available: {available}",
            err=True,
        )
        sys.exit(2)

    extra = _parse_opts(opts)
    ctx = ScenarioContext(
        scenario_name=scenario_name,
        runs=runs,
        out_path=out_path,
        work_root=work_root if work_root is not None else Path.cwd(),
        extra=extra,
    )

    scenario = registry[scenario_name]
    count = 0
    with JsonlWriter(out_path) as writer:
        for record in scenario.run(ctx):
            writer.write(record)
            count += 1

    click.echo(f"wrote {count} records to {out_path}")


if __name__ == "__main__":
    main()
