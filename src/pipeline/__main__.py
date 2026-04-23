"""Entry point for ``python -m pipeline``."""

from __future__ import annotations

import click

from pipeline.cli.ingest import ingest


@click.group(name="pipeline")
@click.version_option(package_name="agentic-data-pipeline")
def cli() -> None:
    """Agentic data transformation pipeline (Bronze → Silver → Gold)."""


cli.add_command(ingest)


def main() -> None:
    """Script entrypoint (invoked via ``python -m pipeline`` or the console script)."""
    cli(standalone_mode=True)


if __name__ == "__main__":
    main()
