"""Entry point for ``python -m pipeline``."""

from __future__ import annotations

# LEARN: ``click`` is a popular Python CLI library. It turns functions
# into command-line commands via decorators — no manual ``sys.argv``
# parsing. Alternatives: ``argparse`` (stdlib, verbose) or ``typer``
# (friendlier but heavier). Click is small and battle-tested.
import click

from pipeline.cli.agent import agent
from pipeline.cli.gold import gold
from pipeline.cli.ingest import ingest
from pipeline.cli.silver import silver


# LEARN: ``@click.group`` declares a command that itself contains other
# commands. Running ``python -m pipeline`` prints the group help; running
# ``python -m pipeline ingest`` dispatches to the ``ingest`` subcommand.
# ``@click.version_option(package_name=...)`` auto-wires ``--version``
# by reading the installed package metadata — zero hand-maintained
# version strings here.
@click.group(name="pipeline")
@click.version_option(package_name="agentic-data-pipeline")
def cli() -> None:
    """Agentic data transformation pipeline (Bronze → Silver → Gold)."""


# LEARN: ``cli.add_command(...)`` registers each subcommand we imported
# above.
cli.add_command(ingest)
cli.add_command(silver)
cli.add_command(gold)
cli.add_command(agent)


def main() -> None:
    """Script entrypoint (invoked via ``python -m pipeline`` or the console script)."""
    # LEARN: ``standalone_mode=True`` is click's default — tells it to
    # catch exceptions, format nicely, and exit with the right code.
    # Explicit-over-implicit for operator-facing entrypoints.
    cli(standalone_mode=True)


# LEARN: ``__name__ == "__main__"`` is the classic Python idiom for
# "only run this block when the file is executed directly". Imported as
# a library, ``__name__`` is the package path and this branch is
# skipped. ``python -m pipeline`` makes ``__main__.py`` the module
# being run, so this branch fires and ``main()`` is invoked.
if __name__ == "__main__":
    main()
