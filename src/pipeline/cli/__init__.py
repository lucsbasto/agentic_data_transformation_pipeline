"""Click-based command-line interface."""

# LEARN: re-export the ``ingest`` subcommand so ``pipeline.cli.ingest``
# resolves both as a package path (the submodule) AND as the command
# object. Watch out when you ``monkeypatch.setattr("pipeline.cli.ingest.X")``
# in tests — the name resolves to the Command, not the module (the
# re-export shadows the submodule). That is why
# ``tests/integration/test_cli_ingest_e2e.py`` grabs the module via
# ``sys.modules["pipeline.cli.ingest"]`` when monkeypatching.
from pipeline.cli.ingest import ingest

__all__ = ["ingest"]
