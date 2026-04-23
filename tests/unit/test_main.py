"""Tests for ``python -m pipeline`` entrypoint group."""

from __future__ import annotations

from click.testing import CliRunner

from pipeline.__main__ import cli


def test_cli_help_lists_ingest() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "ingest" in result.output


def test_cli_no_args_shows_usage() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, [])
    # Click prints help and exits with code 2 when a group is invoked with no
    # subcommand (configured by `no_args_is_help` default behavior).
    assert result.exit_code in (0, 2)
    assert "ingest" in result.output


def test_cli_rejects_unknown_subcommand() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["bogus"])
    assert result.exit_code == 2
    assert "No such command 'bogus'" in result.output
