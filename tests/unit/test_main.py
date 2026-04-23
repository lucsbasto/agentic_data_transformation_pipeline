"""Tests for `python -m pipeline` entrypoint."""

from __future__ import annotations

from pipeline.__main__ import main


def test_main_no_args_prints_usage(capsys) -> None:
    exit_code = main([])
    stderr = capsys.readouterr().err
    assert exit_code == 2
    assert "usage: python -m pipeline" in stderr


def test_main_unknown_subcommand(capsys) -> None:
    exit_code = main(["bogus"])
    stderr = capsys.readouterr().err
    assert exit_code == 2
    assert "unknown subcommand: bogus" in stderr
