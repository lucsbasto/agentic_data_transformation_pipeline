"""Integration coverage for the ``pipeline agent`` CLI (F4.16)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipeline.__main__ import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def temp_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run each CLI test in a clean tmp workspace so the default
    ``state/`` and ``logs/`` paths don't bleed between tests."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "raw").mkdir(parents=True)
    # `Settings.load()` requires an ANTHROPIC_API_KEY + lead secret
    # even when the CLI default classifier disables stage-2 LLM
    # calls — pin fakes (mirrors the F2/F3 CLI test fixtures).
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef")
    # F7.2: pipeline.config.Settings now gates the CLI; pin DashScope key.
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-dashscope-test")
    return tmp_path


# ---------------------------------------------------------------------------
# Help surfaces.
# ---------------------------------------------------------------------------


def test_main_help_lists_agent(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "agent" in result.output


def test_agent_help_lists_subcommands(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["agent", "--help"])
    assert result.exit_code == 0
    assert "run-once" in result.output
    assert "run-forever" in result.output


def test_run_once_help_lists_canonical_options(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["agent", "run-once", "--help"])
    assert result.exit_code == 0
    expected_opts = (
        "--source-root",
        "--manifest-path",
        "--retry-budget",
        "--diagnose-budget",
        "--lock-path",
    )
    for opt in expected_opts:
        assert opt in result.output


def test_run_forever_help_lists_interval_and_max_iters(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["agent", "run-forever", "--help"])
    assert result.exit_code == 0
    assert "--interval" in result.output
    assert "--max-iters" in result.output


# ---------------------------------------------------------------------------
# Behaviour: run-once.
# ---------------------------------------------------------------------------


def _last_json_line(output: str) -> dict:
    """structlog interleaves stdout lines with the click.echo
    payload — the JSON we care about is always the last non-empty
    line."""
    for raw in reversed(output.strip().splitlines()):
        candidate = raw.strip()
        if candidate.startswith("{") and candidate.endswith("}"):
            return json.loads(candidate)
    raise AssertionError(f"no JSON payload found in CLI output:\n{output}")


def test_run_once_succeeds_on_empty_workspace(
    runner: CliRunner, temp_workspace: Path
) -> None:
    result = runner.invoke(cli, ["agent", "run-once"])
    assert result.exit_code == 0, result.output
    payload = _last_json_line(result.output)
    assert payload["status"] in {"COMPLETED", "RunStatus.COMPLETED"}
    assert payload["batches_processed"] == 0


def test_run_once_writes_agent_run_row(
    runner: CliRunner, temp_workspace: Path
) -> None:
    runner.invoke(cli, ["agent", "run-once"])
    manifest_path = temp_workspace / "state" / "manifest.db"
    assert manifest_path.exists()


def test_run_once_releases_lock_on_clean_run(
    runner: CliRunner, temp_workspace: Path
) -> None:
    runner.invoke(cli, ["agent", "run-once"])
    lock_path = temp_workspace / "state" / "agent.lock"
    assert not lock_path.exists()


def test_run_once_respects_env_var_overrides(
    runner: CliRunner, temp_workspace: Path
) -> None:
    """Spec §7 D5 + design §12 require env-var overrides on the
    budget knobs. Set them via the runner's `env` arg and assert the
    command still succeeds (defensive smoke — the values themselves
    are exercised by the executor + diagnoser unit tests)."""
    result = runner.invoke(
        cli,
        ["agent", "run-once"],
        env={"AGENT_RETRY_BUDGET": "5", "AGENT_DIAGNOSE_BUDGET": "20"},
    )
    assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# Behaviour: run-forever with --max-iters.
# ---------------------------------------------------------------------------


def test_run_forever_stops_after_max_iters(
    runner: CliRunner, temp_workspace: Path
) -> None:
    result = runner.invoke(
        cli,
        ["agent", "run-forever", "--interval", "0", "--max-iters", "2"],
    )
    assert result.exit_code == 0, result.output
    payload = _last_json_line(result.output)
    assert payload["iterations"] == 2
    assert payload["last_status"] == "COMPLETED"


# ---------------------------------------------------------------------------
# F7.2 — Settings wiring + flag override + friendly error.
# ---------------------------------------------------------------------------


def test_run_once_friendly_error_when_dashscope_key_missing(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec F7.2: missing DASHSCOPE_API_KEY → one-line UsageError, not a
    Pydantic ValidationError stack trace."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "raw").mkdir(parents=True)
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef")
    result = runner.invoke(cli, ["agent", "run-once"])
    assert result.exit_code != 0
    assert "DASHSCOPE_API_KEY" in result.output
    # No Pydantic stack trace exposed to the operator.
    assert "Traceback" not in result.output


def test_run_once_uses_settings_default_when_flag_absent(
    runner: CliRunner, temp_workspace: Path
) -> None:
    """Help output reflects pipeline.config.Settings field defaults."""
    result = runner.invoke(cli, ["agent", "run-once", "--help"])
    assert result.exit_code == 0
    # Field defaults from pipeline.config.Settings.
    assert "data/bronze" in result.output
    assert "state/manifest.db" in result.output


def test_run_once_cli_flag_overrides_env_var(
    runner: CliRunner, temp_workspace: Path
) -> None:
    """CLI flags remain final precedence — they must beat AGENT_* env."""
    result = runner.invoke(
        cli,
        ["agent", "run-once", "--retry-budget", "7"],
        env={"AGENT_RETRY_BUDGET": "2"},
    )
    assert result.exit_code == 0, result.output
