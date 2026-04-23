"""Tests for ``pipeline.settings``."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.errors import ConfigError
from pipeline.settings import Settings


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Clear LLM/pipeline env vars and chdir to a tmp dir so .env is not loaded."""
    for key in [
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_BASE_URL",
        "LLM_MODEL_PRIMARY",
        "LLM_MODEL_FALLBACK",
        "PIPELINE_RETRY_BUDGET",
        "PIPELINE_LOOP_SLEEP_SECONDS",
        "PIPELINE_STATE_DB",
        "PIPELINE_LOG_LEVEL",
    ]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_missing_api_key_raises_config_error(clean_env: Path) -> None:
    with pytest.raises(ConfigError):
        Settings.load()


def test_defaults_applied_when_only_api_key_provided(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    settings = Settings.load()
    assert settings.anthropic_api_key.get_secret_value() == "sk-test"
    assert str(settings.anthropic_base_url).startswith(
        "https://coding-intl.dashscope.aliyuncs.com/apps/anthropic"
    )
    assert settings.llm_model_primary == "qwen3-max"
    assert settings.llm_model_fallback == "qwen3-coder-plus"
    assert settings.pipeline_retry_budget == 3
    assert settings.pipeline_log_level == "INFO"


def test_log_level_normalized_and_validated(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_LOG_LEVEL", "warning")
    settings = Settings.load()
    assert settings.pipeline_log_level == "WARNING"


def test_invalid_log_level_raises(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_LOG_LEVEL", "LOUD")
    with pytest.raises(ConfigError):
        Settings.load()


def test_retry_budget_bounds(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_RETRY_BUDGET", "0")
    with pytest.raises(ConfigError):
        Settings.load()


def test_state_db_path_resolves_relative_against_project_root(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    settings = Settings.load()
    resolved = settings.state_db_path()
    assert resolved.is_absolute()
    assert resolved.name == "manifest.db"


def test_state_db_rejects_parent_dir_traversal(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_STATE_DB", "../../tmp/evil.db")
    with pytest.raises(ConfigError, match=r"'\.\.' segments are not allowed"):
        Settings.load()
