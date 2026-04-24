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
        "PIPELINE_LEAD_SECRET",
        "PIPELINE_LLM_MAX_CALLS_PER_BATCH",
        "PIPELINE_LLM_CONCURRENCY",
    ]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set every env var that ``Settings`` declares as required (``...``).

    Centralising this means "required" can grow over time without every
    test having to learn each new key. A test that wants to assert what
    happens when one required key is *missing* stays explicit and skips
    this helper.
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef")


def test_missing_api_key_raises_config_error(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # LEARN: setting only the lead secret isolates *which* required
    # field is missing. Without this line the loader could also be
    # complaining about ``PIPELINE_LEAD_SECRET``, and the assertion
    # would be ambiguous.
    monkeypatch.setenv("PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef")
    with pytest.raises(ConfigError):
        Settings.load()


def test_missing_lead_secret_raises_config_error(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Mirror of the API-key case: required-ness is enforced by Pydantic,
    # and ``Settings.load`` wraps ``ValidationError`` in ``ConfigError``.
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    with pytest.raises(ConfigError):
        Settings.load()


def test_defaults_applied_when_required_env_provided(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    settings = Settings.load()
    assert settings.anthropic_api_key.get_secret_value() == "sk-test"
    assert settings.pipeline_lead_secret.get_secret_value().startswith(
        "test-lead-secret"
    )
    assert str(settings.anthropic_base_url).startswith(
        "https://coding-intl.dashscope.aliyuncs.com/apps/anthropic"
    )
    assert settings.llm_model_primary == "qwen3-max"
    assert settings.llm_model_fallback == "qwen3-coder-plus"
    assert settings.pipeline_retry_budget == 3
    assert settings.pipeline_log_level == "INFO"
    assert settings.pipeline_llm_max_calls_per_batch == 5000
    assert settings.pipeline_llm_concurrency == 16


def test_llm_concurrency_override(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_LLM_CONCURRENCY", "32")
    settings = Settings.load()
    assert settings.pipeline_llm_concurrency == 32


def test_llm_concurrency_rejects_zero(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_LLM_CONCURRENCY", "0")
    with pytest.raises(ConfigError):
        Settings.load()


def test_llm_concurrency_rejects_above_ceiling(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """65 exceeds the [1, 64] Pydantic bound."""
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_LLM_CONCURRENCY", "65")
    with pytest.raises(ConfigError):
        Settings.load()


def test_llm_max_calls_per_batch_override(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_LLM_MAX_CALLS_PER_BATCH", "250")
    settings = Settings.load()
    assert settings.pipeline_llm_max_calls_per_batch == 250


def test_llm_max_calls_per_batch_rejects_zero(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_LLM_MAX_CALLS_PER_BATCH", "0")
    with pytest.raises(ConfigError):
        Settings.load()


def test_lead_secret_is_redacted_in_repr(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``SecretStr`` must hide the raw value in ``repr()`` so that a
    stray ``print(settings)`` or an unstructured log cannot leak it.
    """
    _set_required_env(monkeypatch)
    settings = Settings.load()
    as_repr = repr(settings)
    assert "test-lead-secret-0123456789abcdef" not in as_repr
    assert "SecretStr" in as_repr  # pydantic's masked representation


def test_log_level_normalized_and_validated(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_LOG_LEVEL", "warning")
    settings = Settings.load()
    assert settings.pipeline_log_level == "WARNING"


def test_invalid_log_level_raises(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_LOG_LEVEL", "LOUD")
    with pytest.raises(ConfigError):
        Settings.load()


def test_retry_budget_bounds(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_RETRY_BUDGET", "0")
    with pytest.raises(ConfigError):
        Settings.load()


def test_state_db_path_resolves_relative_against_project_root(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    settings = Settings.load()
    resolved = settings.state_db_path()
    assert resolved.is_absolute()
    assert resolved.name == "manifest.db"


def test_state_db_rejects_parent_dir_traversal(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_STATE_DB", "../../tmp/evil.db")
    with pytest.raises(ConfigError, match=r"'\.\.' segments are not allowed"):
        Settings.load()
