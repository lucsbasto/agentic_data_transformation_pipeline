"""Unit tests for ``pipeline.config.settings.Settings`` (F7.1).

Lives at ``test_config_settings.py`` to avoid clobbering the legacy
``test_settings.py`` which still tests ``pipeline.settings`` (the F1-era
config object). F7 introduces a parallel typed Settings under
``pipeline.config``; the two coexist until the F7.2 wiring task migrates
callers.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from pipeline.config import Settings


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Strip every Settings env var so each test starts from a known baseline.

    Also chdir to a clean tmp dir so a stray ``.env`` in the repo root never
    bleeds into a test that asserts defaults.
    """
    for var in (
        "DASHSCOPE_API_KEY",
        "AGENT_RETRY_BUDGET",
        "AGENT_DIAGNOSE_BUDGET",
        "AGENT_LOOP_INTERVAL",
        "AGENT_LOCK_PATH",
        "MANIFEST_PATH",
        "BRONZE_ROOT",
        "SILVER_ROOT",
        "GOLD_ROOT",
        "LOG_LEVEL",
        "LOG_FORMAT",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)


def test_defaults_populate_when_only_api_key_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-test")
    s = Settings()
    assert s.AGENT_RETRY_BUDGET == 3
    assert s.AGENT_DIAGNOSE_BUDGET == 10
    assert s.AGENT_LOOP_INTERVAL == 60.0
    assert Path("state/agent.lock") == s.AGENT_LOCK_PATH
    assert Path("state/manifest.db") == s.MANIFEST_PATH
    assert Path("data/bronze") == s.BRONZE_ROOT
    assert Path("data/silver") == s.SILVER_ROOT
    assert Path("data/gold") == s.GOLD_ROOT
    assert s.LOG_LEVEL == "INFO"
    assert s.LOG_FORMAT == "json"


def test_env_overrides_numeric_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-test")
    monkeypatch.setenv("AGENT_RETRY_BUDGET", "7")
    monkeypatch.setenv("AGENT_DIAGNOSE_BUDGET", "25")
    monkeypatch.setenv("AGENT_LOOP_INTERVAL", "12.5")
    s = Settings()
    assert s.AGENT_RETRY_BUDGET == 7
    assert s.AGENT_DIAGNOSE_BUDGET == 25
    assert s.AGENT_LOOP_INTERVAL == 12.5


def test_dotenv_file_overrides_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DASHSCOPE_API_KEY=sk-from-dotenv\n"
        "AGENT_RETRY_BUDGET=4\n"
        "LOG_FORMAT=console\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    s = Settings()
    assert s.DASHSCOPE_API_KEY.get_secret_value() == "sk-from-dotenv"
    assert s.AGENT_RETRY_BUDGET == 4
    assert s.LOG_FORMAT == "console"


def test_env_overrides_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Process env wins over .env (precedence rule documented in module)."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DASHSCOPE_API_KEY=sk-from-dotenv\nAGENT_RETRY_BUDGET=4\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AGENT_RETRY_BUDGET", "9")
    s = Settings()
    assert s.AGENT_RETRY_BUDGET == 9


def test_validator_rejects_zero_retry_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-test")
    monkeypatch.setenv("AGENT_RETRY_BUDGET", "0")
    with pytest.raises(ValidationError):
        Settings()


def test_validator_rejects_sub_one_loop_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-test")
    monkeypatch.setenv("AGENT_LOOP_INTERVAL", "0.5")
    with pytest.raises(ValidationError):
        Settings()


def test_validator_rejects_unknown_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-test")
    monkeypatch.setenv("LOG_LEVEL", "TRACE")
    with pytest.raises(ValidationError):
        Settings()


def test_validator_rejects_unknown_log_format(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-test")
    monkeypatch.setenv("LOG_FORMAT", "xml")
    with pytest.raises(ValidationError):
        Settings()


def test_missing_api_key_raises() -> None:
    with pytest.raises(ValidationError):
        Settings()


def test_secret_str_does_not_leak_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-super-secret-value")
    s = Settings()
    assert "sk-super-secret-value" not in repr(s)
    assert "sk-super-secret-value" not in str(s)
    assert isinstance(s.DASHSCOPE_API_KEY, SecretStr)
    assert s.DASHSCOPE_API_KEY.get_secret_value() == "sk-super-secret-value"


def test_path_fields_coerce_string_env_to_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-test")
    monkeypatch.setenv("AGENT_LOCK_PATH", "/var/run/agent.lock")
    monkeypatch.setenv("MANIFEST_PATH", "/srv/manifest.db")
    monkeypatch.setenv("BRONZE_ROOT", "/srv/bronze")
    s = Settings()
    assert Path("/var/run/agent.lock") == s.AGENT_LOCK_PATH
    assert isinstance(s.AGENT_LOCK_PATH, Path)
    assert Path("/srv/manifest.db") == s.MANIFEST_PATH
    assert Path("/srv/bronze") == s.BRONZE_ROOT


def test_case_sensitive_lowercase_env_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """case_sensitive=True means lowercase variants must be ignored."""
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-test")
    monkeypatch.setenv("agent_retry_budget", "99")
    s = Settings()
    assert s.AGENT_RETRY_BUDGET == 3
