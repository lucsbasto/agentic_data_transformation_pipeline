"""Tests for the F1 LLMClient stub."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.errors import LLMError, PipelineError
from pipeline.llm import LLMClient, LLMResponse
from pipeline.settings import Settings


@pytest.fixture
def settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Settings:
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
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.chdir(tmp_path)
    return Settings.load()


def test_client_can_be_instantiated(settings: Settings) -> None:
    client = LLMClient(settings)
    assert client.settings is settings


def test_cached_call_raises_llm_error(settings: Settings) -> None:
    client = LLMClient(settings)
    with pytest.raises(LLMError, match=r"not implemented until F1\.7"):
        client.cached_call(system="s", user="u")


def test_invalidate_raises_llm_error(settings: Settings) -> None:
    client = LLMClient(settings)
    with pytest.raises(LLMError, match=r"not implemented until F1\.7"):
        client.invalidate(prefix="x")


def test_llm_error_is_pipeline_error(settings: Settings) -> None:
    client = LLMClient(settings)
    with pytest.raises(PipelineError):
        client.cached_call(system="s", user="u")


def test_llm_response_has_expected_fields() -> None:
    resp = LLMResponse(
        text="hi",
        model="qwen3-max",
        input_tokens=5,
        output_tokens=2,
        cache_hit=False,
    )
    assert resp.text == "hi"
    assert resp.model == "qwen3-max"
    assert resp.input_tokens == 5
    assert resp.output_tokens == 2
    assert resp.cache_hit is False
