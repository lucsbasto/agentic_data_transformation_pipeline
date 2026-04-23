"""Tests for the real :class:`pipeline.llm.client.LLMClient`."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from anthropic import (
    APIConnectionError,
    BadRequestError,
    RateLimitError,
)

from pipeline.errors import LLMCallError
from pipeline.llm.cache import LLMCache
from pipeline.llm.client import LLMClient, LLMResponse
from pipeline.settings import Settings

# --------------------------------------------------------------------------- helpers


def _make_fake_exception(cls: type[Exception], msg: str = "fake") -> Exception:
    """Instantiate an Anthropic exception subclass without its heavy __init__.

    The SDK's error classes require httpx/response objects that are a pain
    to build in a unit test. We bypass __init__ and call Exception's own
    initializer so ``except cls`` still matches via isinstance.
    """
    exc = cls.__new__(cls)
    Exception.__init__(exc, msg)
    return exc


def _fake_message(text: str, *, input_tokens: int = 10, output_tokens: int = 5) -> Any:
    return SimpleNamespace(
        content=[SimpleNamespace(text=text)],
        usage=SimpleNamespace(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        ),
    )


class FakeMessages:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if not self._responses:
            raise AssertionError("FakeMessages exhausted; test did not supply enough responses")
        item = self._responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class FakeAnthropic:
    def __init__(self, responses: list[Any]) -> None:
        self.messages = FakeMessages(responses)


# --------------------------------------------------------------------------- fixtures


@pytest.fixture
def settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
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
    monkeypatch.setenv("LLM_MODEL_PRIMARY", "qwen3-max")
    monkeypatch.setenv("LLM_MODEL_FALLBACK", "qwen3-coder-plus")
    monkeypatch.setenv("PIPELINE_RETRY_BUDGET", "3")
    monkeypatch.chdir(tmp_path)
    return Settings.load()


@pytest.fixture
def cache() -> LLMCache:
    with LLMCache(":memory:") as c:
        yield c


def _make_client(
    settings: Settings,
    cache: LLMCache,
    responses: list[Any],
) -> tuple[LLMClient, FakeAnthropic]:
    fake = FakeAnthropic(responses)
    client = LLMClient(
        settings,
        cache,
        anthropic_client=fake,
        sleeper=lambda _s: None,
    )
    return client, fake


# --------------------------------------------------------------------------- tests


def test_cache_miss_calls_sdk_and_stores(settings: Settings, cache: LLMCache) -> None:
    client, fake = _make_client(settings, cache, [_fake_message("hi")])
    result = client.cached_call(system="sys", user="hello")
    assert isinstance(result, LLMResponse)
    assert result.text == "hi"
    assert result.cache_hit is False
    assert result.model == settings.llm_model_primary
    assert len(fake.messages.calls) == 1
    # Second call with same inputs must hit the cache.
    cached = client.cached_call(system="sys", user="hello")
    assert cached.cache_hit is True
    assert cached.text == "hi"
    assert len(fake.messages.calls) == 1  # no new SDK call


def test_retry_succeeds_after_transient(settings: Settings, cache: LLMCache) -> None:
    transient = _make_fake_exception(APIConnectionError, "boom")
    client, fake = _make_client(
        settings, cache, [transient, _fake_message("ok")]
    )
    result = client.cached_call(system="s", user="u")
    assert result.text == "ok"
    assert result.retry_count == 1
    assert len(fake.messages.calls) == 2


def test_fatal_error_fails_immediately(settings: Settings, cache: LLMCache) -> None:
    fatal = _make_fake_exception(BadRequestError, "bad input")
    client, _fake = _make_client(settings, cache, [fatal])
    with pytest.raises(LLMCallError, match="non-retryable"):
        client.cached_call(system="s", user="u")


def test_exhaust_primary_falls_back(settings: Settings, cache: LLMCache) -> None:
    # Three retryable failures exhaust the budget, then a fallback call succeeds.
    responses = [
        _make_fake_exception(RateLimitError, "r1"),
        _make_fake_exception(RateLimitError, "r2"),
        _make_fake_exception(RateLimitError, "r3"),
        _fake_message("fallback ok"),
    ]
    client, fake = _make_client(settings, cache, responses)
    result = client.cached_call(system="s", user="u")
    assert result.text == "fallback ok"
    assert result.model == settings.llm_model_fallback
    # Four SDK attempts: 3 primary + 1 fallback.
    assert len(fake.messages.calls) == 4
    assert fake.messages.calls[-1]["model"] == settings.llm_model_fallback


def test_all_attempts_fail_raises_llm_call_error(
    settings: Settings, cache: LLMCache
) -> None:
    responses = [
        _make_fake_exception(RateLimitError, "p1"),
        _make_fake_exception(RateLimitError, "p2"),
        _make_fake_exception(RateLimitError, "p3"),
        _make_fake_exception(RateLimitError, "fallback"),
    ]
    client, _fake = _make_client(settings, cache, responses)
    with pytest.raises(LLMCallError, match="after retries"):
        client.cached_call(system="s", user="u")


def test_invalidate_removes_cached_rows(
    settings: Settings, cache: LLMCache
) -> None:
    client, _fake = _make_client(settings, cache, [_fake_message("hi")])
    client.cached_call(system="s", user="u")
    removed = client.invalidate()
    assert removed == 1


def test_explicit_model_override(settings: Settings, cache: LLMCache) -> None:
    client, fake = _make_client(settings, cache, [_fake_message("hi")])
    client.cached_call(system="s", user="u", model="custom-model")
    assert fake.messages.calls[0]["model"] == "custom-model"


def test_cache_key_depends_on_request(
    settings: Settings, cache: LLMCache
) -> None:
    # Two different prompts must both reach the SDK (no cross-contamination).
    client, fake = _make_client(
        settings, cache, [_fake_message("a"), _fake_message("b")]
    )
    first = client.cached_call(system="s", user="one")
    second = client.cached_call(system="s", user="two")
    assert first.text == "a"
    assert second.text == "b"
    assert len(fake.messages.calls) == 2


def test_response_usage_fields_populated(
    settings: Settings, cache: LLMCache
) -> None:
    client, _fake = _make_client(
        settings, cache, [_fake_message("hi", input_tokens=42, output_tokens=7)]
    )
    result = client.cached_call(system="s", user="u")
    assert result.input_tokens == 42
    assert result.output_tokens == 7


def test_construction_with_default_anthropic(
    settings: Settings, cache: LLMCache, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When ``anthropic_client`` is not passed the client must build one.

    We replace the ``Anthropic`` constructor in :mod:`pipeline.llm.client` so
    no network request is attempted. The goal of this test is simply to
    confirm the auto-build path is wired, not to exercise the SDK.
    """

    class DummyAnthropic:
        def __init__(self, *, api_key: str, base_url: str) -> None:
            self.api_key = api_key
            self.base_url = base_url

    monkeypatch.setattr("pipeline.llm.client.Anthropic", DummyAnthropic)
    client = LLMClient(settings, cache)
    assert client.settings is settings
