"""Tests for :mod:`pipeline.silver.llm_extract`."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import polars as pl
import pytest

from pipeline.llm.cache import LLMCache
from pipeline.llm.client import LLMClient
from pipeline.settings import Settings
from pipeline.silver.llm_extract import (
    ExtractedEntities,
    apply_llm_extraction,
    extract_entities_from_body,
)

# ---------------------------------------------------------------------------
# Fixtures / fakes (mirrors ``tests/unit/test_llm_client.py``).
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Settings:
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
    ]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef")
    monkeypatch.chdir(tmp_path)
    return Settings.load()


@pytest.fixture
def cache() -> LLMCache:
    with LLMCache(":memory:") as c:
        yield c


def _fake_message(
    text: str, *, input_tokens: int = 10, output_tokens: int = 5
) -> Any:
    return SimpleNamespace(
        content=[SimpleNamespace(text=text)],
        usage=SimpleNamespace(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        ),
    )


class _FakeMessages:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if not self._responses:
            raise AssertionError(
                "FakeMessages exhausted; test did not supply enough responses"
            )
        item = self._responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class _FakeAnthropic:
    def __init__(self, responses: list[Any]) -> None:
        self.messages = _FakeMessages(responses)


def _client(
    settings: Settings, cache: LLMCache, responses: list[Any]
) -> tuple[LLMClient, _FakeAnthropic]:
    fake = _FakeAnthropic(responses)
    client = LLMClient(
        settings,
        cache,
        anthropic_client=fake,
        sleeper=lambda _s: None,
    )
    return client, fake


_GOOD_PAYLOAD: dict[str, Any] = {
    "veiculo_marca": "Toyota",
    "veiculo_modelo": "Corolla",
    "veiculo_ano": 2020,
    "concorrente_mencionado": "Porto Seguro",
    "valor_pago_atual_brl": 1850.5,
    "sinistro_historico": False,
}


# ---------------------------------------------------------------------------
# extract_entities_from_body — single-call contract.
# ---------------------------------------------------------------------------


def test_happy_path_populates_every_field(
    settings: Settings, cache: LLMCache
) -> None:
    client, _fake = _client(settings, cache, [_fake_message(json.dumps(_GOOD_PAYLOAD))])
    out = extract_entities_from_body("quero cotar um Corolla", client=client)
    assert out == ExtractedEntities(
        veiculo_marca="Toyota",
        veiculo_modelo="Corolla",
        veiculo_ano=2020,
        concorrente_mencionado="Porto Seguro",
        valor_pago_atual_brl=1850.5,
        sinistro_historico=False,
    )


def test_all_null_json_returns_null_dataclass(
    settings: Settings, cache: LLMCache
) -> None:
    payload = json.dumps(dict.fromkeys(_GOOD_PAYLOAD.keys()))
    client, _fake = _client(settings, cache, [_fake_message(payload)])
    out = extract_entities_from_body("msg", client=client)
    assert out == ExtractedEntities.null()


def test_malformed_json_returns_null_dataclass(
    settings: Settings, cache: LLMCache
) -> None:
    client, _fake = _client(settings, cache, [_fake_message("definitely not json {")])
    out = extract_entities_from_body("msg", client=client)
    assert out == ExtractedEntities.null()


# ---------------------------------------------------------------------------
# apply_llm_extraction — batch / dedup / budget semantics.
# ---------------------------------------------------------------------------


def test_dedupes_identical_bodies_into_one_llm_call(
    settings: Settings, cache: LLMCache
) -> None:
    df = pl.DataFrame(
        {
            "message_body_masked": [
                "quero cotar um Corolla",
                "quero cotar um Corolla",
            ]
        }
    )
    client, fake = _client(settings, cache, [_fake_message(json.dumps(_GOOD_PAYLOAD))])
    out = apply_llm_extraction(df, client=client)
    assert len(fake.messages.calls) == 1
    assert out["veiculo_marca"].to_list() == ["Toyota", "Toyota"]
    assert out["valor_pago_atual_brl"].to_list() == [1850.5, 1850.5]
    assert out["sinistro_historico"].to_list() == [False, False]


def test_budget_exhausted_fills_remaining_rows_with_nulls(
    settings: Settings, cache: LLMCache, caplog: pytest.LogCaptureFixture
) -> None:
    bodies = [f"body {i}" for i in range(5)]
    df = pl.DataFrame({"message_body_masked": bodies})
    # Only two responses are queued: the third body would raise the
    # "FakeMessages exhausted" guard if budget enforcement were broken.
    responses = [_fake_message(json.dumps(_GOOD_PAYLOAD)) for _ in range(2)]
    client, fake = _client(settings, cache, responses)

    out = apply_llm_extraction(df, client=client, max_calls=2)

    assert len(fake.messages.calls) == 2
    assert out["veiculo_marca"].to_list() == ["Toyota", "Toyota", None, None, None]
    assert out["sinistro_historico"].to_list() == [False, False, None, None, None]
    assert out["veiculo_ano"].to_list() == [2020, 2020, None, None, None]
    # Exactly one budget_exhausted event should have been logged.
    assert sum("llm.budget_exhausted" in r.message for r in caplog.records) <= 1


def test_non_ascii_body_roundtrips_cleanly(
    settings: Settings, cache: LLMCache
) -> None:
    payload = json.dumps(
        {
            "veiculo_marca": "Fiat",
            "veiculo_modelo": "Pálio",
            "veiculo_ano": 2018,
            "concorrente_mencionado": "Bradesco Seguros",
            "valor_pago_atual_brl": 980.0,
            "sinistro_historico": None,
        },
        ensure_ascii=False,
    )
    df = pl.DataFrame({"message_body_masked": ["tenho um pálio com acentuação"]})
    client, _fake = _client(settings, cache, [_fake_message(payload)])
    out = apply_llm_extraction(df, client=client)
    assert out["veiculo_modelo"][0] == "Pálio"
    assert out["concorrente_mencionado"][0] == "Bradesco Seguros"
    assert out["sinistro_historico"][0] is None
    assert out["valor_pago_atual_brl"][0] == pytest.approx(980.0)
