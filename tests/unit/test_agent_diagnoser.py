"""Coverage for the F4 agent diagnoser (F4.4).

Stage 1 patterns are checked exhaustively. Stage 2 (LLM fallback)
uses a fake :class:`_LLMClientProto` so no network calls fire and
the budget logic can be observed end-to-end.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import polars.exceptions as pl_exc
import pytest

from pipeline.agent.diagnoser import (
    DIAGNOSE_SYSTEM_PROMPT,
    PROMPT_VERSION_DIAGNOSE,
    _DiagnoseBudget,
    classify,
)
from pipeline.agent.types import ErrorKind, Layer
from pipeline.errors import SilverOutOfRangeError, SilverRegexMissError

# ---------------------------------------------------------------------------
# Fake LLM client.
# ---------------------------------------------------------------------------


@dataclass
class _FakeResponse:
    text: str


class _FakeLLM:
    """Records every ``cached_call`` and returns scripted replies."""

    def __init__(self, replies: list[str]) -> None:
        self._replies = list(replies)
        self.calls: list[dict[str, Any]] = []

    def cached_call(self, **kwargs: Any) -> _FakeResponse:
        self.calls.append(kwargs)
        if not self._replies:
            raise AssertionError("FakeLLM ran out of scripted replies")
        return _FakeResponse(text=self._replies.pop(0))


# ---------------------------------------------------------------------------
# Stage 1 — deterministic patterns.
# ---------------------------------------------------------------------------


def test_classify_silver_regex_miss_is_regex_break() -> None:
    exc = SilverRegexMissError("no match for /R\\$\\d+/")
    assert classify(exc, layer=Layer.SILVER, batch_id="bid01") is ErrorKind.REGEX_BREAK


def test_classify_silver_out_of_range_is_out_of_range() -> None:
    exc = SilverOutOfRangeError("valor_pago_atual_brl=-999 below floor 0")
    assert (
        classify(exc, layer=Layer.SILVER, batch_id="bid01") is ErrorKind.OUT_OF_RANGE
    )


def test_classify_file_not_found_is_partition_missing() -> None:
    exc = FileNotFoundError("no such file: data/bronze/batch_id=evil/part-0.parquet")
    assert (
        classify(exc, layer=Layer.BRONZE, batch_id="bid01") is ErrorKind.PARTITION_MISSING
    )


@pytest.mark.parametrize(
    "exc",
    [
        pl_exc.SchemaError("schema mismatch on column 'x'"),
        pl_exc.SchemaFieldNotFoundError("field 'foo' missing"),
        pl_exc.ColumnNotFoundError("column 'bar' not found"),
    ],
)
def test_classify_polars_schema_errors_are_schema_drift(exc: Exception) -> None:
    assert (
        classify(exc, layer=Layer.SILVER, batch_id="bid01") is ErrorKind.SCHEMA_DRIFT
    )


def test_classify_returns_unknown_when_pattern_misses_and_no_llm() -> None:
    """No deterministic match + no LLM client = ``UNKNOWN``. The
    executor escalates without spending a retry."""
    exc = RuntimeError("something fresh and unclassified")
    assert classify(exc, layer=Layer.GOLD, batch_id="bid01") is ErrorKind.UNKNOWN


# ---------------------------------------------------------------------------
# Stage 2 — LLM fallback.
# ---------------------------------------------------------------------------


def test_classify_calls_llm_when_pattern_misses() -> None:
    fake = _FakeLLM(replies=[json.dumps({"kind": "regex_break"})])
    exc = RuntimeError("something exotic")
    kind = classify(
        exc,
        layer=Layer.SILVER,
        batch_id="bid01",
        llm_client=fake,
        budget=_DiagnoseBudget(),
    )
    assert kind is ErrorKind.REGEX_BREAK
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["system"] == DIAGNOSE_SYSTEM_PROMPT
    # User prompt must wrap exception context in the ``untrusted`` block.
    assert '<error_ctx untrusted="true">' in call["user"]
    assert "RuntimeError" in call["user"]
    assert "bid01" in call["user"]
    assert call["temperature"] == 0.0
    assert call["max_tokens"] == 64


def test_classify_does_not_call_llm_when_pattern_hits() -> None:
    """Stage 2 must be skipped entirely on a deterministic hit — no
    cost, no latency."""
    fake = _FakeLLM(replies=[])
    exc = SilverRegexMissError("no match")
    classify(
        exc,
        layer=Layer.SILVER,
        batch_id="bid01",
        llm_client=fake,
        budget=_DiagnoseBudget(),
    )
    assert fake.calls == []


@pytest.mark.parametrize("invalid_kind", ["", "schemA_drIfT", "exotic", "schema-drift"])
def test_classify_returns_unknown_when_llm_returns_invalid_kind(
    invalid_kind: str,
) -> None:
    """Defense against a hallucinated label — only the five canonical
    enum values pass through."""
    fake = _FakeLLM(replies=[json.dumps({"kind": invalid_kind})])
    if invalid_kind.casefold() in {k.value for k in ErrorKind}:
        pytest.skip("parametrize value happens to be a valid kind after casefold")
    kind = classify(
        RuntimeError("boom"),
        layer=Layer.SILVER,
        batch_id="bid01",
        llm_client=fake,
        budget=_DiagnoseBudget(),
    )
    assert kind is ErrorKind.UNKNOWN


def test_classify_returns_unknown_when_llm_returns_non_json() -> None:
    fake = _FakeLLM(replies=["I think this is a regex break"])
    kind = classify(
        RuntimeError("boom"),
        layer=Layer.SILVER,
        batch_id="bid01",
        llm_client=fake,
        budget=_DiagnoseBudget(),
    )
    assert kind is ErrorKind.UNKNOWN


def test_classify_returns_unknown_when_llm_returns_unexpected_shape() -> None:
    fake = _FakeLLM(replies=[json.dumps(["regex_break"])])
    kind = classify(
        RuntimeError("boom"),
        layer=Layer.SILVER,
        batch_id="bid01",
        llm_client=fake,
        budget=_DiagnoseBudget(),
    )
    assert kind is ErrorKind.UNKNOWN


# ---------------------------------------------------------------------------
# Budget.
# ---------------------------------------------------------------------------


def test_diagnose_budget_consume_until_exhausted() -> None:
    budget = _DiagnoseBudget(cap=3)
    assert budget.remaining == 3
    assert budget.consume() is True
    assert budget.consume() is True
    assert budget.consume() is True
    assert budget.consume() is False  # exhausted
    assert budget.remaining == 0


def test_classify_short_circuits_on_exhausted_budget_without_calling_llm() -> None:
    """Budget zero -> ``UNKNOWN`` directly. No LLM call, no spend."""
    fake = _FakeLLM(replies=[])
    budget = _DiagnoseBudget(cap=0)
    kind = classify(
        RuntimeError("boom"),
        layer=Layer.SILVER,
        batch_id="bid01",
        llm_client=fake,
        budget=budget,
    )
    assert kind is ErrorKind.UNKNOWN
    assert fake.calls == []


def test_classify_consumes_exactly_one_budget_per_unknown() -> None:
    fake = _FakeLLM(
        replies=[
            json.dumps({"kind": "regex_break"}),
            json.dumps({"kind": "regex_break"}),
        ]
    )
    budget = _DiagnoseBudget(cap=10)
    classify(
        RuntimeError("a"),
        layer=Layer.SILVER,
        batch_id="bid01",
        llm_client=fake,
        budget=budget,
    )
    classify(
        RuntimeError("b"),
        layer=Layer.SILVER,
        batch_id="bid01",
        llm_client=fake,
        budget=budget,
    )
    assert budget.used == 2
    assert budget.remaining == 8


# ---------------------------------------------------------------------------
# Prompt invariants.
# ---------------------------------------------------------------------------


def test_system_prompt_embeds_prompt_version() -> None:
    """``PROMPT_VERSION_DIAGNOSE`` must appear in the system text so
    bumping the version automatically rotates the LLM cache key."""
    assert (
        f"PROMPT_VERSION_DIAGNOSE={PROMPT_VERSION_DIAGNOSE}" in DIAGNOSE_SYSTEM_PROMPT
    )


def test_system_prompt_lists_every_error_kind_value() -> None:
    for kind in ErrorKind:
        assert kind.value in DIAGNOSE_SYSTEM_PROMPT


def test_system_prompt_warns_against_prompt_injection() -> None:
    """The system prompt must instruct the LLM to treat the
    ``<error_ctx untrusted="true">`` block as evidence only."""
    assert "prompt injection" in DIAGNOSE_SYSTEM_PROMPT.casefold()
    assert '<error_ctx untrusted="true">' in DIAGNOSE_SYSTEM_PROMPT
