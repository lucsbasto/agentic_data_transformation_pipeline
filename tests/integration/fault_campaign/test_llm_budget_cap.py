"""FIC.23 — LLM fallback budget cap.

Instantiates ``_DiagnoseBudget(cap=10)`` and submits 11 unknown
exceptions.  Asserts exactly 10 LLM calls are made and the 11th
short-circuits to ``ErrorKind.UNKNOWN`` without an LLM call.

MED-1 note: ``_DiagnoseBudget`` is an underscore-prefixed internal.
We import it the same way ``tests/unit/test_agent_diagnoser.py`` does.
If the project exposes it publicly in a future release the import
path will change; update here accordingly.
"""

from __future__ import annotations

import json

import pytest

from pipeline.agent.diagnoser import _DiagnoseBudget, classify
from pipeline.agent.types import ErrorKind, Layer

from .conftest import FakeLLMClient

_LAYER = Layer.GOLD
_BATCH = "batch_fic23"
_CAP = 10


@pytest.mark.fault_campaign
def test_llm_budget_cap_exactly_ten_calls() -> None:
    """Submit CAP+1 unknown exceptions; LLM invoked exactly CAP times."""
    fake = FakeLLMClient()
    budget = _DiagnoseBudget(cap=_CAP)

    # Queue CAP replies — all return UNKNOWN.
    for _ in range(_CAP):
        fake.queue_reply(json.dumps({"kind": "unknown"}))

    results: list[ErrorKind] = []
    for i in range(_CAP + 1):
        kind = classify(
            RuntimeError(f"unclassified error #{i}"),
            layer=_LAYER,
            batch_id=_BATCH,
            llm_client=fake,
            budget=budget,
        )
        results.append(kind)

    # Exactly CAP LLM calls made.
    assert len(fake.calls) == _CAP, (
        f"expected exactly {_CAP} LLM calls, got {len(fake.calls)}"
    )

    # All CAP results are UNKNOWN (scripted reply was UNKNOWN).
    for i, kind in enumerate(results[:_CAP]):
        assert kind is ErrorKind.UNKNOWN, f"call {i}: expected UNKNOWN, got {kind!r}"

    # 11th call short-circuits to UNKNOWN without LLM.
    assert results[_CAP] is ErrorKind.UNKNOWN, (
        f"call {_CAP + 1}: expected UNKNOWN (budget exhausted), got {results[_CAP]!r}"
    )
    # Still exactly CAP calls — no extra call for attempt 11.
    assert len(fake.calls) == _CAP
