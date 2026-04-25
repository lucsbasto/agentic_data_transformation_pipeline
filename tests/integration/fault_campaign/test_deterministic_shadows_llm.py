"""FIC.22 — Deterministic patterns shadow LLM.

For each pattern handled by stage-1, asserts:
- ``classify`` returns the expected ErrorKind.
- ``FakeLLMClient.calls`` count == 0 (LLM never invoked).
"""

from __future__ import annotations

import pytest
import polars.exceptions as pl_exc

from pipeline.agent.diagnoser import _DiagnoseBudget, classify
from pipeline.agent.types import ErrorKind, Layer
from pipeline.errors import SilverOutOfRangeError, SilverRegexMissError

from .conftest import FakeLLMClient

_LAYER = Layer.SILVER
_BATCH = "batch_fic22"

_DETERMINISTIC_CASES = [
    (pl_exc.SchemaError("schema mismatch"), ErrorKind.SCHEMA_DRIFT),
    (pl_exc.SchemaFieldNotFoundError("field missing"), ErrorKind.SCHEMA_DRIFT),
    (pl_exc.ColumnNotFoundError("column missing"), ErrorKind.SCHEMA_DRIFT),
    (SilverRegexMissError("no match for /R\\$\\d+/"), ErrorKind.REGEX_BREAK),
    (SilverOutOfRangeError("valor_pago=-999 below floor 0"), ErrorKind.OUT_OF_RANGE),
    (FileNotFoundError("no such file: bronze/part-0.parquet"), ErrorKind.PARTITION_MISSING),
]


@pytest.mark.fault_campaign
@pytest.mark.parametrize(
    "exc,expected_kind",
    _DETERMINISTIC_CASES,
    ids=[type(exc).__name__ for exc, _ in _DETERMINISTIC_CASES],
)
def test_deterministic_pattern_shadows_llm(
    exc: BaseException,
    expected_kind: ErrorKind,
) -> None:
    """Stage-1 match returns expected kind without invoking LLM."""
    fake = FakeLLMClient()
    budget = _DiagnoseBudget(cap=10)

    kind = classify(
        exc,
        layer=_LAYER,
        batch_id=_BATCH,
        llm_client=fake,
        budget=budget,
    )

    assert kind is expected_kind, (
        f"expected {expected_kind!r} for {type(exc).__name__}, got {kind!r}"
    )
    assert fake.calls == [], (
        f"LLM must NOT be called for deterministic pattern {type(exc).__name__}; "
        f"got {len(fake.calls)} call(s)"
    )
