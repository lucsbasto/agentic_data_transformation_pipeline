"""Tests for :func:`pipeline.gold.competitor_intel.build_competitor_intel`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl

from pipeline.gold.competitor_intel import build_competitor_intel
from pipeline.schemas.gold import (
    GOLD_COMPETITOR_INTEL_SCHEMA,
    assert_competitor_intel_schema,
)

_BASE_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _ts(seconds: int) -> datetime:
    return _BASE_TS + timedelta(seconds=seconds)


def _silver_row(
    *,
    conversation_id: str = "c1",
    timestamp: datetime,
    concorrente_mencionado: str | None = None,
    valor_pago_atual_brl: float | None = None,
    conversation_outcome: str | None = None,
    state: str | None = None,
) -> dict[str, Any]:
    return {
        "conversation_id": conversation_id,
        "timestamp": timestamp,
        "concorrente_mencionado": concorrente_mencionado,
        "valor_pago_atual_brl": valor_pago_atual_brl,
        "conversation_outcome": conversation_outcome,
        "metadata": {"state": state},
    }


def _silver_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.DataFrame(
        rows,
        schema={
            "conversation_id": pl.String,
            "timestamp": pl.Datetime("us", time_zone="UTC"),
            "concorrente_mencionado": pl.String,
            "valor_pago_atual_brl": pl.Float64,
            "conversation_outcome": pl.String,
            "metadata": pl.Struct({"state": pl.String}),
        },
    ).lazy()


# ---------------------------------------------------------------------------
# Schema + smoke.
# ---------------------------------------------------------------------------


def test_output_matches_declared_schema() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0),
            concorrente_mencionado="Porto Seguro",
            conversation_outcome="venda_fechada",
            state="SP",
        ),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    assert out.schema == GOLD_COMPETITOR_INTEL_SCHEMA
    assert_competitor_intel_schema(out)


def test_empty_input_emits_empty_frame() -> None:
    out = build_competitor_intel(_silver_lf([])).collect()
    assert out.height == 0
    assert out.schema == GOLD_COMPETITOR_INTEL_SCHEMA


def test_rows_without_competitor_are_dropped() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), concorrente_mencionado=None),
        _silver_row(timestamp=_ts(60), concorrente_mencionado="Porto"),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    assert out.height == 1
    assert out["competitor"][0] == "porto"


# ---------------------------------------------------------------------------
# Normalization (D7).
# ---------------------------------------------------------------------------


def test_competitor_name_is_lowercased_and_trimmed() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), concorrente_mencionado="Porto Seguro"),
        _silver_row(timestamp=_ts(60), concorrente_mencionado="porto seguro"),
        _silver_row(timestamp=_ts(120), concorrente_mencionado="  Porto Seguro  "),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect().sort("competitor")
    assert out.height == 1
    assert out["competitor"][0] == "porto seguro"
    assert out["mention_count"][0] == 3


# ---------------------------------------------------------------------------
# mention_count + avg_quoted_price_brl.
# ---------------------------------------------------------------------------


def test_mention_count_counts_raw_rows_not_distinct_conversations() -> None:
    """Five rows mentioning the same competitor across two conversations
    yield mention_count=5. loss_rate (tested separately) deduplicates
    to distinct conversations."""
    rows = [
        _silver_row(
            conversation_id="c1",
            timestamp=_ts(i),
            concorrente_mencionado="Porto",
        )
        for i in (0, 30, 60)
    ] + [
        _silver_row(
            conversation_id="c2",
            timestamp=_ts(i),
            concorrente_mencionado="Porto",
        )
        for i in (0, 30)
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    assert out.height == 1
    assert out["mention_count"][0] == 5


def test_avg_quoted_price_null_when_no_prices_observed() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), concorrente_mencionado="Porto"),
        _silver_row(timestamp=_ts(60), concorrente_mencionado="Porto"),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    assert out["avg_quoted_price_brl"][0] is None


def test_avg_quoted_price_averages_observed_values() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0),
            concorrente_mencionado="Porto",
            valor_pago_atual_brl=1200.0,
        ),
        _silver_row(
            timestamp=_ts(60),
            concorrente_mencionado="Porto",
            valor_pago_atual_brl=1800.0,
        ),
        _silver_row(timestamp=_ts(120), concorrente_mencionado="Porto"),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    assert out["avg_quoted_price_brl"][0] == 1500.0


# ---------------------------------------------------------------------------
# loss_rate — distinct conversations + null-outcome handling.
# ---------------------------------------------------------------------------


def test_loss_rate_on_distinct_conversations_not_raw_rows() -> None:
    """Same competitor mentioned many times in one conversation must
    contribute ONE outcome, not many."""
    rows = [
        # c1: 3 mentions, outcome = venda_fechada (closed → not a loss)
        *[
            _silver_row(
                conversation_id="c1",
                timestamp=_ts(i),
                concorrente_mencionado="Porto",
                conversation_outcome="venda_fechada",
            )
            for i in (0, 30, 60)
        ],
        # c2: 2 mentions, outcome = ghosting (lost)
        *[
            _silver_row(
                conversation_id="c2",
                timestamp=_ts(i),
                concorrente_mencionado="Porto",
                conversation_outcome="ghosting",
            )
            for i in (0, 30)
        ],
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    # 1 of 2 distinct conversations is a loss → 0.5, not 2/5 = 0.4.
    assert out["loss_rate"][0] == 0.5


def test_loss_rate_null_when_no_known_outcome() -> None:
    rows = [
        _silver_row(
            conversation_id="c1",
            timestamp=_ts(0),
            concorrente_mencionado="Porto",
            conversation_outcome=None,
        ),
        _silver_row(
            conversation_id="c2",
            timestamp=_ts(0),
            concorrente_mencionado="Porto",
            conversation_outcome=None,
        ),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    assert out["loss_rate"][0] is None


# ---------------------------------------------------------------------------
# top_states.
# ---------------------------------------------------------------------------


def test_top_states_returns_top_three_by_volume() -> None:
    rows = [
        *[
            _silver_row(
                conversation_id=f"c{i}",
                timestamp=_ts(i),
                concorrente_mencionado="Porto",
                state="SP",
            )
            for i in range(4)
        ],
        *[
            _silver_row(
                conversation_id=f"c{i}",
                timestamp=_ts(i),
                concorrente_mencionado="Porto",
                state="RJ",
            )
            for i in range(10, 12)
        ],
        _silver_row(
            conversation_id="c20",
            timestamp=_ts(20),
            concorrente_mencionado="Porto",
            state="MG",
        ),
        _silver_row(
            conversation_id="c21",
            timestamp=_ts(21),
            concorrente_mencionado="Porto",
            state="BA",
        ),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    top = out["top_states"][0].to_list()
    # Deterministic secondary sort: tied BA / MG resolve alphabetically.
    assert top == ["SP", "RJ", "BA"]


def test_top_states_excludes_null_state() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0), concorrente_mencionado="Porto", state="SP"
        ),
        _silver_row(
            timestamp=_ts(60), concorrente_mencionado="Porto", state=None
        ),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    assert out["top_states"][0].to_list() == ["SP"]


def test_top_states_empty_list_when_no_state_data() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0), concorrente_mencionado="Porto", state=None
        ),
    ]
    out = build_competitor_intel(_silver_lf(rows)).collect()
    assert out["top_states"][0].to_list() == []
