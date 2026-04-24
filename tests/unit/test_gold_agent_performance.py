"""Tests for :func:`pipeline.gold.agent_performance.build_agent_performance`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl

from pipeline.gold.agent_performance import build_agent_performance
from pipeline.schemas.gold import (
    GOLD_AGENT_PERFORMANCE_SCHEMA,
    PERSONA_VALUES,
    assert_agent_performance_schema,
)

_BASE_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _ts(seconds: int) -> datetime:
    return _BASE_TS + timedelta(seconds=seconds)


def _silver_row(
    *,
    conversation_id: str = "c1",
    lead_id: str = "L1",
    agent_id: str = "A1",
    timestamp: datetime,
    direction: str = "inbound",
    conversation_outcome: str | None = None,
    response_time_sec: int | None = None,
) -> dict[str, Any]:
    return {
        "conversation_id": conversation_id,
        "lead_id": lead_id,
        "agent_id": agent_id,
        "timestamp": timestamp,
        "direction": direction,
        "conversation_outcome": conversation_outcome,
        "metadata": {"response_time_sec": response_time_sec},
    }


def _silver_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.DataFrame(
        rows,
        schema={
            "conversation_id": pl.String,
            "lead_id": pl.String,
            "agent_id": pl.String,
            "timestamp": pl.Datetime("us", time_zone="UTC"),
            "direction": pl.String,
            "conversation_outcome": pl.String,
            "metadata": pl.Struct({"response_time_sec": pl.Int32}),
        },
    ).lazy()


def _personas(pairs: list[tuple[str, str]]) -> pl.LazyFrame:
    return pl.DataFrame(
        pairs,
        schema={"lead_id": pl.String, "persona": pl.String},
        orient="row",
    ).lazy()


# ---------------------------------------------------------------------------
# Schema lock + grain.
# ---------------------------------------------------------------------------


def test_output_matches_declared_schema() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound", response_time_sec=30),
        _silver_row(
            timestamp=_ts(60),
            direction="inbound",
            conversation_outcome="venda_fechada",
        ),
    ]
    out = build_agent_performance(_silver_lf(rows)).collect()
    assert out.schema == GOLD_AGENT_PERFORMANCE_SCHEMA
    assert_agent_performance_schema(out)


def test_one_row_per_agent_id() -> None:
    rows = [
        _silver_row(
            agent_id="A1",
            conversation_id="c1",
            timestamp=_ts(0),
            direction="outbound",
        ),
        _silver_row(
            agent_id="A1",
            conversation_id="c1",
            timestamp=_ts(30),
            direction="inbound",
            conversation_outcome="venda_fechada",
        ),
        _silver_row(
            agent_id="A2",
            conversation_id="c2",
            timestamp=_ts(0),
            direction="outbound",
        ),
    ]
    out = build_agent_performance(_silver_lf(rows)).collect().sort("agent_id")
    assert out.height == 2


def test_empty_input_emits_empty_frame() -> None:
    out = build_agent_performance(_silver_lf([])).collect()
    assert out.height == 0
    assert out.schema == GOLD_AGENT_PERFORMANCE_SCHEMA


# ---------------------------------------------------------------------------
# Counts + close_rate on distinct conversations.
# ---------------------------------------------------------------------------


def test_close_rate_on_distinct_conversations() -> None:
    """Two conversations: one closed, one ghosting → close_rate = 0.5
    even when each conversation has many rows."""
    rows = []
    # c1: closed
    for i in range(5):
        rows.append(
            _silver_row(
                conversation_id="c1",
                timestamp=_ts(i),
                conversation_outcome="venda_fechada" if i == 4 else None,
            )
        )
    # c2: ghosting
    for i in range(3):
        rows.append(
            _silver_row(
                conversation_id="c2",
                timestamp=_ts(10 + i),
                conversation_outcome="ghosting" if i == 2 else None,
            )
        )
    out = build_agent_performance(_silver_lf(rows)).collect()
    assert out["conversations"][0] == 2
    assert out["closed_count"][0] == 1
    assert out["close_rate"][0] == 0.5


# ---------------------------------------------------------------------------
# avg_response_time_sec.
# ---------------------------------------------------------------------------


def test_avg_response_time_uses_outbound_metadata() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound", response_time_sec=30),
        _silver_row(timestamp=_ts(60), direction="outbound", response_time_sec=90),
        _silver_row(timestamp=_ts(120), direction="inbound", response_time_sec=999),
    ]
    out = build_agent_performance(_silver_lf(rows)).collect()
    # Inbound row's response_time_sec must be ignored.
    assert out["avg_response_time_sec"][0] == 60.0


def test_avg_response_time_null_when_no_outbound_data() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="inbound", response_time_sec=30),
    ]
    out = build_agent_performance(_silver_lf(rows)).collect()
    assert out["avg_response_time_sec"][0] is None


# ---------------------------------------------------------------------------
# outcome_mix — drift-safe List[Struct{outcome, count}].
# ---------------------------------------------------------------------------


def test_outcome_mix_preserves_unknown_outcomes() -> None:
    """A future Bronze outcome value lands as its own struct entry,
    not silently dropped — that's the whole point of D8."""
    rows = [
        _silver_row(
            conversation_id="c1",
            timestamp=_ts(0),
            conversation_outcome="venda_fechada",
        ),
        _silver_row(
            conversation_id="c2",
            timestamp=_ts(0),
            conversation_outcome="ghosting",
        ),
        _silver_row(
            conversation_id="c3",
            timestamp=_ts(0),
            conversation_outcome="reagendado_for_2030",
        ),
    ]
    out = build_agent_performance(_silver_lf(rows)).collect()
    mix = out["outcome_mix"][0].to_list()
    by_outcome = {entry["outcome"]: entry["count"] for entry in mix}
    assert by_outcome == {
        "venda_fechada": 1,
        "ghosting": 1,
        "reagendado_for_2030": 1,
    }


def test_outcome_mix_empty_list_when_no_known_outcome() -> None:
    rows = [
        _silver_row(
            conversation_id="c1", timestamp=_ts(0), conversation_outcome=None
        ),
    ]
    out = build_agent_performance(_silver_lf(rows)).collect()
    assert out["outcome_mix"][0].to_list() == []


# ---------------------------------------------------------------------------
# top_persona_converted.
# ---------------------------------------------------------------------------


def test_top_persona_null_when_no_lead_personas_provided() -> None:
    rows = [
        _silver_row(
            conversation_id="c1",
            timestamp=_ts(0),
            conversation_outcome="venda_fechada",
        ),
    ]
    out = build_agent_performance(_silver_lf(rows)).collect()
    assert out["top_persona_converted"][0] is None


def test_top_persona_null_when_agent_has_zero_closes() -> None:
    rows = [
        _silver_row(
            conversation_id="c1",
            lead_id="L1",
            timestamp=_ts(0),
            conversation_outcome="ghosting",
        ),
    ]
    personas = _personas([("L1", "comprador_racional")])
    out = build_agent_performance(_silver_lf(rows), lead_personas=personas).collect()
    assert out["closed_count"][0] == 0
    assert out["top_persona_converted"][0] is None


def test_top_persona_picks_highest_close_rate_persona() -> None:
    """Two leads of persona A close 1/1 (100%), three leads of
    persona B close 1/3 (33%). Top = A."""
    rows = []
    for i, (cid, lid, outcome) in enumerate(
        [
            ("c1", "L1", "venda_fechada"),  # comprador_racional, closed
            ("c2", "L2", "venda_fechada"),  # comprador_racional, closed
            ("c3", "L3", "venda_fechada"),  # indeciso, closed
            ("c4", "L4", "ghosting"),  # indeciso, lost
            ("c5", "L5", "ghosting"),  # indeciso, lost
        ]
    ):
        rows.append(
            _silver_row(
                conversation_id=cid,
                lead_id=lid,
                timestamp=_ts(i),
                conversation_outcome=outcome,
            )
        )
    personas = _personas(
        [
            ("L1", "comprador_racional"),
            ("L2", "comprador_racional"),
            ("L3", "indeciso"),
            ("L4", "indeciso"),
            ("L5", "indeciso"),
        ]
    )
    out = build_agent_performance(_silver_lf(rows), lead_personas=personas).collect()
    assert out["top_persona_converted"][0] == "comprador_racional"


def test_top_persona_tie_break_on_absolute_closed_count() -> None:
    """Both personas have 100% close_rate (1/1 each). Tie-break:
    absolute closed_count — equal. Final tie-break: persona ascending
    → ``comprador_rapido`` < ``indeciso`` alphabetically."""
    rows = [
        _silver_row(
            conversation_id="c1",
            lead_id="L1",
            timestamp=_ts(0),
            conversation_outcome="venda_fechada",
        ),
        _silver_row(
            conversation_id="c2",
            lead_id="L2",
            timestamp=_ts(60),
            conversation_outcome="venda_fechada",
        ),
    ]
    personas = _personas(
        [("L1", "indeciso"), ("L2", "comprador_rapido")]
    )
    out = build_agent_performance(_silver_lf(rows), lead_personas=personas).collect()
    # comprador_rapido sorts before indeciso → wins after rate + closed ties.
    assert out["top_persona_converted"][0] == "comprador_rapido"
    # Still constrained to the persona enum.
    assert out["top_persona_converted"].dtype == pl.Enum(list(PERSONA_VALUES))
