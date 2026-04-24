"""Tests for :func:`pipeline.gold.conversation_scores.build_conversation_scores`.

Builds light Silver-shaped LazyFrames carrying only the columns the
builder reads — full :data:`SILVER_SCHEMA` round-tripping is covered
by the silver tests, not here.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl

from pipeline.gold.conversation_scores import build_conversation_scores
from pipeline.schemas.gold import (
    GOLD_CONVERSATION_SCORES_SCHEMA,
    assert_conversation_scores_schema,
)

# ---------------------------------------------------------------------------
# Helpers — minimal Silver-shape builders.
# ---------------------------------------------------------------------------


_BASE_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _ts(seconds: int) -> datetime:
    """Return a UTC-aware ``datetime`` ``seconds`` past 2026-04-23 noon."""
    return _BASE_TS + timedelta(seconds=seconds)


def _silver_row(
    *,
    conversation_id: str = "c1",
    lead_id: str = "L1",
    campaign_id: str = "K1",
    agent_id: str = "A1",
    timestamp: datetime,
    direction: str = "inbound",
    valor_pago_atual_brl: float | None = None,
    sinistro_historico: bool | None = None,
    concorrente_mencionado: str | None = None,
    veiculo_marca: str | None = None,
    veiculo_modelo: str | None = None,
    veiculo_ano: int | None = None,
    conversation_outcome: str | None = None,
    is_business_hours: bool | None = True,
) -> dict[str, Any]:
    return {
        "conversation_id": conversation_id,
        "lead_id": lead_id,
        "campaign_id": campaign_id,
        "agent_id": agent_id,
        "timestamp": timestamp,
        "direction": direction,
        "valor_pago_atual_brl": valor_pago_atual_brl,
        "sinistro_historico": sinistro_historico,
        "concorrente_mencionado": concorrente_mencionado,
        "veiculo_marca": veiculo_marca,
        "veiculo_modelo": veiculo_modelo,
        "veiculo_ano": veiculo_ano,
        "conversation_outcome": conversation_outcome,
        "metadata": {"is_business_hours": is_business_hours},
    }


def _silver_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    """Materialize ``rows`` as a Silver-shaped ``LazyFrame`` carrying
    only the columns the builder reads, with the dtypes the builder
    expects."""
    df = pl.DataFrame(
        rows,
        schema={
            "conversation_id": pl.String,
            "lead_id": pl.String,
            "campaign_id": pl.String,
            "agent_id": pl.String,
            "timestamp": pl.Datetime("us", time_zone="UTC"),
            "direction": pl.String,
            "valor_pago_atual_brl": pl.Float64,
            "sinistro_historico": pl.Boolean,
            "concorrente_mencionado": pl.String,
            "veiculo_marca": pl.String,
            "veiculo_modelo": pl.String,
            "veiculo_ano": pl.Int32,
            "conversation_outcome": pl.String,
            "metadata": pl.Struct({"is_business_hours": pl.Boolean}),
        },
    )
    return df.lazy()


# ---------------------------------------------------------------------------
# Schema lock + smoke.
# ---------------------------------------------------------------------------


def test_output_matches_declared_schema() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound"),
        _silver_row(timestamp=_ts(60), direction="inbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out.schema == GOLD_CONVERSATION_SCORES_SCHEMA
    # Drift validator runs clean too.
    assert_conversation_scores_schema(out)


def test_one_row_per_conversation_id() -> None:
    rows = [
        _silver_row(conversation_id="c1", timestamp=_ts(0), direction="outbound"),
        _silver_row(conversation_id="c1", timestamp=_ts(30), direction="inbound"),
        _silver_row(conversation_id="c2", timestamp=_ts(0), direction="outbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out.height == 2
    assert set(out["conversation_id"].to_list()) == {"c1", "c2"}


def test_empty_input_emits_empty_frame_with_correct_schema() -> None:
    out = build_conversation_scores(_silver_lf([])).collect()
    assert out.height == 0
    assert out.schema == GOLD_CONVERSATION_SCORES_SCHEMA


# ---------------------------------------------------------------------------
# avg_lead_response_sec — the spec F3-RF-05 pair-walk semantics.
# ---------------------------------------------------------------------------


def test_avg_lead_response_sec_pairs_first_inbound_after_each_outbound() -> None:
    """Sequence: out @ 0, in @ 60, in @ 75, out @ 120, in @ 180.

    Pairs:
    - (out @ 0, in @ 60) -> 60 s
    - (out @ 120, in @ 180) -> 60 s
    The second inbound (@ 75) belongs to the same response burst as the
    first inbound; spec says only the FIRST inbound after each outbound
    counts, so it must NOT contribute.
    """
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound"),
        _silver_row(timestamp=_ts(60), direction="inbound"),
        _silver_row(timestamp=_ts(75), direction="inbound"),
        _silver_row(timestamp=_ts(120), direction="outbound"),
        _silver_row(timestamp=_ts(180), direction="inbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["avg_lead_response_sec"][0] == 60.0
    # First pair delta = 60s.
    assert out["time_to_first_response_sec"][0] == 60.0


def test_avg_lead_response_sec_null_when_no_outbound_to_inbound_pair() -> None:
    """A conversation that only has inbound messages, or only outbound
    messages, has no round-trip — both response-time aggregates are
    null."""
    rows_inbound_only = [
        _silver_row(timestamp=_ts(0), direction="inbound"),
        _silver_row(timestamp=_ts(60), direction="inbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows_inbound_only)).collect()
    assert out["avg_lead_response_sec"][0] is None
    assert out["time_to_first_response_sec"][0] is None


def test_avg_lead_response_sec_isolates_conversations() -> None:
    """Pair walk runs ``over("conversation_id")`` so an outbound in
    one conversation cannot pair with an inbound in another."""
    rows = [
        _silver_row(conversation_id="c1", timestamp=_ts(0), direction="outbound"),
        _silver_row(conversation_id="c2", timestamp=_ts(30), direction="inbound"),
        _silver_row(conversation_id="c1", timestamp=_ts(120), direction="inbound"),
    ]
    out = (
        build_conversation_scores(_silver_lf(rows))
        .collect()
        .sort("conversation_id")
    )
    # c1: pair @ (0, 120) -> 120 s. c2: no pair, null.
    by_conv = dict(
        zip(
            out["conversation_id"].to_list(),
            out["avg_lead_response_sec"].to_list(),
            strict=True,
        )
    )
    assert by_conv["c1"] == 120.0
    assert by_conv["c2"] is None


# ---------------------------------------------------------------------------
# Counts, durations, off-hours.
# ---------------------------------------------------------------------------


def test_msg_counts_and_duration() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound"),
        _silver_row(timestamp=_ts(30), direction="inbound"),
        _silver_row(timestamp=_ts(90), direction="outbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["msgs_inbound"][0] == 1
    assert out["msgs_outbound"][0] == 2
    assert out["duration_sec"][0] == 90
    assert out["first_message_at"][0] == _ts(0)
    assert out["last_message_at"][0] == _ts(90)


def test_off_hours_counts_only_messages_with_is_business_hours_false() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound", is_business_hours=True),
        _silver_row(timestamp=_ts(60), direction="inbound", is_business_hours=False),
        _silver_row(timestamp=_ts(120), direction="inbound", is_business_hours=False),
        _silver_row(timestamp=_ts(180), direction="outbound", is_business_hours=None),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["off_hours_msgs"][0] == 2


def test_off_hours_zero_when_is_business_hours_is_all_null() -> None:
    """Every row has unknown business-hours state — the count column
    must be ``0``, not ``null``. An all-null boolean ``.sum()`` returns
    ``null`` in Polars, so the builder must clamp it."""
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound", is_business_hours=None),
        _silver_row(timestamp=_ts(60), direction="inbound", is_business_hours=None),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["off_hours_msgs"][0] == 0


# ---------------------------------------------------------------------------
# Concorrente + vehicle reconciliation + valor_pago_atual_brl.
# ---------------------------------------------------------------------------


def test_mencionou_concorrente_and_concorrente_citado_pick_most_frequent() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound"),
        _silver_row(timestamp=_ts(30), direction="inbound", concorrente_mencionado="Porto Seguro"),
        _silver_row(timestamp=_ts(60), direction="inbound", concorrente_mencionado="Porto Seguro"),
        _silver_row(timestamp=_ts(90), direction="inbound", concorrente_mencionado="Azul"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["mencionou_concorrente"][0] is True
    assert out["concorrente_citado"][0] == "Porto Seguro"


def test_mencionou_concorrente_false_when_no_competitor_present() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound"),
        _silver_row(timestamp=_ts(30), direction="inbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["mencionou_concorrente"][0] is False
    assert out["concorrente_citado"][0] is None


def test_vehicle_reconciliation_takes_most_frequent_non_null() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0),
            direction="inbound",
            veiculo_marca="Toyota",
            veiculo_modelo="Corolla",
            veiculo_ano=2020,
        ),
        _silver_row(timestamp=_ts(30), direction="outbound"),
        _silver_row(
            timestamp=_ts(60),
            direction="inbound",
            veiculo_marca="Toyota",
            veiculo_modelo="Corolla",
            veiculo_ano=2020,
        ),
        _silver_row(timestamp=_ts(90), direction="inbound", veiculo_marca="Honda"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["veiculo_marca"][0] == "Toyota"
    assert out["veiculo_modelo"][0] == "Corolla"
    assert out["veiculo_ano"][0] == 2020


def test_valor_pago_atual_brl_takes_max() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="inbound", valor_pago_atual_brl=1200.0),
        _silver_row(timestamp=_ts(60), direction="inbound", valor_pago_atual_brl=1500.0),
        _silver_row(timestamp=_ts(120), direction="inbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["valor_pago_atual_brl"][0] == 1500.0


# ---------------------------------------------------------------------------
# Kleene sinistro_historico.
# ---------------------------------------------------------------------------


def test_sinistro_historico_true_when_any_true() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="inbound", sinistro_historico=True),
        _silver_row(timestamp=_ts(30), direction="inbound", sinistro_historico=False),
        _silver_row(timestamp=_ts(60), direction="inbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["sinistro_historico"][0] is True


def test_sinistro_historico_false_when_only_false_present() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="inbound", sinistro_historico=False),
        _silver_row(timestamp=_ts(30), direction="inbound", sinistro_historico=None),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["sinistro_historico"][0] is False


def test_sinistro_historico_null_when_all_null() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="inbound"),
        _silver_row(timestamp=_ts(30), direction="outbound"),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["sinistro_historico"][0] is None


# ---------------------------------------------------------------------------
# conversation_outcome — last non-null wins.
# ---------------------------------------------------------------------------


def test_conversation_outcome_takes_last_non_null() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), direction="outbound", conversation_outcome=None),
        _silver_row(timestamp=_ts(30), direction="inbound", conversation_outcome="em_negociacao"),
        _silver_row(timestamp=_ts(60), direction="outbound", conversation_outcome="venda_fechada"),
        _silver_row(timestamp=_ts(90), direction="inbound", conversation_outcome=None),
    ]
    out = build_conversation_scores(_silver_lf(rows)).collect()
    assert out["conversation_outcome"][0] == "venda_fechada"
