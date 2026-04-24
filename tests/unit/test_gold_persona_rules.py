"""Tests for the F3.8 persona rule engine + aggregate_leads."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl
import pytest

from pipeline.gold.persona import (
    PERSONA_EXPECTED_OUTCOME,
    LeadAggregate,
    PersonaResult,
    aggregate_leads,
    evaluate_rules,
)
from pipeline.schemas.gold import PERSONA_VALUES

_BASE_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _ts(seconds: int) -> datetime:
    return _BASE_TS + timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# PERSONA_EXPECTED_OUTCOME — lookup contract.
# ---------------------------------------------------------------------------


def test_persona_expected_outcome_covers_every_persona() -> None:
    assert set(PERSONA_EXPECTED_OUTCOME.keys()) == set(PERSONA_VALUES)


def test_persona_expected_outcome_values_are_frozensets() -> None:
    for persona, expected in PERSONA_EXPECTED_OUTCOME.items():
        assert isinstance(expected, frozenset), (
            f"{persona} must map to a frozenset, not {type(expected).__name__}"
        )
        assert len(expected) >= 1


# ---------------------------------------------------------------------------
# evaluate_rules — each rule positive + negative + priority tie-breaks.
# ---------------------------------------------------------------------------


def _agg(
    *,
    num_msgs: int = 5,
    outcome: str | None = None,
    forneceu_dado_pessoal: bool = True,
    last_message_at: datetime | None = None,
    **extras: Any,
) -> LeadAggregate:
    defaults: dict[str, Any] = {
        "lead_id": "L1",
        "num_msgs": num_msgs,
        "num_msgs_inbound": max(0, num_msgs - 1),
        "num_msgs_outbound": 1,
        "outcome": outcome,
        "mencionou_concorrente": False,
        "competitor_count_distinct": 0,
        "forneceu_dado_pessoal": forneceu_dado_pessoal,
        "last_message_at": last_message_at or _ts(0),
        "conversation_text": "",
    }
    defaults.update(extras)
    return LeadAggregate(**defaults)


def test_rule_r2_fires_on_closed_within_10_msgs() -> None:
    agg = _agg(num_msgs=8, outcome="venda_fechada")
    result = evaluate_rules(agg, batch_latest_timestamp=_ts(0))
    assert result is not None
    assert result.persona == "comprador_rapido"
    assert result.persona_source == "rule"
    assert result.persona_confidence == 1.0


def test_rule_r2_does_not_fire_on_unclosed_conversation() -> None:
    agg = _agg(num_msgs=3, outcome="ghosting")
    result = evaluate_rules(agg, batch_latest_timestamp=_ts(0))
    # R2 miss, R1 miss (outcome not null), R3 miss (has personal data)
    # → None (defer to LLM).
    assert result is None


def test_rule_r1_fires_on_short_stale_unanswered_conversation() -> None:
    # 49h stale.
    last_at = _ts(0)
    batch_latest = last_at + timedelta(hours=49)
    agg = _agg(num_msgs=3, outcome=None, last_message_at=last_at)
    result = evaluate_rules(agg, batch_latest_timestamp=batch_latest)
    assert result is not None
    assert result.persona == "bouncer"


def test_rule_r1_does_not_fire_on_fresh_short_conversation() -> None:
    """The D12 staleness check is load-bearing: a fresh 2-message
    conversation is NOT a bouncer — it may still become one."""
    last_at = _ts(0)
    batch_latest = last_at + timedelta(hours=1)  # 1h, not 48+
    agg = _agg(num_msgs=2, outcome=None, last_message_at=last_at)
    result = evaluate_rules(agg, batch_latest_timestamp=batch_latest)
    assert result is None


def test_rule_r3_fires_when_no_personal_data_supplied() -> None:
    agg = _agg(num_msgs=8, outcome=None, forneceu_dado_pessoal=False)
    result = evaluate_rules(agg, batch_latest_timestamp=_ts(0))
    assert result is not None
    assert result.persona == "cacador_de_informacao"


def test_rule_r2_beats_r1_on_closed_short_conversation() -> None:
    """A 4-message closed conversation must fire R2 (comprador_rapido),
    NOT R1 (bouncer). Priority matters: otherwise sales-closers get
    mis-labelled as ghosts."""
    last_at = _ts(0)
    batch_latest = last_at + timedelta(hours=72)  # stale enough for R1
    agg = _agg(num_msgs=4, outcome="venda_fechada", last_message_at=last_at)
    result = evaluate_rules(agg, batch_latest_timestamp=batch_latest)
    assert result is not None
    assert result.persona == "comprador_rapido"


def test_rule_r1_beats_r3_when_both_conditions_match() -> None:
    """A short stale no-data conversation fires R1, not R3 — R1
    precedes R3 in the evaluator."""
    last_at = _ts(0)
    batch_latest = last_at + timedelta(hours=49)
    agg = _agg(
        num_msgs=2,
        outcome=None,
        forneceu_dado_pessoal=False,
        last_message_at=last_at,
    )
    result = evaluate_rules(agg, batch_latest_timestamp=batch_latest)
    assert result is not None
    assert result.persona == "bouncer"


def test_all_rules_miss_returns_none_defers_to_llm() -> None:
    agg = _agg(num_msgs=15, outcome=None, forneceu_dado_pessoal=True)
    result = evaluate_rules(agg, batch_latest_timestamp=_ts(0))
    assert result is None


# ---------------------------------------------------------------------------
# PersonaResult.
# ---------------------------------------------------------------------------


def test_persona_result_skipped_sentinel() -> None:
    r = PersonaResult.skipped()
    assert r.persona is None
    assert r.persona_confidence is None
    assert r.persona_source == "skipped"


def test_persona_result_is_frozen() -> None:
    r = PersonaResult(
        persona="comprador_rapido",
        persona_confidence=1.0,
        persona_source="rule",
    )
    with pytest.raises(FrozenInstanceError):
        r.persona = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# aggregate_leads — Silver -> list[LeadAggregate].
# ---------------------------------------------------------------------------


def _silver_row(
    *,
    lead_id: str = "L1",
    conversation_id: str = "c1",
    timestamp: datetime,
    direction: str = "inbound",
    message_body_masked: str | None = None,
    concorrente_mencionado: str | None = None,
    has_cpf: bool = False,
    has_phone_mention: bool = False,
    email_domain: str | None = None,
    conversation_outcome: str | None = None,
) -> dict[str, Any]:
    return {
        "lead_id": lead_id,
        "conversation_id": conversation_id,
        "timestamp": timestamp,
        "direction": direction,
        "message_body_masked": message_body_masked,
        "concorrente_mencionado": concorrente_mencionado,
        "has_cpf": has_cpf,
        "has_phone_mention": has_phone_mention,
        "email_domain": email_domain,
        "conversation_outcome": conversation_outcome,
    }


def _silver_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.DataFrame(
        rows,
        schema={
            "lead_id": pl.String,
            "conversation_id": pl.String,
            "timestamp": pl.Datetime("us", time_zone="UTC"),
            "direction": pl.String,
            "message_body_masked": pl.String,
            "concorrente_mencionado": pl.String,
            "has_cpf": pl.Boolean,
            "has_phone_mention": pl.Boolean,
            "email_domain": pl.String,
            "conversation_outcome": pl.String,
        },
    ).lazy()


def test_aggregate_leads_computes_counts_and_last_message_at() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0),
            direction="outbound",
            message_body_masked="oi",
        ),
        _silver_row(
            timestamp=_ts(60),
            direction="inbound",
            message_body_masked="bom dia",
        ),
        _silver_row(
            timestamp=_ts(120),
            direction="inbound",
            message_body_masked="quero cotar",
        ),
    ]
    [agg] = aggregate_leads(_silver_lf(rows))
    assert agg.lead_id == "L1"
    assert agg.num_msgs == 3
    assert agg.num_msgs_inbound == 2
    assert agg.num_msgs_outbound == 1
    assert agg.last_message_at == _ts(120)


def test_aggregate_leads_forneceu_dado_pessoal_from_deterministic_signals() -> None:
    """D13: only has_cpf / has_phone_mention / email_domain drive
    ``forneceu_dado_pessoal``. An LLM-only signal (e.g.,
    valor_pago_atual_brl) must NOT flip the bit — we don't even
    read it here."""
    rows = [
        _silver_row(timestamp=_ts(0)),  # nothing provided
    ]
    [agg] = aggregate_leads(_silver_lf(rows))
    assert agg.forneceu_dado_pessoal is False

    rows_with_cpf = [
        _silver_row(timestamp=_ts(0), has_cpf=True),
    ]
    [agg] = aggregate_leads(_silver_lf(rows_with_cpf))
    assert agg.forneceu_dado_pessoal is True


def test_aggregate_leads_forneceu_dado_pessoal_email_only_branch() -> None:
    """Only an observed email_domain is enough to flip the bit —
    locks the OR's second branch so a future refactor cannot drop it."""
    rows = [
        _silver_row(timestamp=_ts(0), email_domain="gmail.com"),
    ]
    [agg] = aggregate_leads(_silver_lf(rows))
    assert agg.forneceu_dado_pessoal is True


def test_aggregate_leads_forneceu_dado_pessoal_phone_only_branch() -> None:
    """Only a phone mention flips the bit — locks the OR's third
    branch."""
    rows = [
        _silver_row(timestamp=_ts(0), has_phone_mention=True),
    ]
    [agg] = aggregate_leads(_silver_lf(rows))
    assert agg.forneceu_dado_pessoal is True


def test_aggregate_leads_competitor_count_distinct_case_insensitive() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), concorrente_mencionado="Porto Seguro"),
        _silver_row(timestamp=_ts(60), concorrente_mencionado="porto seguro"),
        _silver_row(timestamp=_ts(120), concorrente_mencionado="Azul"),
    ]
    [agg] = aggregate_leads(_silver_lf(rows))
    assert agg.mencionou_concorrente is True
    assert agg.competitor_count_distinct == 2  # "porto seguro" collapses


def test_aggregate_leads_conversation_text_concats_last_inbound_bodies() -> None:
    """Only inbound, sorted by timestamp, newline-joined, last 20."""
    rows = [
        _silver_row(
            timestamp=_ts(0),
            direction="outbound",
            message_body_masked="not me",
        ),
        _silver_row(
            timestamp=_ts(60),
            direction="inbound",
            message_body_masked="primeira",
        ),
        _silver_row(
            timestamp=_ts(120),
            direction="inbound",
            message_body_masked="segunda",
        ),
    ]
    [agg] = aggregate_leads(_silver_lf(rows))
    assert agg.conversation_text == "primeira\nsegunda"


def test_aggregate_leads_conversation_text_empty_when_no_inbound_bodies() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0),
            direction="outbound",
            message_body_masked="hi",
        ),
    ]
    [agg] = aggregate_leads(_silver_lf(rows))
    assert agg.conversation_text == ""


def test_aggregate_leads_outcome_last_non_null_across_conversations() -> None:
    """A lead with multiple conversations: outcome aggregates to the
    latest non-null ``conversation_outcome`` observed."""
    rows = [
        _silver_row(
            conversation_id="c1",
            timestamp=_ts(0),
            conversation_outcome="em_negociacao",
        ),
        _silver_row(
            conversation_id="c2",
            timestamp=_ts(3600),
            conversation_outcome="venda_fechada",
        ),
    ]
    [agg] = aggregate_leads(_silver_lf(rows))
    assert agg.outcome == "venda_fechada"
