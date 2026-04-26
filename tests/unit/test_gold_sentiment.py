"""F5 sentiment lane — hard rules + combined-prompt parser tests.

Covers spec test matrix T-S1..T-S7 + T-S6b/T-S6c.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pipeline.gold.persona import (
    LeadAggregate,
    classify_with_overrides,
    parse_classifier_reply,
)
from pipeline.gold.sentiment import (
    SENTIMENT_FALLBACK_LABEL,
    SentimentResult,
    evaluate_sentiment_rules,
    validate_sentiment_label,
)
from pipeline.llm.client import LLMResponse


def _agg(
    *,
    lead_id: str = "L0",
    num_msgs: int = 6,
    num_msgs_inbound: int = 3,
    num_msgs_outbound: int = 3,
    outcome: str | None = None,
    mencionou_concorrente: bool = False,
    forneceu_dado_pessoal: bool = True,
) -> LeadAggregate:
    return LeadAggregate(
        lead_id=lead_id,
        num_msgs=num_msgs,
        num_msgs_inbound=num_msgs_inbound,
        num_msgs_outbound=num_msgs_outbound,
        outcome=outcome,
        mencionou_concorrente=mencionou_concorrente,
        competitor_count_distinct=1 if mencionou_concorrente else 0,
        forneceu_dado_pessoal=forneceu_dado_pessoal,
        last_message_at=datetime(2026, 4, 25, tzinfo=UTC),
        conversation_text="lorem",
    )


# ---------------------------------------------------------------------------
# T-S1..T-S4 — hard rules.
# ---------------------------------------------------------------------------


def test_sr1_ghosting_fires_negativo() -> None:
    """Lead barely responded (≤2 inbound) while agent pushed (≥3
    outbound) and conversation has no outcome → negativo."""
    agg = _agg(
        outcome=None,
        num_msgs_inbound=1,
        num_msgs_outbound=4,
        num_msgs=5,
    )
    result = evaluate_sentiment_rules(agg)
    assert result is not None
    assert result.sentiment == "negativo"
    assert result.sentiment_confidence == 0.9
    assert result.sentiment_source == "rule"


def test_sr2_fast_close_fires_positivo() -> None:
    """outcome=venda_fechada AND msgs ≤10 → positivo (mirrors persona R2)."""
    agg = _agg(outcome="venda_fechada", num_msgs=8)
    result = evaluate_sentiment_rules(agg)
    assert result is not None
    assert result.sentiment == "positivo"
    assert result.sentiment_confidence == 1.0
    assert result.sentiment_source == "rule"


def test_sr3_competitor_no_close_fires_negativo() -> None:
    """Competitor mentioned and outcome != venda_fechada → negativo."""
    agg = _agg(
        outcome="nao_fechou",
        mencionou_concorrente=True,
        num_msgs=12,
        num_msgs_inbound=6,
        num_msgs_outbound=6,
    )
    result = evaluate_sentiment_rules(agg)
    assert result is not None
    assert result.sentiment == "negativo"
    assert result.sentiment_confidence == 0.85
    assert result.sentiment_source == "rule"


def test_no_rule_fires_returns_none() -> None:
    """Symmetric inbound/outbound, no competitor, ambiguous outcome
    → fall through to LLM (returns None)."""
    agg = _agg(
        outcome="nao_fechou",
        num_msgs=20,
        num_msgs_inbound=10,
        num_msgs_outbound=10,
        mencionou_concorrente=False,
    )
    assert evaluate_sentiment_rules(agg) is None


def test_sr2_priority_over_sr3_when_both_match() -> None:
    """Lead closed fast (SR2) AND mentioned competitor (SR3 negativo)
    → SR2 wins (positivo). Closed-fast is the stronger positive
    signal even with competitor mention."""
    agg = _agg(
        outcome="venda_fechada",
        num_msgs=5,
        mencionou_concorrente=True,
    )
    result = evaluate_sentiment_rules(agg)
    assert result is not None
    assert result.sentiment == "positivo"


# ---------------------------------------------------------------------------
# T-S5..T-S7 — parse_classifier_reply.
# ---------------------------------------------------------------------------


def test_parse_classifier_reply_valid_json() -> None:
    persona, sentiment = parse_classifier_reply(
        '{"persona": "indeciso", "sentimento": "positivo"}'
    )
    assert persona == "indeciso"
    assert sentiment == "positivo"


def test_parse_classifier_reply_malformed_json() -> None:
    """Non-JSON text → both labels None."""
    assert parse_classifier_reply("not json at all") == (None, None)
    assert parse_classifier_reply("") == (None, None)
    assert parse_classifier_reply('{"persona": "indeciso"') == (None, None)


def test_parse_classifier_reply_unknown_persona_label() -> None:
    """Unknown persona → None for that field, sentiment still validated."""
    persona, sentiment = parse_classifier_reply(
        '{"persona": "ceo_aspirante", "sentimento": "positivo"}'
    )
    assert persona is None
    assert sentiment == "positivo"


def test_parse_classifier_reply_unknown_sentiment_label() -> None:
    """Unknown sentiment → None, persona still validated."""
    persona, sentiment = parse_classifier_reply(
        '{"persona": "indeciso", "sentimento": "ecstatic"}'
    )
    assert persona == "indeciso"
    assert sentiment is None


# ---------------------------------------------------------------------------
# T-S6b — markdown-fence robustness.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fenced",
    [
        '```json\n{"persona": "indeciso", "sentimento": "neutro"}\n```',
        '```\n{"persona": "indeciso", "sentimento": "neutro"}\n```',
        '   ```json   {"persona": "indeciso", "sentimento": "neutro"}   ```   ',
    ],
)
def test_parse_classifier_reply_strips_markdown_fences(fenced: str) -> None:
    """Some providers wrap JSON in ```json ... ``` despite explicit
    instructions otherwise. Parser must tolerate both labels."""
    persona, sentiment = parse_classifier_reply(fenced)
    assert persona == "indeciso"
    assert sentiment == "neutro"


# ---------------------------------------------------------------------------
# T-S6c — half-valid JSON.
# ---------------------------------------------------------------------------


def test_parse_classifier_reply_missing_sentiment_key() -> None:
    """Persona present, sentiment key missing → (persona, None)."""
    persona, sentiment = parse_classifier_reply(
        '{"persona": "indeciso"}'
    )
    assert persona == "indeciso"
    assert sentiment is None


def test_parse_classifier_reply_non_dict_payload() -> None:
    """Valid JSON but not an object → (None, None)."""
    assert parse_classifier_reply('["indeciso"]') == (None, None)
    assert parse_classifier_reply('"indeciso"') == (None, None)


def test_parse_classifier_reply_case_insensitive_labels() -> None:
    """Mixed-case labels accepted via casefold (LLM may shout)."""
    persona, sentiment = parse_classifier_reply(
        '{"persona": "INDECISO", "sentimento": "Positivo"}'
    )
    assert persona == "indeciso"
    assert sentiment == "positivo"


# ---------------------------------------------------------------------------
# validate_sentiment_label — independent enum validator.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "label", ["positivo", "neutro", "negativo", "misto"]
)
def test_validate_sentiment_label_accepts_all_enum_values(label: str) -> None:
    assert validate_sentiment_label(label) == label


def test_validate_sentiment_label_rejects_unknown() -> None:
    assert validate_sentiment_label("happy") is None
    assert validate_sentiment_label("") is None
    assert validate_sentiment_label(None) is None


def test_sentiment_fallback_label_is_valid_enum_member() -> None:
    """The constant the LLM-fallback path emits must itself be valid."""
    assert validate_sentiment_label(SENTIMENT_FALLBACK_LABEL) == "neutro"


# ---------------------------------------------------------------------------
# SentimentResult.skipped sentinel.
# ---------------------------------------------------------------------------


def test_sentiment_result_skipped_has_null_fields() -> None:
    skipped = SentimentResult.skipped()
    assert skipped.sentiment is None
    assert skipped.sentiment_confidence is None
    assert skipped.sentiment_source == "skipped"


# ---------------------------------------------------------------------------
# Defense-in-depth — oversized reply is rejected before json.loads.
# ---------------------------------------------------------------------------


def test_parse_classifier_reply_rejects_oversize_payload() -> None:
    """A pathological multi-megabyte reply must not reach json.loads.

    Bound is intentionally well above any valid combined reply
    (~80 bytes vs 4096 cap) but small enough to keep worker memory
    bounded if ``max_tokens`` ever misconfigures.
    """
    huge = '{"persona": "indeciso", "sentimento": "neutro"}' + "x" * 10_000
    assert parse_classifier_reply(huge) == (None, None)


# ---------------------------------------------------------------------------
# classify_with_overrides — persona rule + sentiment rule merge path.
# ---------------------------------------------------------------------------


def test_classify_with_overrides_attaches_sentiment_rule_to_persona_rule() -> None:
    """When BOTH a persona rule (R2 fast close) AND a sentiment rule
    (SR2 positivo) fire, the LLM is never invoked and the merged
    PersonaResult carries persona + sentiment from their respective
    rule paths."""
    class _RaisingClient:
        """Any cached_call invocation here would be a regression."""

        def cached_call(self, *, system: str, user: str) -> LLMResponse:
            raise AssertionError("LLM must not be called when persona rule fires")

    agg = _agg(
        outcome="venda_fechada",
        num_msgs=4,
        num_msgs_inbound=2,
        num_msgs_outbound=2,
    )
    result = classify_with_overrides(
        agg,
        batch_latest_timestamp=datetime(2026, 4, 25, tzinfo=UTC),
        client=_RaisingClient(),  # type: ignore[arg-type]
    )
    # Persona side: R2 fast close.
    assert result.persona == "comprador_rapido"
    assert result.persona_source == "rule"
    # Sentiment side: SR2 fast close.
    assert result.sentiment == "positivo"
    assert result.sentiment_confidence == 1.0
    assert result.sentiment_source == "rule"


def test_classify_with_overrides_persona_rule_no_sentiment_rule_yields_null_sentiment() -> None:
    """Persona rule fires but no sentiment rule matches → sentiment
    fields stay ``None`` (deliberate: deterministic persona rule does
    not justify spending an LLM call on sentiment alone)."""
    class _RaisingClient:
        def cached_call(self, *, system: str, user: str) -> LLMResponse:
            raise AssertionError("LLM must not be called when persona rule fires")

    agg = _agg(
        outcome=None,
        forneceu_dado_pessoal=False,  # triggers persona R3 → cacador_de_informacao
        num_msgs=20,
        num_msgs_inbound=10,
        num_msgs_outbound=10,
        mencionou_concorrente=False,
    )
    result = classify_with_overrides(
        agg,
        batch_latest_timestamp=datetime(2026, 4, 25, tzinfo=UTC),
        client=_RaisingClient(),  # type: ignore[arg-type]
    )
    assert result.persona == "cacador_de_informacao"
    assert result.persona_source == "rule"
    assert result.sentiment is None
    assert result.sentiment_source == "skipped"
