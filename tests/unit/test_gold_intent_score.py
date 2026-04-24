"""Tests for F3.11 intent_score formula — RED side of TDD.

The module under test (pipeline.gold.intent_score) does NOT exist yet.
Every test here is expected to fail with ImportError / ModuleNotFoundError.
"""

from __future__ import annotations

import itertools

import polars as pl
import pytest

from pipeline.gold.intent_score import (  # type: ignore[import-untyped]
    COMPONENT_ORDER,
    INPUT_COLUMNS,
    WEIGHTS,
    compute_intent_score,
    intent_score_expr,
    intent_score_from_component_columns,
)
from pipeline.gold.persona import PERSONA_EXPECTED_OUTCOME  # type: ignore[import-untyped]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COMPONENT_NAMES = (
    "fornecimento_dados_pessoais",
    "velocidade_resposta_normalizada",
    "presenca_termo_fechamento",
    "perguntas_tecnicas_normalizado",
    "ausencia_palavras_evasivas",
    "coerencia_outcome_historico_persona",
    "janela_horaria_comercial",
)

# Baseline lead: every component contributes 0 points EXCEPT
# coerencia_outcome_historico_persona which fires at 0.5 (null fallback).
# Expected baseline score = 0.5 * 10 = 5.
_BASELINE: dict[str, object] = {
    "forneceu_dado_pessoal": False,  # fornecimento = 0.0  → 0 pts
    "avg_lead_response_sec": 600.0,  # velocidade = 0.0   → 0 pts
    "closing_phrase_hits": 0,  # presenca = 0.0     → 0 pts
    "technical_question_hits": 0,  # perguntas = 0.0    → 0 pts
    "evasive_phrase_hits": 3,  # ausencia = 0.0     → 0 pts
    "persona": None,  # coerencia = 0.5    → 5 pts
    "outcome": None,
    "off_hours_msgs": 2,  # janela = 0.0       → 0 pts
    "num_msgs_inbound": 2,
}
_BASELINE_SCORE = 5


def _make_lf(**overrides: object) -> pl.LazyFrame:
    row = {**_BASELINE, **overrides}
    return pl.LazyFrame(
        {
            "forneceu_dado_pessoal": pl.Series([row["forneceu_dado_pessoal"]], dtype=pl.Boolean),
            "avg_lead_response_sec": pl.Series([row["avg_lead_response_sec"]], dtype=pl.Float64),
            "closing_phrase_hits": pl.Series([row["closing_phrase_hits"]], dtype=pl.Int32),
            "technical_question_hits": pl.Series([row["technical_question_hits"]], dtype=pl.Int32),
            "evasive_phrase_hits": pl.Series([row["evasive_phrase_hits"]], dtype=pl.Int32),
            "persona": pl.Series([row["persona"]], dtype=pl.Utf8),
            "outcome": pl.Series([row["outcome"]], dtype=pl.Utf8),
            "off_hours_msgs": pl.Series([row["off_hours_msgs"]], dtype=pl.Int32),
            "num_msgs_inbound": pl.Series([row["num_msgs_inbound"]], dtype=pl.Int32),
        }
    )


def _score(**overrides: object) -> int:
    lf = _make_lf(**overrides)
    return int(compute_intent_score(lf).collect()["intent_score"][0])


# ---------------------------------------------------------------------------
# 1. Module invariants
# ---------------------------------------------------------------------------


def test_weights_sum_to_100() -> None:
    assert sum(WEIGHTS.values()) == 100


def test_component_order_matches_weights_keys() -> None:
    assert tuple(WEIGHTS.keys()) == COMPONENT_ORDER


def test_weights_contains_all_seven_components() -> None:
    for name in _COMPONENT_NAMES:
        assert name in WEIGHTS, f"Missing component in WEIGHTS: {name}"


def test_input_columns_are_declared() -> None:
    expected = {
        "forneceu_dado_pessoal",
        "avg_lead_response_sec",
        "closing_phrase_hits",
        "technical_question_hits",
        "evasive_phrase_hits",
        "persona",
        "outcome",
        "off_hours_msgs",
        "num_msgs_inbound",
    }
    assert set(INPUT_COLUMNS) == expected


def test_input_columns_is_a_tuple() -> None:
    assert isinstance(INPUT_COLUMNS, tuple)


def test_component_order_is_a_tuple() -> None:
    assert isinstance(COMPONENT_ORDER, tuple)


# ---------------------------------------------------------------------------
# 2. Individual component behaviour
# ---------------------------------------------------------------------------


class TestFornecimentoDadosPessoais:
    def test_true_contributes_full_weight(self) -> None:
        delta = _score(forneceu_dado_pessoal=True) - _BASELINE_SCORE
        assert delta == WEIGHTS["fornecimento_dados_pessoais"]

    def test_false_contributes_zero(self) -> None:
        assert _score() == _BASELINE_SCORE


class TestVelocidadeRespostaNormalizada:
    def test_zero_latency_gives_full_weight(self) -> None:
        delta = _score(avg_lead_response_sec=0.0) - _BASELINE_SCORE
        assert delta == WEIGHTS["velocidade_resposta_normalizada"]

    def test_600s_latency_gives_zero(self) -> None:
        # 1 - min(600/600, 1) = 0.0
        assert _score(avg_lead_response_sec=600.0) == _BASELINE_SCORE

    def test_latency_above_600s_saturates_at_zero(self) -> None:
        # min(1200/600, 1) = 1 → score 0
        assert _score(avg_lead_response_sec=1200.0) == _BASELINE_SCORE

    def test_null_latency_gives_zero(self) -> None:
        assert _score(avg_lead_response_sec=None) == _BASELINE_SCORE


class TestPresencaTermoFechamento:
    def test_one_hit_contributes_full_weight(self) -> None:
        delta = _score(closing_phrase_hits=1) - _BASELINE_SCORE
        assert delta == WEIGHTS["presenca_termo_fechamento"]

    def test_many_hits_does_not_exceed_full_weight(self) -> None:
        delta = _score(closing_phrase_hits=99) - _BASELINE_SCORE
        assert delta == WEIGHTS["presenca_termo_fechamento"]

    def test_zero_hits_contributes_zero(self) -> None:
        assert _score(closing_phrase_hits=0) == _BASELINE_SCORE


class TestPerguntasTecnicasNormalizado:
    def test_three_hits_saturates_at_full_weight(self) -> None:
        delta = _score(technical_question_hits=3) - _BASELINE_SCORE
        assert delta == WEIGHTS["perguntas_tecnicas_normalizado"]

    def test_above_three_still_gives_full_weight(self) -> None:
        delta = _score(technical_question_hits=10) - _BASELINE_SCORE
        assert delta == WEIGHTS["perguntas_tecnicas_normalizado"]

    def test_zero_hits_gives_zero(self) -> None:
        assert _score(technical_question_hits=0) == _BASELINE_SCORE


class TestAusenciaPalavrasEvasivas:
    def test_zero_evasive_hits_gives_full_weight(self) -> None:
        delta = _score(evasive_phrase_hits=0) - _BASELINE_SCORE
        assert delta == WEIGHTS["ausencia_palavras_evasivas"]

    def test_three_or_more_evasive_hits_gives_zero(self) -> None:
        assert _score(evasive_phrase_hits=3) == _BASELINE_SCORE
        assert _score(evasive_phrase_hits=5) == _BASELINE_SCORE


class TestJanelaHorariaComercial:
    def test_all_business_hours_gives_full_weight(self) -> None:
        # off_hours=0, inbound=4 → 1 - 0/4 = 1.0
        score = _score(off_hours_msgs=0, num_msgs_inbound=4)
        assert score > _BASELINE_SCORE

    def test_all_off_hours_gives_zero(self) -> None:
        # off_hours=4, inbound=4 → 1 - 1 = 0.0
        all_off = _score(off_hours_msgs=4, num_msgs_inbound=4)
        assert all_off == _BASELINE_SCORE

    def test_zero_inbound_gives_half_weight_ordering(self) -> None:
        """0.5 * weight is fractional for weight=5; assert ordering only."""
        all_off = _score(off_hours_msgs=2, num_msgs_inbound=2)
        zero_inbound = _score(off_hours_msgs=0, num_msgs_inbound=0)
        all_business = _score(off_hours_msgs=0, num_msgs_inbound=4)
        assert all_off < zero_inbound < all_business


# ---------------------------------------------------------------------------
# 3. F3-RF-10 coerencia_outcome_historico_persona carve-out
# ---------------------------------------------------------------------------


class TestCoerenciaOutcomeHistoricoPersona:
    def test_persona_null_gives_half_fallback(self) -> None:
        # Baseline already has persona=None → score = 5 (0.5 * 10)
        assert _score() == _BASELINE_SCORE

    def test_outcome_null_gives_half_fallback(self) -> None:
        # persona set to a valid value but outcome is null → 0.5
        persona = next(iter(PERSONA_EXPECTED_OUTCOME))
        score = _score(persona=persona, outcome=None)
        assert score == _BASELINE_SCORE  # coerencia still 0.5 → same 5 pts

    def test_match_gives_full_component_weight(self) -> None:
        # persona=match, outcome=match → coerencia=1.0 → full weight (10 pts).
        # All other components stay at 0 (baseline), so absolute score equals
        # the coerencia weight. Delta from null-baseline is half the weight
        # because null also contributes 0.5 * weight.
        persona = "comprador_rapido"  # expects venda_fechada
        matching_outcome = "venda_fechada"
        score = _score(persona=persona, outcome=matching_outcome)
        assert score == WEIGHTS["coerencia_outcome_historico_persona"]

    def test_contradiction_gives_zero(self) -> None:
        persona = "comprador_rapido"  # expects venda_fechada
        contradiction = "ghosting"
        # coerencia=0.0 → 0 pts; baseline 5 comes from null fallback (0.5 * 10)
        # but here persona+outcome are both set → no null fallback → score = 0
        assert _score(persona=persona, outcome=contradiction) == 0

    @pytest.mark.parametrize(
        "persona,expected_outcomes",
        [(p, list(outcomes)) for p, outcomes in PERSONA_EXPECTED_OUTCOME.items()],
    )
    def test_each_persona_match_gives_full_component_weight(
        self, persona: str, expected_outcomes: list[str]
    ) -> None:
        for outcome in expected_outcomes:
            score = _score(persona=persona, outcome=outcome)
            assert score == WEIGHTS["coerencia_outcome_historico_persona"], (
                f"persona={persona}, outcome={outcome} should score full weight"
            )

    @pytest.mark.parametrize(
        "persona",
        list(PERSONA_EXPECTED_OUTCOME.keys()),
    )
    def test_each_persona_contradiction_gives_zero_coerencia(self, persona: str) -> None:
        """Pick an outcome that is NOT in PERSONA_EXPECTED_OUTCOME[persona]."""
        all_outcomes = {"venda_fechada", "ghosting", "nao_fechou"}
        contradictions = all_outcomes - PERSONA_EXPECTED_OUTCOME[persona]
        if not contradictions:
            pytest.skip(f"No contradiction outcome available for {persona}")
        contradiction = next(iter(contradictions))
        # coerencia=0.0 → score should be 0 (baseline 5 - 5 = 0)
        assert _score(persona=persona, outcome=contradiction) == 0


# ---------------------------------------------------------------------------
# 4. Property test: intent_score_from_component_columns over {0,0.5,1}^7
# ---------------------------------------------------------------------------


def test_property_score_dtype_and_bounds_over_component_cube() -> None:
    """3^7 = 2187 rows through intent_score_from_component_columns.

    Asserts:
    - output dtype is pl.Int32
    - min >= 0, max <= 100
    - all-zero row scores 0
    - all-one row scores 100
    """
    values = [0.0, 0.5, 1.0]
    component_names = list(COMPONENT_ORDER)
    rows: list[dict[str, float]] = [
        dict(zip(component_names, combo, strict=True))
        for combo in itertools.product(values, repeat=len(component_names))
    ]
    df = pl.DataFrame(rows)

    col_map = {name: name for name in component_names}
    result = (
        df.lazy()
        .with_columns(intent_score_from_component_columns(col_map).alias("intent_score"))
        .collect()
    )

    col = result["intent_score"]
    assert col.dtype == pl.Int32
    assert int(col.min()) >= 0  # type: ignore[arg-type]
    assert int(col.max()) <= 100  # type: ignore[arg-type]

    # all-zero row
    all_zero_mask = pl.all_horizontal([pl.col(n) == 0.0 for n in component_names])
    zero_rows = result.filter(all_zero_mask)
    assert (zero_rows["intent_score"] == 0).all()

    # all-one row
    all_one_mask = pl.all_horizontal([pl.col(n) == 1.0 for n in component_names])
    one_rows = result.filter(all_one_mask)
    assert (one_rows["intent_score"] == 100).all()


# ---------------------------------------------------------------------------
# 5. compute_intent_score end-to-end
# ---------------------------------------------------------------------------


def test_perfect_lead_scores_100() -> None:
    """All components at maximum → 100."""
    perfect = {
        "forneceu_dado_pessoal": True,
        "avg_lead_response_sec": 0.0,
        "closing_phrase_hits": 1,
        "technical_question_hits": 3,
        "evasive_phrase_hits": 0,
        "persona": "comprador_rapido",
        "outcome": "venda_fechada",
        "off_hours_msgs": 0,
        "num_msgs_inbound": 4,
    }
    assert _score(**perfect) == 100


def test_drop_dead_lead_scores_0() -> None:
    """All components at minimum → 0."""
    worst = {
        "forneceu_dado_pessoal": False,
        "avg_lead_response_sec": 600.0,
        "closing_phrase_hits": 0,
        "technical_question_hits": 0,
        "evasive_phrase_hits": 3,
        "persona": "comprador_rapido",  # contradiction → 0
        "outcome": "ghosting",
        "off_hours_msgs": 2,
        "num_msgs_inbound": 2,
    }
    assert _score(**worst) == 0


def test_compute_intent_score_adds_intent_score_column() -> None:
    lf = _make_lf()
    result = compute_intent_score(lf).collect()
    assert "intent_score" in result.columns


def test_compute_intent_score_output_dtype_is_int32() -> None:
    lf = _make_lf()
    result = compute_intent_score(lf).collect()
    assert result["intent_score"].dtype == pl.Int32


# ---------------------------------------------------------------------------
# 6. Laziness: intent_score_expr stays lazy
# ---------------------------------------------------------------------------


def test_intent_score_expr_in_with_columns_returns_lazyframe() -> None:
    lf = _make_lf()
    result = lf.with_columns(intent_score_expr().alias("intent_score"))
    assert isinstance(result, pl.LazyFrame)


def test_compute_intent_score_returns_lazyframe() -> None:
    lf = _make_lf()
    result = compute_intent_score(lf)
    assert isinstance(result, pl.LazyFrame)


# ---------------------------------------------------------------------------
# 7. intent_score_from_component_columns raises on missing components
# ---------------------------------------------------------------------------


def test_missing_component_raises_value_error() -> None:
    partial_map = {name: name for name in list(COMPONENT_ORDER)[:-1]}  # drop last
    with pytest.raises(ValueError, match="missing components"):
        intent_score_from_component_columns(partial_map)


def test_empty_component_map_raises_value_error() -> None:
    with pytest.raises(ValueError, match="missing components"):
        intent_score_from_component_columns({})


def test_extra_keys_do_not_raise() -> None:
    """Passing extra keys beyond the 7 required should be tolerated."""
    full_map = {name: name for name in COMPONENT_ORDER}
    full_map["extra_column"] = "extra_column"
    # Should not raise — just ignore extras
    expr = intent_score_from_component_columns(full_map)
    assert expr is not None
