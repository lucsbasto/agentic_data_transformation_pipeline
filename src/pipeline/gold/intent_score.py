"""F3.11 intent score formula (PRD §17.1).

Seven weighted components → Int32 ∈ [0, 100]. F3-RF-10: persona null
OR outcome null ⇒ ``coerencia_outcome_historico_persona = 0.5``.
"""

from __future__ import annotations

from typing import Final

import polars as pl

from pipeline.gold.persona import PERSONA_EXPECTED_OUTCOME

__all__ = [
    "COMPONENT_ORDER",
    "INPUT_COLUMNS",
    "WEIGHTS",
    "compute_intent_score",
    "intent_score_expr",
    "intent_score_from_component_columns",
]


WEIGHTS: Final[dict[str, int]] = {
    "fornecimento_dados_pessoais": 25,
    "velocidade_resposta_normalizada": 20,
    "presenca_termo_fechamento": 15,
    "perguntas_tecnicas_normalizado": 15,
    "ausencia_palavras_evasivas": 10,
    "coerencia_outcome_historico_persona": 10,
    "janela_horaria_comercial": 5,
}

_TOTAL_WEIGHT: Final[int] = 100

if sum(WEIGHTS.values()) != _TOTAL_WEIGHT:  # pragma: no cover - import-time guard
    raise ValueError("WEIGHTS must sum to 100")

COMPONENT_ORDER: Final[tuple[str, ...]] = tuple(WEIGHTS.keys())

INPUT_COLUMNS: Final[tuple[str, ...]] = (
    "forneceu_dado_pessoal",
    "avg_lead_response_sec",
    "closing_phrase_hits",
    "technical_question_hits",
    "evasive_phrase_hits",
    "persona",
    "outcome",
    "off_hours_msgs",
    "num_msgs_inbound",
)

_LATENCY_CAP_SEC: Final[float] = 600.0
_TECHNICAL_HITS_CAP: Final[int] = 3
_EVASIVE_HITS_CAP: Final[int] = 3
_NULL_COERENCIA_FALLBACK: Final[float] = 0.5
_ZERO_INBOUND_FALLBACK: Final[float] = 0.5


def _fornecimento_expr() -> pl.Expr:
    """Binary 1.0 / 0.0 for ``forneceu_dado_pessoal`` (weight 25)."""
    return pl.when(pl.col("forneceu_dado_pessoal")).then(pl.lit(1.0)).otherwise(pl.lit(0.0))


def _velocidade_expr() -> pl.Expr:
    """Response-speed score: 1.0 when the lead responds instantly, 0.0 at or
    beyond the 600-second cap. Null latency (no paired messages) is treated
    as the worst-case cap rather than a missing value (weight 20).
    """
    latency = pl.col("avg_lead_response_sec").fill_null(_LATENCY_CAP_SEC)
    normalized = (latency / _LATENCY_CAP_SEC).clip(lower_bound=0.0, upper_bound=1.0)
    return pl.lit(1.0) - normalized


def _presenca_fechamento_expr() -> pl.Expr:
    """Binary 1.0 if the lead used at least one closing phrase (weight 15)."""
    return pl.when(pl.col("closing_phrase_hits") >= 1).then(pl.lit(1.0)).otherwise(pl.lit(0.0))


def _perguntas_tecnicas_expr() -> pl.Expr:
    """Linearly scale technical-question phrase hits up to the 3-hit cap (weight 15).

    Why a cap at 3: beyond three distinct technical questions the marginal
    intent signal saturates — a lead asking five coverage questions is not
    measurably more likely to close than one asking three.
    """
    hits = pl.col("technical_question_hits").cast(pl.Float64)
    return (hits / _TECHNICAL_HITS_CAP).clip(lower_bound=0.0, upper_bound=1.0)


def _ausencia_evasivas_expr() -> pl.Expr:
    """Inverted evasive-phrase score: 1.0 when no evasive phrases appear,
    decreasing linearly to 0.0 at the 3-hit cap (weight 10).
    """
    hits = pl.col("evasive_phrase_hits").cast(pl.Float64)
    return (pl.lit(1.0) - hits / _EVASIVE_HITS_CAP).clip(lower_bound=0.0, upper_bound=1.0)


def _coerencia_expr() -> pl.Expr:
    """1.0 when the lead's outcome matches the persona's expected outcome set,
    0.5 when persona or outcome is null (F3-RF-10 fallback), 0.0 on mismatch.

    Uses :data:`pipeline.gold.persona.PERSONA_EXPECTED_OUTCOME` as the lookup
    so the coherence definition stays co-located with the persona taxonomy
    (weight 10).
    """
    persona = pl.col("persona")
    outcome = pl.col("outcome")
    match_expr = pl.lit(False)
    for persona_label, expected_outcomes in PERSONA_EXPECTED_OUTCOME.items():
        match_expr = match_expr | (
            (persona == persona_label) & outcome.is_in(list(expected_outcomes))
        )
    return (
        pl.when(persona.is_null() | outcome.is_null())
        .then(pl.lit(_NULL_COERENCIA_FALLBACK))
        .when(match_expr)
        .then(pl.lit(1.0))
        .otherwise(pl.lit(0.0))
    )


def _janela_horaria_expr() -> pl.Expr:
    """Fraction of inbound messages sent during business hours (weight 5).

    0.5 fallback when there are no inbound messages — avoids a zero that
    would unfairly penalise agent-only conversations.
    """
    inbound = pl.col("num_msgs_inbound").cast(pl.Float64)
    off = pl.col("off_hours_msgs").cast(pl.Float64)
    ratio = (off / inbound).clip(lower_bound=0.0, upper_bound=1.0)
    return pl.when(inbound <= 0).then(pl.lit(_ZERO_INBOUND_FALLBACK)).otherwise(pl.lit(1.0) - ratio)


def _component_exprs() -> dict[str, pl.Expr]:
    """Collect all seven component expressions keyed by ``COMPONENT_ORDER``
    name, ready to be passed into :func:`_weighted_sum`.
    """
    return {
        "fornecimento_dados_pessoais": _fornecimento_expr(),
        "velocidade_resposta_normalizada": _velocidade_expr(),
        "presenca_termo_fechamento": _presenca_fechamento_expr(),
        "perguntas_tecnicas_normalizado": _perguntas_tecnicas_expr(),
        "ausencia_palavras_evasivas": _ausencia_evasivas_expr(),
        "coerencia_outcome_historico_persona": _coerencia_expr(),
        "janela_horaria_comercial": _janela_horaria_expr(),
    }


def _weighted_sum(components: dict[str, pl.Expr]) -> pl.Expr:
    """Apply :data:`WEIGHTS` to each component expression and sum to an
    ``Int32`` clipped to [0, 100].
    """
    total = pl.lit(0.0)
    for name, weight in WEIGHTS.items():
        total = total + components[name] * pl.lit(float(weight))
    return total.round(0).cast(pl.Int32).clip(lower_bound=0, upper_bound=100)


def intent_score_expr() -> pl.Expr:
    """Return a Polars expression that computes the PRD §17.1 intent score.

    Reads the columns declared in :data:`INPUT_COLUMNS` from the calling
    frame. Alias the result to ``"intent_score"`` at the call site.
    """
    return _weighted_sum(_component_exprs())


def intent_score_from_component_columns(components: dict[str, str]) -> pl.Expr:
    """Compute the intent score from pre-existing component *columns* rather
    than re-deriving the component expressions.

    ``components`` maps each :data:`COMPONENT_ORDER` name to the column that
    already holds its [0, 1] value. Raises ``ValueError`` on missing keys so
    the caller learns about schema mismatches at plan time, not collect time.
    """
    missing = [name for name in COMPONENT_ORDER if name not in components]
    if missing:
        raise ValueError(f"missing components: {missing}")
    component_exprs = {name: pl.col(components[name]) for name in COMPONENT_ORDER}
    return _weighted_sum(component_exprs)


def compute_intent_score(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Append an ``intent_score`` column to ``lf`` using :func:`intent_score_expr`.

    ``lf`` must contain all columns declared in :data:`INPUT_COLUMNS`.
    """
    return lf.with_columns(intent_score_expr().alias("intent_score"))
