"""Per-lead sentiment classifier — hard rules + LLM fallback.

Mirrors the persona lane shape (``src/pipeline/gold/persona.py``).
The LLM call is shared with persona via the combined-prompt design
in F5: a single round-trip returns both labels in JSON. This module
owns only the deterministic half (hard rules SR1-SR3) and the
parser/validator for the sentiment side of the combined reply.

Spec drivers
------------
- F5 — assessment §"Análise de sentimento do cliente".
- :data:`pipeline.schemas.gold.SENTIMENT_VALUES` — closed enum.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from pipeline.schemas.gold import SENTIMENT_VALUES

if TYPE_CHECKING:
    # Importing only for type hints avoids the persona ⇄ sentiment
    # cycle at runtime — sentiment.py owns the rule predicates,
    # persona.py wires them into the combined LLM path.
    from pipeline.gold.persona import LeadAggregate

__all__ = [
    "SENTIMENT_FALLBACK_LABEL",
    "SentimentResult",
    "evaluate_sentiment_rules",
    "validate_sentiment_label",
]


# SR thresholds — single source of truth for tests + classifier.
_SR1_INBOUND_MAX: Final[int] = 2
_SR1_OUTBOUND_MIN: Final[int] = 3
_SR2_MAX_MSGS: Final[int] = 10
_SR1_CONFIDENCE: Final[float] = 0.9
_SR2_CONFIDENCE: Final[float] = 1.0
_SR3_CONFIDENCE: Final[float] = 0.85

SENTIMENT_FALLBACK_LABEL: Final[str] = "neutro"
"""LLM-fallback label used when the combined reply lacks a valid
sentiment value (mirrors persona's ``comprador_racional`` fallback)."""

_SENTIMENT_VALUES_SET: Final[frozenset[str]] = frozenset(SENTIMENT_VALUES)


@dataclass(frozen=True, slots=True, kw_only=True)
class SentimentResult:
    """Per-lead sentiment output. ``kw_only`` matches PersonaResult."""

    sentiment: str | None
    sentiment_confidence: float | None
    sentiment_source: str  # 'rule' | 'llm' | 'llm_fallback' | 'skipped'

    @classmethod
    def skipped(cls) -> SentimentResult:
        """Sentinel used when the persona LLM call was budget-skipped
        — sentiment shares the same per-batch budget."""
        return cls(
            sentiment=None,
            sentiment_confidence=None,
            sentiment_source="skipped",
        )


def evaluate_sentiment_rules(agg: LeadAggregate) -> SentimentResult | None:
    """Apply SR2 → SR1 → SR3. First match wins; ``None`` falls through
    to the combined LLM call.

    Priority rationale: SR2 (fast close) is the strongest positive
    signal and runs first so a closed-fast lead with a competitor
    mention still resolves to ``positivo`` rather than the weaker
    SR3 negative signal. SR1 (ghosting) and SR3 (competitor + no
    close) both produce ``negativo`` — SR1 fires first because the
    asymmetric msg-count predicate is cheaper and self-contained.
    """
    if agg.outcome == "venda_fechada" and agg.num_msgs <= _SR2_MAX_MSGS:
        return SentimentResult(
            sentiment="positivo",
            sentiment_confidence=_SR2_CONFIDENCE,
            sentiment_source="rule",
        )
    if (
        agg.outcome is None
        and agg.num_msgs_inbound <= _SR1_INBOUND_MAX
        and agg.num_msgs_outbound >= _SR1_OUTBOUND_MIN
    ):
        return SentimentResult(
            sentiment="negativo",
            sentiment_confidence=_SR1_CONFIDENCE,
            sentiment_source="rule",
        )
    if agg.mencionou_concorrente and agg.outcome != "venda_fechada":
        return SentimentResult(
            sentiment="negativo",
            sentiment_confidence=_SR3_CONFIDENCE,
            sentiment_source="rule",
        )
    return None


def validate_sentiment_label(value: str | None) -> str | None:
    """Return ``value`` if it is a valid sentiment enum member after
    strip + casefold, else ``None``. Used by the combined-prompt
    parser to validate the ``sentimento`` JSON key independently
    from ``persona``."""
    if not value:
        return None
    cleaned = value.strip().casefold()
    return cleaned if cleaned in _SENTIMENT_VALUES_SET else None
