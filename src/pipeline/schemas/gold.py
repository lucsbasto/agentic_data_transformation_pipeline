"""Gold layer schemas — four analytical parquet tables.

The Gold layer takes a COMPLETED Silver batch and produces the four
PRD §6.3 deliverables:

- :data:`GOLD_CONVERSATION_SCORES_SCHEMA` (PRD calls this
  ``conversation_features``; renamed per F3 D10 to disambiguate from
  ML feature stores).
- :data:`GOLD_LEAD_PROFILE_SCHEMA` — one row per lead, carries the
  LLM-classified persona + intent score (PRD §17 + §17.1).
- :data:`GOLD_AGENT_PERFORMANCE_SCHEMA` — one row per agent.
- :data:`GOLD_COMPETITOR_INTEL_SCHEMA` — one row per normalized
  competitor mention.

Each schema ships with an ``assert_<table>_schema(df)`` helper that
raises :class:`pipeline.errors.SchemaDriftError` on mismatch — same
drift-guard pattern :mod:`pipeline.schemas.silver` uses, so every
layer fails loudly rather than silently widening dtypes.
"""

from __future__ import annotations

# LEARN: ``Final`` marks module-level constants we never reassign.
# mypy / ruff enforce it; runtime treats it as a comment.
from typing import Final

import polars as pl

from pipeline.errors import SchemaDriftError

__all__ = [
    "ENGAGEMENT_PROFILE_VALUES",
    "GOLD_AGENT_PERFORMANCE_SCHEMA",
    "GOLD_COMPETITOR_INTEL_SCHEMA",
    "GOLD_CONVERSATION_SCORES_SCHEMA",
    "GOLD_LEAD_PROFILE_SCHEMA",
    "OUTCOME_MIX_STRUCT",
    "PERSONA_VALUES",
    "PRICE_SENSITIVITY_VALUES",
    "SENTIMENT_VALUES",
    "assert_agent_performance_schema",
    "assert_competitor_intel_schema",
    "assert_conversation_scores_schema",
    "assert_lead_profile_schema",
]


# ---------------------------------------------------------------------------
# Closed-set enums.
# ---------------------------------------------------------------------------
# LEARN: tuples (not sets) so iteration order is stable. Stable order
# matters for Polars' ``pl.Enum`` — categories are positional, and a
# reorder would be a breaking change in any cached parquet.

PERSONA_VALUES: Final[tuple[str, ...]] = (
    # PRD §18.2 — eight labels exactly. Persona classifier (Gold lane)
    # rejects anything outside this set; guard-rails R1/R2/R3 force
    # one of these values when their conditions fire.
    "pesquisador_de_preco",
    "comprador_racional",
    "negociador_agressivo",
    "indeciso",
    "comprador_rapido",
    "refem_de_concorrente",
    "bouncer",
    "cacador_de_informacao",
)
"""Closed set of persona labels (PRD §18.2)."""

ENGAGEMENT_PROFILE_VALUES: Final[tuple[str, ...]] = ("hot", "warm", "cold")
"""Three-bucket engagement classification (deterministic, F3 design §5.1)."""

PRICE_SENSITIVITY_VALUES: Final[tuple[str, ...]] = ("low", "medium", "high")
"""Three-bucket price-sensitivity classification (F3-RF-16)."""

SENTIMENT_VALUES: Final[tuple[str, ...]] = (
    # F5 — per-lead sentiment (assessment §"Análise de sentimento do
    # cliente"). Ordered by polarity for stable dashboard sort.
    # "misto" is intentional for leads with multi-conversation
    # alternation; the LLM and hard rules pick one value per lead.
    "positivo",
    "neutro",
    "negativo",
    "misto",
)
"""Closed set of per-lead sentiment labels (F5)."""


# ---------------------------------------------------------------------------
# outcome_mix struct shape — list of (outcome, count) per agent.
# ---------------------------------------------------------------------------
# LEARN: a ``List[Struct]`` is drift-safe — Bronze can widen the
# ``conversation_outcome`` enum at any time and Gold preserves every
# observed value as its own list element. A fixed-field Struct (the
# obvious alternative) would silently drop the new outcome instead.
OUTCOME_MIX_STRUCT: Final[pl.DataType] = pl.List(
    pl.Struct({"outcome": pl.String(), "count": pl.Int32()})
)


# ---------------------------------------------------------------------------
# gold.conversation_scores — one row per conversation_id.
# ---------------------------------------------------------------------------
_CONVERSATION_SCORES_FIELDS: dict[str, pl.DataType] = {
    "conversation_id": pl.String(),
    "lead_id": pl.String(),
    "campaign_id": pl.String(),
    "agent_id": pl.String(),
    # Counts: ``Int32`` is plenty — a single conversation will not
    # accumulate >2 billion messages.
    "msgs_inbound": pl.Int32(),
    "msgs_outbound": pl.Int32(),
    "first_message_at": pl.Datetime("us", time_zone="UTC"),
    "last_message_at": pl.Datetime("us", time_zone="UTC"),
    # Duration in seconds; ``Int64`` accommodates multi-day chats.
    "duration_sec": pl.Int64(),
    # Float64 because both response-time aggregates can be sub-second.
    # PRD §17.1 weight the lead-side latency component out of [0, 1]
    # later; precision matters until then.
    "avg_lead_response_sec": pl.Float64(),
    "time_to_first_response_sec": pl.Float64(),
    "off_hours_msgs": pl.Int32(),
    "mencionou_concorrente": pl.Boolean(),
    "concorrente_citado": pl.String(),
    "veiculo_marca": pl.String(),
    "veiculo_modelo": pl.String(),
    "veiculo_ano": pl.Int32(),
    "valor_pago_atual_brl": pl.Float64(),
    "sinistro_historico": pl.Boolean(),
    "conversation_outcome": pl.String(),
}

GOLD_CONVERSATION_SCORES_SCHEMA: Final[pl.Schema] = pl.Schema(
    _CONVERSATION_SCORES_FIELDS
)
"""Authoritative Polars schema for ``gold.conversation_scores``."""


# ---------------------------------------------------------------------------
# gold.lead_profile — one row per lead_id.
# ---------------------------------------------------------------------------
_LEAD_PROFILE_FIELDS: dict[str, pl.DataType] = {
    "lead_id": pl.String(),
    "conversations": pl.Int32(),
    "closed_count": pl.Int32(),
    # ``close_rate`` ∈ [0, 1]; Float64 keeps the division precise even
    # for leads with hundreds of conversations.
    "close_rate": pl.Float64(),
    "sender_name_normalized": pl.String(),
    "dominant_email_domain": pl.String(),
    "dominant_state": pl.String(),
    "dominant_city": pl.String(),
    # Closed-set enums — fail loudly if the upstream computation ever
    # tries to write a value not in the declared list.
    "engagement_profile": pl.Enum(list(ENGAGEMENT_PROFILE_VALUES)),
    "persona": pl.Enum(list(PERSONA_VALUES)),
    "persona_confidence": pl.Float64(),
    "sentiment": pl.Enum(list(SENTIMENT_VALUES)),
    "sentiment_confidence": pl.Float64(),
    "price_sensitivity": pl.Enum(list(PRICE_SENSITIVITY_VALUES)),
    # ``intent_score`` ∈ [0, 100] — clamped + cast at compute time.
    "intent_score": pl.Int32(),
    "first_seen_at": pl.Datetime("us", time_zone="UTC"),
    "last_seen_at": pl.Datetime("us", time_zone="UTC"),
}

GOLD_LEAD_PROFILE_SCHEMA: Final[pl.Schema] = pl.Schema(_LEAD_PROFILE_FIELDS)
"""Authoritative Polars schema for ``gold.lead_profile``."""


# ---------------------------------------------------------------------------
# gold.agent_performance — one row per agent_id.
# ---------------------------------------------------------------------------
_AGENT_PERFORMANCE_FIELDS: dict[str, pl.DataType] = {
    "agent_id": pl.String(),
    "conversations": pl.Int32(),
    "closed_count": pl.Int32(),
    "close_rate": pl.Float64(),
    # ``avg_response_time_sec`` here IS allowed to use Bronze's
    # agent-side ``metadata.response_time_sec`` — the column name
    # advertises agent latency, not lead latency.
    "avg_response_time_sec": pl.Float64(),
    "outcome_mix": OUTCOME_MIX_STRUCT,
    # ``top_persona_converted`` may be null when the agent has no
    # closed conversations; the enum still constrains non-null values.
    "top_persona_converted": pl.Enum(list(PERSONA_VALUES)),
}

GOLD_AGENT_PERFORMANCE_SCHEMA: Final[pl.Schema] = pl.Schema(
    _AGENT_PERFORMANCE_FIELDS
)
"""Authoritative Polars schema for ``gold.agent_performance``."""


# ---------------------------------------------------------------------------
# gold.competitor_intel — one row per normalized competitor name.
# ---------------------------------------------------------------------------
_COMPETITOR_INTEL_FIELDS: dict[str, pl.DataType] = {
    "competitor": pl.String(),
    "mention_count": pl.Int32(),
    # Float64 with explicit ``null`` semantics — null when no
    # lead-stated current premium has ever been observed for this
    # competitor.
    "avg_quoted_price_brl": pl.Float64(),
    "loss_rate": pl.Float64(),
    "top_states": pl.List(pl.String()),
}

GOLD_COMPETITOR_INTEL_SCHEMA: Final[pl.Schema] = pl.Schema(
    _COMPETITOR_INTEL_FIELDS
)
"""Authoritative Polars schema for ``gold.competitor_intel``."""


# ---------------------------------------------------------------------------
# Drift validators — one per table, all delegating to the shared
# diff helper so the error message format stays identical to Silver.
# ---------------------------------------------------------------------------


def _assert_schema_match(
    df: pl.DataFrame, *, expected: pl.Schema, table_name: str
) -> None:
    """Raise :class:`SchemaDriftError` unless ``df.schema == expected``.

    The error message names every missing / mismatched / extra column
    so a reviewer reading a CI failure can identify the drift without
    re-running the test.
    """
    actual = df.schema
    if actual == expected:
        return
    mismatches: list[str] = []
    for name, dtype in expected.items():
        got = actual.get(name)
        if got is None:
            mismatches.append(f"missing column {name!r}")
        elif got != dtype:
            mismatches.append(f"{name!r}: expected {dtype}, got {got}")
    extras = [c for c in actual if c not in expected]
    for extra in extras:
        mismatches.append(f"unexpected column {extra!r}")
    raise SchemaDriftError(
        f"{table_name} schema mismatch: " + "; ".join(mismatches)
    )


def assert_conversation_scores_schema(df: pl.DataFrame) -> None:
    """Lock ``df`` to :data:`GOLD_CONVERSATION_SCORES_SCHEMA`."""
    _assert_schema_match(
        df,
        expected=GOLD_CONVERSATION_SCORES_SCHEMA,
        table_name="gold.conversation_scores",
    )


def assert_lead_profile_schema(df: pl.DataFrame) -> None:
    """Lock ``df`` to :data:`GOLD_LEAD_PROFILE_SCHEMA`."""
    _assert_schema_match(
        df,
        expected=GOLD_LEAD_PROFILE_SCHEMA,
        table_name="gold.lead_profile",
    )


def assert_agent_performance_schema(df: pl.DataFrame) -> None:
    """Lock ``df`` to :data:`GOLD_AGENT_PERFORMANCE_SCHEMA`."""
    _assert_schema_match(
        df,
        expected=GOLD_AGENT_PERFORMANCE_SCHEMA,
        table_name="gold.agent_performance",
    )


def assert_competitor_intel_schema(df: pl.DataFrame) -> None:
    """Lock ``df`` to :data:`GOLD_COMPETITOR_INTEL_SCHEMA`."""
    _assert_schema_match(
        df,
        expected=GOLD_COMPETITOR_INTEL_SCHEMA,
        table_name="gold.competitor_intel",
    )
