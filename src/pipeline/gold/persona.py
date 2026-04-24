"""Persona classification — rule engine + lead aggregates.

This module ships the deterministic half of the F3 persona lane:

- :class:`LeadAggregate` — per-lead input shape the classifier and
  the hard rules consume.
- :class:`PersonaResult` — per-lead output (persona label or null,
  confidence, source).
- :data:`PERSONA_EXPECTED_OUTCOME` — lookup table F3.11's intent
  score uses for the ``coerencia_outcome_historico_persona``
  component.
- :func:`evaluate_rules` — PRD §18.2's three hard rules in
  priority order R2 → R1 → R3. First hit wins.
- :func:`aggregate_leads` — one pass over the Silver LazyFrame
  producing ``LeadAggregate`` instances.

The LLM classifier + semaphore-bounded concurrency lane land in
F3.9 / F3.10 respectively; this module only emits the scaffolding
those slices will import.

Spec drivers
------------
- F3-RF-09 — persona labels + hard rules.
- F3-RF-17 + D13 — ``forneceu_dado_pessoal`` uses only deterministic
  Silver columns (``has_cpf``, ``email_domain``, ``has_phone_mention``)
  so the rule never inherits an LLM-inferred value.
- D12 — R1 staleness anchored to ``batch_latest_timestamp`` for
  deterministic replay.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

import polars as pl

__all__ = [
    "PERSONA_EXPECTED_OUTCOME",
    "LeadAggregate",
    "PersonaResult",
    "aggregate_leads",
    "evaluate_rules",
]


# ---------------------------------------------------------------------------
# Rule thresholds (PRD §18.2).
# ---------------------------------------------------------------------------
# LEARN: each threshold is a ``Final`` constant so (a) ruff PLR2004
# does not flag the literals and (b) the mapping from rule number to
# boundary stays one source of truth for the classifier + tests.
_R2_MAX_MSGS: Final[int] = 10
_R1_MAX_MSGS: Final[int] = 4
_R1_STALENESS_HOURS: Final[int] = 48
_CONFIDENCE_RULE: Final[float] = 1.0

# Text-budget caps on ``conversation_text`` handed to the LLM prompt
# (design §5.2). Kept here so the LLM slice in F3.9 imports the same
# numbers without duplicating magic values.
_MAX_PROMPT_MESSAGES: Final[int] = 20
_MAX_PROMPT_CHARS: Final[int] = 2000


# ---------------------------------------------------------------------------
# Persona → expected conversation outcome (F3.11 intent-score lookup).
# ---------------------------------------------------------------------------
# LEARN: the value is a ``frozenset`` — a persona like
# ``pesquisador_de_preco`` matches EITHER ``ghosting`` or
# ``nao_fechou``, not a single value. F3.11 maps this to
# ``coerencia_outcome_historico_persona``: 1.0 on match, 0.0 on
# contradiction, 0.5 when persona is null (F3-RF-10 fallback) or the
# actual outcome is unknown.
PERSONA_EXPECTED_OUTCOME: Final[dict[str, frozenset[str]]] = {
    "pesquisador_de_preco": frozenset({"ghosting", "nao_fechou"}),
    "comprador_racional": frozenset({"venda_fechada"}),
    "negociador_agressivo": frozenset({"nao_fechou"}),
    "indeciso": frozenset({"ghosting", "nao_fechou"}),
    "comprador_rapido": frozenset({"venda_fechada"}),
    "refem_de_concorrente": frozenset({"nao_fechou"}),
    "bouncer": frozenset({"ghosting"}),
    "cacador_de_informacao": frozenset({"nao_fechou", "ghosting"}),
}


# ---------------------------------------------------------------------------
# Dataclasses.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LeadAggregate:
    """Per-lead snapshot consumed by the classifier."""

    lead_id: str
    num_msgs: int
    num_msgs_inbound: int
    num_msgs_outbound: int
    outcome: str | None
    mencionou_concorrente: bool
    competitor_count_distinct: int
    forneceu_dado_pessoal: bool
    last_message_at: datetime
    conversation_text: str


@dataclass(frozen=True, slots=True, kw_only=True)
class PersonaResult:
    """Per-lead classifier output. ``kw_only`` keeps call sites
    explicit and leaves room for future fields (cost, latency)
    without breaking positional args."""

    persona: str | None
    persona_confidence: float | None
    persona_source: str  # 'rule' | 'llm' | 'rule_override' | 'skipped'

    @classmethod
    def skipped(cls) -> PersonaResult:
        """Budget-exhausted sentinel. F3.10 returns this when the
        per-batch cap is spent before the lead's turn."""
        return cls(
            persona=None, persona_confidence=None, persona_source="skipped"
        )


# ---------------------------------------------------------------------------
# Hard-rule evaluator (PRD §18.2).
# ---------------------------------------------------------------------------


def evaluate_rules(
    agg: LeadAggregate, *, batch_latest_timestamp: datetime
) -> PersonaResult | None:
    """Apply R2 → R1 → R3 in priority order. Return the forced
    ``PersonaResult`` on the first match; ``None`` when no rule
    fires (LLM path in F3.9 handles the aggregate next).

    Priority rationale (D12-adjacent): R2 runs first so a 4-message
    conversation that closed is classified ``comprador_rapido`` —
    the bouncer rule's "no outcome AND stale" guard would otherwise
    also match such leads on their surface shape. R1's staleness
    check uses ``batch_latest_timestamp`` so re-runs over the same
    Silver are byte-identical; R3 is the catch-all lead-provided-
    nothing signal.
    """
    if agg.outcome == "venda_fechada" and agg.num_msgs <= _R2_MAX_MSGS:
        return PersonaResult(
            persona="comprador_rapido",
            persona_confidence=_CONFIDENCE_RULE,
            persona_source="rule",
        )
    staleness_cutoff = batch_latest_timestamp - timedelta(
        hours=_R1_STALENESS_HOURS
    )
    if (
        agg.num_msgs <= _R1_MAX_MSGS
        and agg.outcome is None
        and agg.last_message_at < staleness_cutoff
    ):
        return PersonaResult(
            persona="bouncer",
            persona_confidence=_CONFIDENCE_RULE,
            persona_source="rule",
        )
    if not agg.forneceu_dado_pessoal:
        return PersonaResult(
            persona="cacador_de_informacao",
            persona_confidence=_CONFIDENCE_RULE,
            persona_source="rule",
        )
    return None


# ---------------------------------------------------------------------------
# Lead aggregation from Silver.
# ---------------------------------------------------------------------------


def aggregate_leads(silver_lf: pl.LazyFrame) -> list[LeadAggregate]:
    """One pass over the Silver LazyFrame → list of
    :class:`LeadAggregate`.

    Call sites that also need the R1 staleness anchor pass
    ``batch_latest_timestamp`` directly to :func:`evaluate_rules`.
    """
    per_conv_outcome = silver_lf.group_by(
        ["lead_id", "conversation_id"], maintain_order=False
    ).agg(
        pl.col("conversation_outcome")
        .sort_by("timestamp")
        .drop_nulls()
        .last()
        .alias("outcome"),
        pl.col("timestamp").max().alias("_conv_end_at"),
    )

    # LEARN: aggregate per-conversation outcomes up to the lead
    # ordered by each conversation's latest timestamp so the final
    # ``outcome`` reflects the chronologically most recent
    # conversation that had a known outcome — not whichever
    # conversation Polars emits first.
    outcome_per_lead = per_conv_outcome.group_by(
        "lead_id", maintain_order=False
    ).agg(
        pl.col("outcome")
        .sort_by("_conv_end_at")
        .drop_nulls()
        .last()
        .alias("outcome"),
    )

    text_per_lead = _build_conversation_text(silver_lf)

    aggregates_lf = silver_lf.group_by("lead_id", maintain_order=False).agg(
        pl.len().cast(pl.Int32).alias("num_msgs"),
        (pl.col("direction") == "inbound")
        .sum()
        .cast(pl.Int32)
        .alias("num_msgs_inbound"),
        (pl.col("direction") == "outbound")
        .sum()
        .cast(pl.Int32)
        .alias("num_msgs_outbound"),
        pl.col("concorrente_mencionado")
        .is_not_null()
        .any()
        .fill_null(value=False)
        .alias("mencionou_concorrente"),
        pl.col("concorrente_mencionado")
        .drop_nulls()
        .str.strip_chars()
        .str.to_lowercase()
        .n_unique()
        .cast(pl.Int32)
        .alias("competitor_count_distinct"),
        (
            pl.col("has_cpf").any().fill_null(value=False)
            | pl.col("has_phone_mention").any().fill_null(value=False)
            | pl.col("email_domain").is_not_null().any().fill_null(value=False)
        ).alias("forneceu_dado_pessoal"),
        pl.col("timestamp").max().alias("last_message_at"),
    )

    joined = aggregates_lf.join(
        outcome_per_lead, on="lead_id", how="left"
    ).join(text_per_lead, on="lead_id", how="left")

    # LEARN: a lead with zero lead-side messages drops out of the
    # text builder; fill the join-null with an empty string so the
    # dataclass contract (``conversation_text: str``) holds.
    finalized = joined.with_columns(
        pl.col("conversation_text").fill_null(pl.lit(""))
    )

    df = finalized.collect()
    return [_row_to_aggregate(row) for row in df.iter_rows(named=True)]


def _build_conversation_text(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Concatenate up to the last ``_MAX_PROMPT_MESSAGES`` non-null
    inbound bodies per lead (sorted by timestamp), newline-joined,
    truncated to ``_MAX_PROMPT_CHARS``."""
    return (
        silver_lf.filter(
            (pl.col("direction") == "inbound")
            & pl.col("message_body_masked").is_not_null()
        )
        .sort(["lead_id", "timestamp"])
        .group_by("lead_id", maintain_order=False)
        .agg(
            pl.col("message_body_masked")
            .tail(_MAX_PROMPT_MESSAGES)
            .str.join("\n")
            .alias("conversation_text")
        )
        .with_columns(
            pl.col("conversation_text")
            .str.slice(0, _MAX_PROMPT_CHARS)
            .alias("conversation_text")
        )
    )


def _row_to_aggregate(row: dict[str, object]) -> LeadAggregate:
    """Convert a collected row into a :class:`LeadAggregate`."""
    return LeadAggregate(
        lead_id=str(row["lead_id"]),
        num_msgs=int(row["num_msgs"]),  # type: ignore[arg-type]
        num_msgs_inbound=int(row["num_msgs_inbound"]),  # type: ignore[arg-type]
        num_msgs_outbound=int(row["num_msgs_outbound"]),  # type: ignore[arg-type]
        outcome=row["outcome"],  # type: ignore[arg-type]
        mencionou_concorrente=bool(row["mencionou_concorrente"]),
        competitor_count_distinct=int(row["competitor_count_distinct"]),  # type: ignore[arg-type]
        forneceu_dado_pessoal=bool(row["forneceu_dado_pessoal"]),
        last_message_at=row["last_message_at"],  # type: ignore[arg-type]
        conversation_text=str(row["conversation_text"] or ""),
    )
