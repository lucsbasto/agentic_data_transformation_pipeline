"""Build :data:`GOLD_AGENT_PERFORMANCE_SCHEMA` from a Silver LazyFrame.

One row per ``agent_id``. Pure Polars; the persona-dependent
``top_persona_converted`` column accepts an optional
``lead_personas`` LazyFrame so this builder can run before the F3.10
persona lane has produced any labels (CLI passes it later via the
F3.14 orchestrator).

Spec drivers
------------
- F3-RF-07 — column list.
- D8 — ``outcome_mix`` as ``List[Struct{outcome, count}]`` so unknown
  Bronze outcomes are preserved instead of silently dropped.
- Design §7.1 — distinct-conversation close_rate, agent-side avg
  response time, top_persona tie-break (highest close_rate, then
  absolute closed_count).
"""

from __future__ import annotations

import polars as pl

from pipeline.schemas.gold import (
    GOLD_AGENT_PERFORMANCE_SCHEMA,
    OUTCOME_MIX_STRUCT,
    PERSONA_VALUES,
)

__all__ = ["build_agent_performance"]


def build_agent_performance(
    silver_lf: pl.LazyFrame,
    *,
    lead_personas: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Aggregate Silver rows into one row per ``agent_id``.

    ``lead_personas`` (optional) is a LazyFrame with columns
    ``(lead_id, persona)``. When ``None`` (default), every row's
    ``top_persona_converted`` is null — the F3.14 orchestrator wires
    the populated frame after the persona lane has run.
    """
    per_conv = _aggregate_per_conversation(silver_lf)

    main = (
        per_conv.group_by("agent_id", maintain_order=False)
        .agg(
            pl.len().cast(pl.Int32).alias("conversations"),
            pl.col("outcome")
            .eq("venda_fechada")
            .fill_null(value=False)
            .sum()
            .cast(pl.Int32)
            .alias("closed_count"),
        )
        .with_columns(
            (
                pl.col("closed_count").cast(pl.Float64)
                / pl.col("conversations").cast(pl.Float64)
            ).alias("close_rate")
        )
    )

    avg_resp = _avg_outbound_response(silver_lf)
    outcome_mix = _outcome_mix(per_conv)
    top_persona = _top_persona_converted(per_conv, lead_personas=lead_personas)

    joined = (
        main.join(avg_resp, on="agent_id", how="left")
        .join(outcome_mix, on="agent_id", how="left")
        .join(top_persona, on="agent_id", how="left")
    )

    # LEARN: an agent with zero closed conversations may still
    # surface a persona via the 0/N rate tie inside
    # ``_top_persona_converted``. Gate to null so the contract
    # (``null when zero closes``) holds regardless of how the
    # tie-break ranks zero-rate personas.
    persona_dtype = pl.Enum(list(PERSONA_VALUES))
    gated = joined.with_columns(
        pl.when(pl.col("closed_count") > 0)
        .then(pl.col("top_persona_converted"))
        .otherwise(pl.lit(None, dtype=persona_dtype))
        .alias("top_persona_converted"),
        pl.col("outcome_mix").fill_null(pl.lit([], dtype=OUTCOME_MIX_STRUCT)),
    )

    return gated.select(list(GOLD_AGENT_PERFORMANCE_SCHEMA.names()))


# ---------------------------------------------------------------------------
# Internal helpers.
# ---------------------------------------------------------------------------


def _aggregate_per_conversation(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    """One row per (agent_id, conversation_id) carrying the
    chronologically-last non-null outcome and the lead_id."""
    return silver_lf.group_by(
        ["agent_id", "conversation_id"], maintain_order=False
    ).agg(
        pl.col("conversation_outcome")
        .sort_by("timestamp")
        .drop_nulls()
        .last()
        .alias("outcome"),
        pl.col("lead_id").first().alias("lead_id"),
    )


def _avg_outbound_response(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Mean of agent-side ``metadata.response_time_sec`` over the
    agent's outbound rows. Null when the agent has no outbound row
    with a known response-time value."""
    outbound = silver_lf.filter(pl.col("direction") == "outbound").with_columns(
        pl.col("metadata")
        .struct.field("response_time_sec")
        .alias("_response_time_sec")
    )
    return outbound.group_by("agent_id", maintain_order=False).agg(
        pl.col("_response_time_sec")
        .drop_nulls()
        .mean()
        .cast(pl.Float64)
        .alias("avg_response_time_sec")
    )


def _outcome_mix(per_conv: pl.LazyFrame) -> pl.LazyFrame:
    """``List[Struct{outcome, count}]`` per agent — every observed
    outcome value gets its own struct entry, including labels Bronze
    may add in the future (D8). Null outcomes are excluded."""
    counts = (
        per_conv.filter(pl.col("outcome").is_not_null())
        .group_by(["agent_id", "outcome"], maintain_order=False)
        .agg(pl.len().cast(pl.Int32).alias("count"))
        .sort(["agent_id", "outcome"])
    )
    return counts.group_by("agent_id", maintain_order=False).agg(
        pl.struct(["outcome", "count"]).alias("outcome_mix")
    )


def _top_persona_converted(
    per_conv: pl.LazyFrame,
    *,
    lead_personas: pl.LazyFrame | None,
) -> pl.LazyFrame:
    """Per agent, the persona with the highest ``close_rate`` among
    that agent's leads; ties broken by absolute ``closed_count``;
    final tie-break: persona name **lexical ascending** (NOT
    ``PERSONA_VALUES`` declaration order). Lexical was chosen because
    it is reproducible without coupling to the enum's category order
    — a future enum reorder would otherwise silently flip the
    winner."""
    persona_dtype = pl.Enum(list(PERSONA_VALUES))
    if lead_personas is None:
        return per_conv.group_by("agent_id", maintain_order=False).agg(
            pl.lit(None, dtype=persona_dtype).alias("top_persona_converted")
        )

    with_persona = per_conv.join(lead_personas, on="lead_id", how="left").filter(
        pl.col("persona").is_not_null()
    )

    per_persona = (
        with_persona.group_by(
            ["agent_id", "persona"], maintain_order=False
        )
        .agg(
            pl.len().cast(pl.Int32).alias("_n"),
            pl.col("outcome")
            .eq("venda_fechada")
            .fill_null(value=False)
            .sum()
            .cast(pl.Int32)
            .alias("_closed"),
        )
        .with_columns(
            (
                pl.col("_closed").cast(pl.Float64)
                / pl.col("_n").cast(pl.Float64)
            ).alias("_pp_rate")
        )
    )

    ranked = per_persona.sort(
        ["agent_id", "_pp_rate", "_closed", "persona"],
        descending=[False, True, True, False],
    )
    return ranked.group_by("agent_id", maintain_order=False).agg(
        pl.col("persona").first().cast(persona_dtype).alias("top_persona_converted")
    )
