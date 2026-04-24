"""Build :data:`GOLD_COMPETITOR_INTEL_SCHEMA` from a Silver LazyFrame.

One row per normalized competitor name (lower-cased + trimmed), so
``"Porto Seguro"`` and ``"porto seguro"`` collapse to a single row
``competitor='porto seguro'``. Pure Polars; no LLM.

Spec drivers
------------
- F3-RF-08 — column list.
- D7 — grain / normalization rule.
- Design §7.2 — loss_rate computed on distinct conversations, mention_count
  on raw rows, top_states from ``metadata.state``.
"""

from __future__ import annotations

import polars as pl

from pipeline.schemas.gold import GOLD_COMPETITOR_INTEL_SCHEMA

__all__ = ["build_competitor_intel"]

_TOP_STATES_LIMIT = 3


def build_competitor_intel(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregate competitor mentions from Silver.

    Rows with a null ``concorrente_mencionado`` are dropped before
    grouping — every output row represents at least one mention.
    """
    mentions = silver_lf.filter(
        pl.col("concorrente_mencionado").is_not_null()
    ).with_columns(
        pl.col("concorrente_mencionado")
        .str.strip_chars()
        .str.to_lowercase()
        .alias("competitor")
    )

    mention_aggs = mentions.group_by("competitor", maintain_order=False).agg(
        pl.len().cast(pl.Int32).alias("mention_count"),
        pl.col("valor_pago_atual_brl")
        .drop_nulls()
        .mean()
        .cast(pl.Float64)
        .alias("avg_quoted_price_brl"),
    )

    loss_rate_lf = _compute_loss_rate(mentions)
    top_states_lf = _compute_top_states(mentions)

    joined = mention_aggs.join(loss_rate_lf, on="competitor", how="left").join(
        top_states_lf, on="competitor", how="left"
    )

    # LEARN: a competitor with no known state lands with
    # ``top_states = null`` after the left-join. The schema
    # declares ``List[String]``; coerce null to an empty list so
    # readers never have to null-check before iterating.
    filled = joined.with_columns(
        pl.col("top_states").fill_null(pl.lit([], dtype=pl.List(pl.String)))
    )

    return filled.select(list(GOLD_COMPETITOR_INTEL_SCHEMA.names()))


def _compute_loss_rate(mentions: pl.LazyFrame) -> pl.LazyFrame:
    """``loss_rate`` computed on DISTINCT conversations — one lead
    mentioning the same competitor ten times counts once.

    Null when no conversation under this competitor has a known
    outcome.
    """
    per_conversation = mentions.group_by(
        ["competitor", "conversation_id"], maintain_order=False
    ).agg(
        pl.col("conversation_outcome")
        .sort_by("timestamp")
        .drop_nulls()
        .last()
        .alias("outcome")
    )

    known_outcomes = per_conversation.filter(pl.col("outcome").is_not_null())

    return known_outcomes.group_by("competitor", maintain_order=False).agg(
        # LEARN: ``.fill_null(False)`` is defensive — today every row
        # reaching this point has a non-null outcome (upstream filter),
        # but a future refactor that removes the filter would otherwise
        # silently undercount the numerator because ``.sum()`` skips
        # nulls.
        (
            pl.col("outcome")
            .ne("venda_fechada")
            .fill_null(value=False)
            .sum()
            .cast(pl.Float64)
            / pl.len().cast(pl.Float64)
        ).alias("loss_rate"),
    )


def _compute_top_states(mentions: pl.LazyFrame) -> pl.LazyFrame:
    """Top ``_TOP_STATES_LIMIT`` states by mention volume per
    competitor. States with a null value are excluded."""
    states = mentions.with_columns(
        pl.col("metadata").struct.field("state").alias("state")
    ).filter(pl.col("state").is_not_null())

    # LEARN: secondary ascending sort on ``state`` breaks volume ties
    # deterministically (``BA`` before ``MG`` on equal counts). Without
    # it, ties propagate the group_by insertion order, which is not a
    # stable Polars contract and would make Gold artifacts diverge
    # between runs over the same Silver.
    ranked = (
        states.group_by(["competitor", "state"], maintain_order=False)
        .agg(pl.len().alias("state_volume"))
        .sort(
            ["competitor", "state_volume", "state"],
            descending=[False, True, False],
        )
    )

    return ranked.group_by("competitor", maintain_order=False).agg(
        pl.col("state").head(_TOP_STATES_LIMIT).alias("top_states")
    )
