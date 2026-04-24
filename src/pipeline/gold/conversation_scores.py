"""Build :data:`pipeline.schemas.gold.GOLD_CONVERSATION_SCORES_SCHEMA`
from a Silver :class:`polars.LazyFrame`.

Pure-Polars: no I/O, no LLM, no Python loops over rows. The transform
runs as one ``sort -> with_columns -> group_by -> agg -> select`` pass
so Polars can fuse it into a single physical execution. The CLI layer
(``pipeline.cli.gold``) owns the read / write.

Spec drivers
------------

- F3-RF-05 — column list + ``avg_lead_response_sec`` pairwise definition.
- F3-RF-04 — schema-drift validator runs after collect.
- D8 (only matters for the ``agent_performance`` builder, not here) —
  ``outcome_mix`` shape; ``conversation_outcome`` here is the ``last
  non-null`` over the conversation's rows.
"""

from __future__ import annotations

import polars as pl

from pipeline.schemas.gold import GOLD_CONVERSATION_SCORES_SCHEMA

__all__ = ["build_conversation_scores"]


def build_conversation_scores(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregate a Silver ``LazyFrame`` into one row per
    ``conversation_id``.

    Returns a ``LazyFrame`` whose schema matches
    :data:`GOLD_CONVERSATION_SCORES_SCHEMA` column-for-column. The
    callsite is responsible for ``collect`` and the schema assertion.
    """
    paired = _add_pair_deltas(silver_lf)

    is_inbound = pl.col("direction") == "inbound"
    is_outbound = pl.col("direction") == "outbound"
    # LEARN: ``is_business_hours`` is a struct field — extract it
    # before comparison so the boolean aggregation has a flat column
    # to walk. ``.eq(False)`` keeps ruff's explicit-comparison lint
    # happy. Three-valued logic: a ``null`` business-hours flag stays
    # ``null`` under ``.eq(False)`` and does NOT contribute to the
    # off-hours count — the ``fill_null(0)`` below converts an
    # all-null group's ``.sum()`` result from ``null`` back to ``0``
    # so the column's count semantics hold.
    is_off_hours = (
        pl.col("metadata").struct.field("is_business_hours").eq(False)
    )

    aggregated = paired.group_by("conversation_id", maintain_order=False).agg(
        pl.col("lead_id").first().alias("lead_id"),
        pl.col("campaign_id").first().alias("campaign_id"),
        pl.col("agent_id").first().alias("agent_id"),
        is_inbound.sum().cast(pl.Int32).alias("msgs_inbound"),
        is_outbound.sum().cast(pl.Int32).alias("msgs_outbound"),
        pl.col("timestamp").min().alias("first_message_at"),
        pl.col("timestamp").max().alias("last_message_at"),
        (
            (pl.col("timestamp").max() - pl.col("timestamp").min())
            .dt.total_seconds()
            .cast(pl.Int64)
        ).alias("duration_sec"),
        pl.col("_pair_delta_sec")
        .drop_nulls()
        .mean()
        .cast(pl.Float64)
        .alias("avg_lead_response_sec"),
        # LEARN: ``time_to_first_response_sec`` is the FIRST pair
        # delta in chronological order. The source frame is sorted by
        # ``(conversation_id, timestamp)`` upstream, so
        # ``drop_nulls().first()`` honours that order.
        pl.col("_pair_delta_sec")
        .drop_nulls()
        .first()
        .cast(pl.Float64)
        .alias("time_to_first_response_sec"),
        is_off_hours.sum().fill_null(0).cast(pl.Int32).alias("off_hours_msgs"),
        pl.col("concorrente_mencionado")
        .is_not_null()
        .any()
        .alias("mencionou_concorrente"),
        pl.col("concorrente_mencionado")
        .drop_nulls()
        .mode()
        .first()
        .alias("concorrente_citado"),
        pl.col("veiculo_marca")
        .drop_nulls()
        .mode()
        .first()
        .alias("veiculo_marca"),
        pl.col("veiculo_modelo")
        .drop_nulls()
        .mode()
        .first()
        .alias("veiculo_modelo"),
        pl.col("veiculo_ano")
        .drop_nulls()
        .mode()
        .first()
        .alias("veiculo_ano"),
        pl.col("valor_pago_atual_brl").max().alias("valor_pago_atual_brl"),
        _kleene_sinistro_expr().alias("sinistro_historico"),
        # LEARN: sort_by inside the agg makes the chronological
        # intent explicit so this stays correct even if a future
        # change reorders rows above the ``group_by``.
        pl.col("conversation_outcome")
        .sort_by("timestamp")
        .drop_nulls()
        .last()
        .alias("conversation_outcome"),
    )

    # LEARN: pin column order to the declared Gold schema so the
    # parquet writer hits the contract regardless of the
    # ``group_by().agg(...)`` evaluation order.
    return aggregated.select(list(GOLD_CONVERSATION_SCORES_SCHEMA.names()))


def _add_pair_deltas(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Annotate each row with the seconds elapsed since the previous
    outbound message in the same conversation, when this row is the
    first inbound after that outbound. ``null`` otherwise.

    The "first inbound after each outbound" rule (spec F3-RF-05) is
    encoded as: this row is inbound AND the row immediately preceding
    it (in chronological order within the conversation) is outbound.
    Subsequent inbound replies before the next outbound contribute
    nothing — they belong to the same lead-response burst, not to a
    new round-trip.
    """
    sorted_lf = silver_lf.sort(["conversation_id", "timestamp"])
    with_prev = sorted_lf.with_columns(
        pl.col("direction").shift(1).over("conversation_id").alias("_prev_dir"),
        pl.col("timestamp").shift(1).over("conversation_id").alias("_prev_ts"),
    )
    delta = (
        pl.when(
            (pl.col("direction") == "inbound")
            & (pl.col("_prev_dir") == "outbound")
        )
        .then(
            (pl.col("timestamp") - pl.col("_prev_ts"))
            .dt.total_seconds()
            .cast(pl.Float64)
        )
        .otherwise(None)
        .alias("_pair_delta_sec")
    )
    return with_prev.with_columns(delta)


def _kleene_sinistro_expr() -> pl.Expr:
    """Three-valued aggregation for ``sinistro_historico``.

    PRD §6.3 wants ``true`` if the lead ever confirmed an accident,
    ``false`` if they ever explicitly denied one, ``null`` when the
    LLM never extracted a value across the conversation. This mirrors
    SQL Kleene-style boolean OR: a single ``true`` dominates; without
    one, a single ``false`` dominates; otherwise null.
    """
    return (
        pl.when(pl.col("sinistro_historico").eq(True).any())
        .then(pl.lit(True))
        .when(pl.col("sinistro_historico").eq(False).any())
        .then(pl.lit(False))
        .otherwise(pl.lit(None, dtype=pl.Boolean))
    )
