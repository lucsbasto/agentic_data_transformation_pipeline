"""Build :data:`GOLD_LEAD_PROFILE_SCHEMA` skeleton from a Silver
LazyFrame.

"Skeleton" because four columns land as typed null placeholders:

- ``persona`` / ``persona_confidence`` — filled by the F3.10 persona
  classifier.
- ``price_sensitivity`` — filled by F3.11 once the haggling-phrase
  count is computed alongside the intent-score components.
- ``intent_score`` — filled by F3.11.

Keeping them as typed nulls means :data:`GOLD_LEAD_PROFILE_SCHEMA`
already matches at collect time, so the schema-drift validator works
the same before and after enrichment.

Spec drivers
------------
- F3-RF-06 — column list.
- Design §5.1 — engagement_profile bucketing (first match wins),
  dominant-email/state/city dominance.
- D12 — engagement uses ``batch_latest_timestamp - 48h`` rather than
  wall-clock ``now()`` so re-running the same Silver yields
  byte-identical Gold.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

import polars as pl

from pipeline.schemas.gold import (
    ENGAGEMENT_PROFILE_VALUES,
    GOLD_LEAD_PROFILE_SCHEMA,
    PERSONA_VALUES,
    PRICE_SENSITIVITY_VALUES,
    SENTIMENT_VALUES,
)

__all__ = ["build_lead_profile_skeleton"]


# LEARN: engagement-bucket cut-points pulled out as named ``Final``
# constants so ruff PLR2004 doesn't flag the literals and the next
# reader sees the rule shape at a glance. ``Final`` blocks accidental
# rebinding (mypy-enforced); the values define the analytics
# contract — changing them needs a spec update, not a code edit.
_HOT_CLOSE_RATE_THRESHOLD: Final[float] = 0.5
_HOT_CONVERSATIONS_THRESHOLD: Final[int] = 3
_COLD_STALENESS_HOURS: Final[int] = 48


def build_lead_profile_skeleton(
    silver_lf: pl.LazyFrame,
    *,
    batch_latest_timestamp: datetime,
) -> pl.LazyFrame:
    """Aggregate Silver into one row per ``lead_id`` with persona /
    intent_score / price_sensitivity placeholders.

    ``batch_latest_timestamp`` (max ``timestamp`` over the Silver
    frame) anchors the engagement-profile staleness check so the
    output is deterministic with respect to the Silver inputs alone
    — see D12.
    """
    per_conv = _aggregate_per_conversation(silver_lf)
    per_lead = _aggregate_per_lead(silver_lf, per_conv)
    enriched = _add_engagement_profile(
        per_lead, batch_latest_timestamp=batch_latest_timestamp
    )
    with_placeholders = _add_persona_and_score_placeholders(enriched)
    return with_placeholders.select(list(GOLD_LEAD_PROFILE_SCHEMA.names()))


# ---------------------------------------------------------------------------
# Per-conversation aggregation (re-used to compute close_rate and the
# per-lead first/last_seen_at).
# ---------------------------------------------------------------------------


def _aggregate_per_conversation(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    return silver_lf.group_by(
        ["lead_id", "conversation_id"], maintain_order=False
    ).agg(
        pl.col("conversation_outcome")
        .sort_by("timestamp")
        .drop_nulls()
        .last()
        .alias("outcome")
    )


def _aggregate_per_lead(
    silver_lf: pl.LazyFrame, per_conv: pl.LazyFrame
) -> pl.LazyFrame:
    """Compose the conversation-level + row-level aggregations into
    one row per ``lead_id``."""
    conv_aggs = per_conv.group_by("lead_id", maintain_order=False).agg(
        pl.len().cast(pl.Int32).alias("conversations"),
        pl.col("outcome")
        .eq("venda_fechada")
        .fill_null(value=False)
        .sum()
        .cast(pl.Int32)
        .alias("closed_count"),
    )

    row_aggs = silver_lf.group_by("lead_id", maintain_order=False).agg(
        pl.col("sender_name_normalized")
        .drop_nulls()
        .mode()
        .first()
        .alias("sender_name_normalized"),
        pl.col("email_domain")
        .drop_nulls()
        .mode()
        .first()
        .alias("dominant_email_domain"),
        pl.col("metadata")
        .struct.field("state")
        .drop_nulls()
        .mode()
        .first()
        .alias("dominant_state"),
        pl.col("metadata")
        .struct.field("city")
        .drop_nulls()
        .mode()
        .first()
        .alias("dominant_city"),
        pl.col("timestamp").min().alias("first_seen_at"),
        pl.col("timestamp").max().alias("last_seen_at"),
    )

    return conv_aggs.join(row_aggs, on="lead_id", how="inner").with_columns(
        (
            pl.col("closed_count").cast(pl.Float64)
            / pl.col("conversations").cast(pl.Float64)
        ).alias("close_rate")
    )


# ---------------------------------------------------------------------------
# Engagement profile bucket (D12 staleness anchored to batch latest).
# ---------------------------------------------------------------------------


def _add_engagement_profile(
    per_lead: pl.LazyFrame, *, batch_latest_timestamp: datetime
) -> pl.LazyFrame:
    """First-match-wins bucket: hot > cold > warm.

    Boundaries (locked by tests in
    ``tests/unit/test_gold_lead_profile.py``):

    - ``conversations=2, close_rate=0.5`` ⇒ hot (close_rate ≥ 0.5).
    - ``conversations=1, close_rate=1.0`` ⇒ hot (close_rate ≥ 0.5).
    - ``conversations=1, closed_count=0, fresh`` ⇒ warm.
    - ``conversations=1, closed_count=0, stale (>48h)`` ⇒ cold.
    """
    cold_cutoff = batch_latest_timestamp - timedelta(hours=_COLD_STALENESS_HOURS)
    engagement_dtype = pl.Enum(list(ENGAGEMENT_PROFILE_VALUES))
    # LEARN: clause 2 (``conversations >= 3 AND close_rate > 0``) is
    # NOT redundant with clause 1. It catches "engaged but mediocre
    # closer" leads — e.g. 3 conversations with 1 close (rate ~0.33)
    # would fall to ``warm`` under clause 1 alone but are flagged
    # ``hot`` here because volume + any close together signal
    # interest worth the agent's time. Design §5.1 lists both rules
    # as a deliberate two-pronged definition.
    return per_lead.with_columns(
        pl.when(
            (pl.col("close_rate") >= _HOT_CLOSE_RATE_THRESHOLD)
            | (
                (pl.col("conversations") >= _HOT_CONVERSATIONS_THRESHOLD)
                & (pl.col("close_rate") > 0)
            )
        )
        .then(pl.lit("hot"))
        .when(
            (pl.col("conversations") == 1)
            & (pl.col("closed_count") == 0)
            & (pl.col("last_seen_at") < cold_cutoff)
        )
        .then(pl.lit("cold"))
        .otherwise(pl.lit("warm"))
        .cast(engagement_dtype)
        .alias("engagement_profile")
    )


# ---------------------------------------------------------------------------
# Typed null placeholders for the columns F3.10 / F3.11 fill.
# ---------------------------------------------------------------------------


def _add_persona_and_score_placeholders(per_lead: pl.LazyFrame) -> pl.LazyFrame:
    """Materialize the classifier-filled columns as typed nulls so
    :data:`GOLD_LEAD_PROFILE_SCHEMA` matches at collect time.

    Columns: ``persona`` / ``persona_confidence`` (F3.10),
    ``sentiment`` / ``sentiment_confidence`` (F5),
    ``price_sensitivity`` / ``intent_score`` (F3.11). Until the
    enrichment pass runs, downstream consumers see ``null`` and can
    distinguish "skipped" from "zero".
    """
    persona_dtype = pl.Enum(list(PERSONA_VALUES))
    sentiment_dtype = pl.Enum(list(SENTIMENT_VALUES))
    price_dtype = pl.Enum(list(PRICE_SENSITIVITY_VALUES))
    return per_lead.with_columns(
        pl.lit(None, dtype=persona_dtype).alias("persona"),
        pl.lit(None, dtype=pl.Float64).alias("persona_confidence"),
        pl.lit(None, dtype=sentiment_dtype).alias("sentiment"),
        pl.lit(None, dtype=pl.Float64).alias("sentiment_confidence"),
        pl.lit(None, dtype=price_dtype).alias("price_sensitivity"),
        pl.lit(None, dtype=pl.Int32).alias("intent_score"),
    )
