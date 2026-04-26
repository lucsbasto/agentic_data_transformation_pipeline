"""F3.14 Gold orchestrator — compose the four table builders, run the
persona lane, compute intent_score, and persist every output.

This module is the in-memory pipeline. The CLI layer
(:mod:`pipeline.cli.gold`, F3.15) owns the Silver read, the manifest
contract, and stdout. The orchestrator is therefore I/O-pure on the
input side (it accepts a Silver :class:`polars.LazyFrame`) and writes
its outputs through the F3.13 atomic writers.

Spec drivers
------------
- ``F3-RF-03`` — four parquet partitions + one insights JSON per batch.
- ``F3-RF-09`` / ``F3-RF-10`` — persona lane and intent-score wiring
  on the lead-profile frame.
- ``F3-RF-13`` — ``rows_in`` / ``rows_out`` reporting for the manifest.
- Design §2 — data flow diagram this orchestrator traces.
- Design §9.6 — synchronous entry point that bridges into the async
  persona concurrency lane.

The persona classifier is dependency-injected so unit tests can run
without spinning up the real LLM thread-pool. Production callers leave
``persona_classifier=None`` and the orchestrator builds the default
implementation on top of :func:`pipeline.gold.concurrency.classify_all`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import polars as pl

from pipeline.errors import PipelineError
from pipeline.gold._regex import (
    CLOSING_PHRASES,
    EVASIVE_PHRASES,
    HAGGLING_PHRASES,
    TECHNICAL_QUESTION_PHRASES,
    count_phrase_hits,
)
from pipeline.gold.agent_performance import build_agent_performance
from pipeline.gold.competitor_intel import build_competitor_intel
from pipeline.gold.conversation_scores import build_conversation_scores
from pipeline.gold.insights import build_insights
from pipeline.gold.intent_score import compute_intent_score
from pipeline.gold.lead_profile import build_lead_profile_skeleton
from pipeline.gold.persona import (
    LeadAggregate,
    PersonaResult,
    aggregate_leads,
)
from pipeline.gold.writer import (
    write_gold_agent_performance,
    write_gold_competitor_intel,
    write_gold_conversation_scores,
    write_gold_insights,
    write_gold_lead_profile,
)
from pipeline.logging import get_logger
from pipeline.schemas.gold import (
    PERSONA_VALUES,
    PRICE_SENSITIVITY_VALUES,
    SENTIMENT_VALUES,
)
from pipeline.settings import Settings

__all__ = [
    "GoldTransformResult",
    "PersonaClassifier",
    "transform_gold",
]


PersonaClassifier = Callable[[list[LeadAggregate], datetime], dict[str, PersonaResult]]
"""Per-batch persona classifier. Receives the aggregated leads and the
``batch_latest_timestamp`` anchor (R1 staleness) and must return a
``lead_id -> PersonaResult`` map covering every input aggregate.

Tests inject a synchronous fake; production wires
:func:`pipeline.gold.concurrency.classify_all`.
"""


_LOG = get_logger(__name__)

# LEARN: ``classify_all`` runs the persona LLM lane on threads it owns.
# When the orchestrator is called from a sync context (CLI) we drive
# the coroutine via ``asyncio.run``; when it is called from an already
# running loop we would need a different bridge. The CLI is sync so
# the simple ``asyncio.run`` is enough for F3.14.
_DEFAULT_BATCH_ANCHOR: Final[datetime] = datetime(1970, 1, 1, tzinfo=UTC)

_PRICE_SENSITIVITY_MEDIUM_THRESHOLD: Final[int] = 1
_PRICE_SENSITIVITY_HIGH_THRESHOLD: Final[int] = 3


@dataclass(frozen=True, slots=True)
class GoldTransformResult:
    """Outcome of one Gold transform run.

    Carries every path the CLI needs for log + manifest plus the row
    counts required by ``F3-RF-13`` (``rows_out`` is the sum of the
    four parquet table heights; insights JSON is not counted).
    """

    batch_id: str
    conversation_scores_path: Path
    lead_profile_path: Path
    agent_performance_path: Path
    competitor_intel_path: Path
    insights_path: Path
    conversation_scores_rows: int
    lead_profile_rows: int
    agent_performance_rows: int
    competitor_intel_rows: int

    @property
    def rows_out(self) -> int:
        return (
            self.conversation_scores_rows
            + self.lead_profile_rows
            + self.agent_performance_rows
            + self.competitor_intel_rows
        )


def transform_gold(
    silver_lf: pl.LazyFrame,
    *,
    batch_id: str,
    gold_root: Path,
    persona_classifier: PersonaClassifier | None = None,
    settings: Settings | None = None,
) -> GoldTransformResult:
    """Run the Gold lane end-to-end for one Silver batch.

    The four pure-Polars tables are built lazily, then collected at
    the writer leaf. The persona lane runs on the materialised
    aggregate list because the LLM call sites are Python-level. After
    persona / intent_score fill the lead profile, ``agent_performance``
    is rebuilt with the populated ``lead_personas`` frame so
    ``top_persona_converted`` reflects the LLM output (design §7.1).
    """
    _LOG.info("gold.start", batch_id=batch_id)

    silver_lf = silver_lf.cache()

    batch_latest = _batch_latest_timestamp(silver_lf)

    conversation_scores_lf = build_conversation_scores(silver_lf)
    competitor_intel_lf = build_competitor_intel(silver_lf)
    lead_profile_skeleton_lf = build_lead_profile_skeleton(
        silver_lf, batch_latest_timestamp=batch_latest
    )

    aggregates = aggregate_leads(silver_lf)
    classifier = persona_classifier or _default_persona_classifier(settings)
    persona_results = classifier(aggregates, batch_latest)
    _LOG.info(
        "gold.persona.done",
        batch_id=batch_id,
        leads=len(aggregates),
    )
    _LOG.info(
        "gold.sentiment.batch_complete",
        batch_id=batch_id,
        leads=len(aggregates),
        **_sentiment_telemetry(persona_results),
    )

    intent_inputs_lf = _compute_intent_score_inputs(silver_lf, conversation_scores_lf)
    lead_profile_df = _apply_persona_and_intent_score(
        lead_profile_skeleton_lf, persona_results, intent_inputs_lf
    ).collect()
    _LOG.info(
        "gold.lead_profile.done",
        batch_id=batch_id,
        rows=lead_profile_df.height,
        persona_distribution=_persona_distribution(lead_profile_df),
    )

    lead_personas_lf = lead_profile_df.lazy().select(["lead_id", "persona"])
    agent_performance_lf = build_agent_performance(silver_lf, lead_personas=lead_personas_lf)

    conversation_scores_df = conversation_scores_lf.collect()
    _LOG.info(
        "gold.conversation_scores.done",
        batch_id=batch_id,
        rows=conversation_scores_df.height,
    )
    agent_performance_df = agent_performance_lf.collect()
    _LOG.info(
        "gold.agent_performance.done",
        batch_id=batch_id,
        rows=agent_performance_df.height,
    )
    competitor_intel_df = competitor_intel_lf.collect()
    _LOG.info(
        "gold.competitor_intel.done",
        batch_id=batch_id,
        rows=competitor_intel_df.height,
    )

    insights_payload = build_insights(silver_lf, lead_profile_df.lazy())
    insights_payload = {
        "generated_at": batch_latest.isoformat(),
        "batch_id": batch_id,
        **insights_payload,
    }
    _LOG.info("gold.insights.done", batch_id=batch_id)

    cs_result = write_gold_conversation_scores(
        conversation_scores_df, gold_root=gold_root, batch_id=batch_id
    )
    lp_result = write_gold_lead_profile(lead_profile_df, gold_root=gold_root, batch_id=batch_id)
    ap_result = write_gold_agent_performance(
        agent_performance_df, gold_root=gold_root, batch_id=batch_id
    )
    ci_result = write_gold_competitor_intel(
        competitor_intel_df, gold_root=gold_root, batch_id=batch_id
    )
    insights_result = write_gold_insights(insights_payload, gold_root=gold_root, batch_id=batch_id)

    result = GoldTransformResult(
        batch_id=batch_id,
        conversation_scores_path=cs_result.gold_path,
        lead_profile_path=lp_result.gold_path,
        agent_performance_path=ap_result.gold_path,
        competitor_intel_path=ci_result.gold_path,
        insights_path=insights_result.insights_path,
        conversation_scores_rows=cs_result.rows_written,
        lead_profile_rows=lp_result.rows_written,
        agent_performance_rows=ap_result.rows_written,
        competitor_intel_rows=ci_result.rows_written,
    )
    _LOG.info(
        "gold.complete",
        batch_id=batch_id,
        rows_out=result.rows_out,
    )
    return result


def _batch_latest_timestamp(silver_lf: pl.LazyFrame) -> datetime:
    """Compute ``max(timestamp)`` across the Silver frame.

    Used as the R1 staleness anchor (design §5.3 / D12). Empty Silver
    falls back to the Unix epoch so the persona rule engine still has
    a comparable datetime — every lead also has its own
    ``last_message_at`` so the rule simply never fires on the empty
    case anyway.
    """
    row = silver_lf.select(pl.col("timestamp").max().alias("ts")).collect()
    value = row["ts"][0] if row.height else None
    if value is None:
        return _DEFAULT_BATCH_ANCHOR
    if isinstance(value, datetime):
        return value
    raise PipelineError(f"unexpected timestamp type from Silver scan: {type(value).__name__}")


def _apply_persona_and_intent_score(
    skeleton_lf: pl.LazyFrame,
    persona_results: dict[str, PersonaResult],
    intent_inputs_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Replace the F3.7 typed-null persona / persona_confidence /
    intent_score placeholders with the persona lane output and the
    F3.11 intent-score expression.

    ``intent_inputs_lf`` carries the 9 per-lead input columns
    declared in :data:`pipeline.gold.intent_score.INPUT_COLUMNS`
    minus ``persona`` (joined here from ``persona_results``).
    """
    persona_dtype = pl.Enum(list(PERSONA_VALUES))
    sentiment_dtype = pl.Enum(list(SENTIMENT_VALUES))
    persona_rows = [
        {
            "lead_id": lead_id,
            "persona_filled": result.persona,
            "persona_confidence_filled": result.persona_confidence,
            "sentiment_filled": result.sentiment,
            "sentiment_confidence_filled": result.sentiment_confidence,
        }
        for lead_id, result in persona_results.items()
    ]
    persona_df = pl.DataFrame(
        persona_rows,
        schema={
            "lead_id": pl.String(),
            "persona_filled": persona_dtype,
            "persona_confidence_filled": pl.Float64(),
            "sentiment_filled": sentiment_dtype,
            "sentiment_confidence_filled": pl.Float64(),
        },
    )

    joined = skeleton_lf.join(persona_df.lazy(), on="lead_id", how="left").join(
        intent_inputs_lf, on="lead_id", how="left"
    )
    price_dtype = pl.Enum(list(PRICE_SENSITIVITY_VALUES))
    hits = pl.col("haggling_phrase_hits").fill_null(0)
    price_sensitivity_expr = (
        pl.when(hits >= _PRICE_SENSITIVITY_HIGH_THRESHOLD)
        .then(pl.lit("high"))
        .when(hits >= _PRICE_SENSITIVITY_MEDIUM_THRESHOLD)
        .then(pl.lit("medium"))
        .otherwise(pl.lit("low"))
        .cast(price_dtype)
    )
    filled = joined.with_columns(
        pl.col("persona_filled").alias("persona"),
        pl.col("persona_confidence_filled").alias("persona_confidence"),
        pl.col("sentiment_filled").alias("sentiment"),
        pl.col("sentiment_confidence_filled").alias("sentiment_confidence"),
        price_sensitivity_expr.alias("price_sensitivity"),
    ).drop(
        [
            "persona_filled",
            "persona_confidence_filled",
            "sentiment_filled",
            "sentiment_confidence_filled",
        ]
    )

    scored = compute_intent_score(filled)
    # Drop the helper input columns and reorder so the output matches
    # GOLD_LEAD_PROFILE_SCHEMA position-for-position. F5: sentiment +
    # sentiment_confidence appended between persona_confidence and
    # price_sensitivity to match _LEAD_PROFILE_FIELDS order.
    return scored.select(
        [
            "lead_id",
            "conversations",
            "closed_count",
            "close_rate",
            "sender_name_normalized",
            "dominant_email_domain",
            "dominant_state",
            "dominant_city",
            "engagement_profile",
            "persona",
            "persona_confidence",
            "sentiment",
            "sentiment_confidence",
            "price_sensitivity",
            "intent_score",
            "first_seen_at",
            "last_seen_at",
        ]
    )


def _compute_intent_score_inputs(
    silver_lf: pl.LazyFrame, conversation_scores_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Aggregate Silver into the per-lead columns that
    :func:`pipeline.gold.intent_score.compute_intent_score` consumes.

    The eight non-persona columns from
    :data:`pipeline.gold.intent_score.INPUT_COLUMNS` are derived here;
    ``persona`` is joined separately from the LLM lane output. The
    pre-computed ``conversation_scores_lf`` is threaded through to avoid
    recomputing :func:`build_conversation_scores` on the same Silver
    frame (Polars ``.cache()`` only memoises the scan, not downstream
    aggregation).
    """
    inbound_text_per_lead = (
        silver_lf.filter(pl.col("direction") == "inbound")
        .group_by("lead_id", maintain_order=False)
        .agg(
            pl.col("message_body_masked")
            .drop_nulls()
            .str.join(delimiter=" ")
            .alias("_inbound_text"),
        )
    )

    per_lead = silver_lf.group_by("lead_id", maintain_order=False).agg(
        (
            pl.col("has_cpf").any()
            | pl.col("email_domain").is_not_null().any()
            | pl.col("has_phone_mention").any()
        )
        .fill_null(value=False)
        .alias("forneceu_dado_pessoal"),
        (pl.col("direction") == "inbound").sum().cast(pl.Int32).alias("num_msgs_inbound"),
        (pl.col("metadata").struct.field("is_business_hours") == False)  # noqa: E712
        .sum()
        .cast(pl.Int32)
        .alias("off_hours_msgs"),
        pl.col("conversation_outcome").sort_by("timestamp").drop_nulls().last().alias("outcome"),
    )

    avg_lead_response_per_lead = _avg_lead_response_per_lead(conversation_scores_lf)

    phrase_inputs = inbound_text_per_lead.with_columns(
        pl.col("_inbound_text")
        .map_elements(
            lambda body: count_phrase_hits(body, CLOSING_PHRASES),
            return_dtype=pl.Int32,
        )
        .alias("closing_phrase_hits"),
        pl.col("_inbound_text")
        .map_elements(
            lambda body: count_phrase_hits(body, TECHNICAL_QUESTION_PHRASES),
            return_dtype=pl.Int32,
        )
        .alias("technical_question_hits"),
        pl.col("_inbound_text")
        .map_elements(
            lambda body: count_phrase_hits(body, EVASIVE_PHRASES),
            return_dtype=pl.Int32,
        )
        .alias("evasive_phrase_hits"),
        pl.col("_inbound_text")
        .map_elements(
            lambda body: count_phrase_hits(body, HAGGLING_PHRASES),
            return_dtype=pl.Int32,
        )
        .alias("haggling_phrase_hits"),
    ).drop("_inbound_text")

    return (
        per_lead.join(avg_lead_response_per_lead, on="lead_id", how="left")
        .join(phrase_inputs, on="lead_id", how="left")
        .with_columns(
            pl.col("closing_phrase_hits").fill_null(0).cast(pl.Int32),
            pl.col("technical_question_hits").fill_null(0).cast(pl.Int32),
            pl.col("evasive_phrase_hits").fill_null(0).cast(pl.Int32),
            pl.col("haggling_phrase_hits").fill_null(0).cast(pl.Int32),
        )
    )


def _avg_lead_response_per_lead(conversation_scores_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Per-lead mean of the conversation-level
    ``avg_lead_response_sec`` (F3-RF-05). Consumes the already-built
    ``conversation_scores_lf`` (computed once by
    :func:`transform_gold`) — calling :func:`build_conversation_scores`
    again here re-walked the pairwise sequence on every Gold run."""
    return conversation_scores_lf.group_by("lead_id", maintain_order=False).agg(
        pl.col("avg_lead_response_sec").mean().alias("avg_lead_response_sec")
    )


def _sentiment_telemetry(
    persona_results: dict[str, PersonaResult],
) -> dict[str, object]:
    """Per-batch sentiment counters for observability.

    Reports both the source mix (rule / llm / llm_fallback / skipped)
    and the per-label histogram. The label histogram drives the
    F5 calibration gate (e.g. ``misto`` ≤ 25% post-deploy).
    """
    source_counts: dict[str, int] = {}
    label_counts: dict[str, int] = {}
    for result in persona_results.values():
        source_counts[result.sentiment_source] = (
            source_counts.get(result.sentiment_source, 0) + 1
        )
        label = result.sentiment if result.sentiment is not None else "_null"
        label_counts[label] = label_counts.get(label, 0) + 1
    return {
        "sentiment_source_mix": source_counts,
        "sentiment_label_mix": label_counts,
    }


def _persona_distribution(lead_profile_df: pl.DataFrame) -> dict[str, int]:
    if lead_profile_df.height == 0:
        return {}
    counts = lead_profile_df.lazy().group_by("persona").agg(pl.len().alias("count")).collect()
    # Null persona keys would crash structlog's sort_keys JSON encoder
    # (str < None comparison). Bucket unclassified leads under a sentinel.
    return {
        (row["persona"] if row["persona"] is not None else "_unclassified"): int(row["count"])
        for row in counts.iter_rows(named=True)
    }


def _default_persona_classifier(settings: Settings | None) -> PersonaClassifier:
    """Build the production persona classifier on top of
    :func:`pipeline.gold.concurrency.classify_all`.

    Lazy import keeps the orchestrator importable in unit tests that
    inject a fake classifier and never touch the LLM stack.
    """
    if settings is None:
        settings = Settings.load()
    # Local import: avoid pulling the LLM stack into module import for
    # callers that always inject a fake classifier.
    from pipeline.gold.concurrency import classify_all  # noqa: PLC0415

    db_path = settings.state_db_path()

    def _run(
        aggregates: list[LeadAggregate], batch_latest_timestamp: datetime
    ) -> dict[str, PersonaResult]:
        from pipeline.gold.concurrency import _BudgetCounter  # noqa: PLC0415

        budget = _BudgetCounter(settings.pipeline_llm_max_calls_per_batch)
        return asyncio.run(
            classify_all(
                aggregates,
                settings=settings,
                db_path=db_path,
                budget=budget,
                batch_latest_timestamp=batch_latest_timestamp,
            )
        )

    return _run
