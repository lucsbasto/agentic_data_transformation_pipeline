"""F3.12 insights JSON builder (PRD §6.3, spec §9 / F3-RF-11).

Emits the per-batch ``summary.json`` payload from a Silver LazyFrame
+ the post-persona ``gold.lead_profile`` LazyFrame. Three insights
are deterministic (Silver-only regex + count aggregates); one
(``persona_outcome_correlation``) depends on LLM-sourced persona
labels and is flagged as such in the ``determinism`` block.

Spec drivers
------------
- ``F3-RF-11`` — top-level keys + per-insight record shape.
- ``F3 design §8`` — bucket definitions for each insight.
- ``D9`` — determinism caveat for the persona-coupled insight.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Final

import polars as pl

from pipeline.gold._regex import (
    OBJECTION_PHRASES,
    count_phrase_hits,
    matches_any,
)

__all__ = [
    "DETERMINISM_FLAGS",
    "INSIGHT_KEYS",
    "build_disengagement_moment",
    "build_ghosting_taxonomy",
    "build_insights",
    "build_objections",
    "build_persona_outcome_correlation",
]

INSIGHT_KEYS: Final[tuple[str, ...]] = (
    "ghosting_taxonomy",
    "objections",
    "disengagement_moment",
    "persona_outcome_correlation",
)

DETERMINISM_FLAGS: Final[dict[str, bool]] = {
    "ghosting_taxonomy": True,
    "objections": True,
    "disengagement_moment": True,
    "persona_outcome_correlation": False,
}

# ---------------------------------------------------------------------------
# Bucket catalogues — declared once so the JSON shape is stable even
# when no rows hit a given bucket.
# ---------------------------------------------------------------------------

_GHOSTING_BUCKETS: Final[tuple[tuple[str, int, int], ...]] = (
    ("1-2", 1, 2),
    ("3-4", 3, 4),
    ("5-9", 5, 9),
    ("10+", 10, 10**9),
)

_INDEX_BUCKETS: Final[tuple[str, ...]] = ("first", "2nd-3rd", "4th+")

_CONTENT_BUCKETS: Final[tuple[str, ...]] = (
    "after first quote",
    "after price ask",
    "after personal data ask",
    "other",
)

_QUOTE_PHRASES: Final[tuple[str, ...]] = ("cotação", "cotacao", "r$", "valor de")
_PRICE_ASK_PHRASES: Final[tuple[str, ...]] = (
    "qual o valor",
    "qual valor",
    "preço",
    "quanto fica",
)
_PERSONAL_DATA_PHRASES: Final[tuple[str, ...]] = (
    "cpf",
    "rg",
    "endereço",
    "endereco",
    "data de nascimento",
)

_FIRST_INDEX_MAX: Final[int] = 1
_MID_INDEX_MAX: Final[int] = 3

_OBJECTION_NOTES: Final[dict[str, str]] = {
    "tá caro": "Lead pushed back on price.",
    "vou pensar": "Lead deferred a decision.",
    "depois": "Lead postponed engagement.",
    "me dá desconto": "Lead asked for a discount.",
    "outro lugar": "Lead invoked a competing quote.",
}


# ---------------------------------------------------------------------------
# ghosting_taxonomy.
# ---------------------------------------------------------------------------


def build_ghosting_taxonomy(silver_lf: pl.LazyFrame) -> list[dict[str, Any]]:
    per_lead = (
        silver_lf.group_by("lead_id")
        .agg(
            pl.len().alias("msg_count"),
            pl.col("conversation_outcome").is_not_null().any().alias("has_outcome"),
        )
        .collect()
    )
    total_leads = per_lead.height
    ghosted = per_lead.filter(~pl.col("has_outcome"))
    counts = ghosted["msg_count"].to_list()

    return [
        _ghosting_bucket(label, lo, hi, counts, total_leads) for label, lo, hi in _GHOSTING_BUCKETS
    ]


def _ghosting_bucket(
    label: str,
    lo: int,
    hi: int,
    counts: list[int],
    total_leads: int,
) -> dict[str, Any]:
    bucket_count = sum(1 for c in counts if lo <= c <= hi)
    rate = bucket_count / total_leads if total_leads else 0.0
    return {
        "label": label,
        "count": bucket_count,
        "rate": rate,
        "note": f"{label} messages before ghosting.",
    }


# ---------------------------------------------------------------------------
# objections.
# ---------------------------------------------------------------------------


def build_objections(silver_lf: pl.LazyFrame) -> list[dict[str, Any]]:
    inbound = silver_lf.filter(pl.col("direction") == "inbound").collect()
    bodies = inbound["message_body_masked"].to_list()
    leads = inbound["lead_id"].to_list()
    outcomes = inbound["conversation_outcome"].to_list()

    lead_outcome: dict[str, str | None] = {}
    for lead_id, outcome in zip(leads, outcomes, strict=True):
        if outcome is not None:
            lead_outcome[lead_id] = outcome
        else:
            lead_outcome.setdefault(lead_id, None)

    return [_objection_record(phrase, bodies, leads, lead_outcome) for phrase in OBJECTION_PHRASES]


def _objection_record(
    phrase: str,
    bodies: list[str | None],
    leads: list[str],
    lead_outcome: dict[str, str | None],
) -> dict[str, Any]:
    leads_with_phrase = {
        lead_id
        for lead_id, body in zip(leads, bodies, strict=True)
        if count_phrase_hits(body, (phrase,)) > 0
    }
    count = len(leads_with_phrase)
    if count == 0:
        close_rate: float | None = None
    else:
        closed = sum(
            1 for lead_id in leads_with_phrase if lead_outcome.get(lead_id) == "venda_fechada"
        )
        close_rate = closed / count
    return {
        "label": phrase,
        "count": count,
        "close_rate_when_present": close_rate,
        "note": _OBJECTION_NOTES.get(phrase, "Objection phrase observed."),
    }


# ---------------------------------------------------------------------------
# disengagement_moment.
# ---------------------------------------------------------------------------


def build_disengagement_moment(silver_lf: pl.LazyFrame) -> dict[str, Any]:
    outbound = (
        silver_lf.filter(pl.col("direction") == "outbound")
        .sort(["conversation_id", "timestamp"])
        .with_columns(
            pl.cum_count("timestamp").over("conversation_id").alias("_idx"),
        )
        .collect()
    )

    last_outbound = (
        outbound.group_by("conversation_id")
        .agg(
            pl.col("_idx").max().alias("_last_idx"),
            pl.col("message_body_masked").last().alias("_last_body"),
            pl.col("conversation_outcome").last().alias("_outcome"),
        )
        .filter(pl.col("_outcome").is_null())
    )

    indices = last_outbound["_last_idx"].to_list()
    bodies = last_outbound["_last_body"].to_list()

    counts: dict[tuple[str, str], int] = {
        (idx, content): 0 for idx in _INDEX_BUCKETS for content in _CONTENT_BUCKETS
    }
    for idx, body in zip(indices, bodies, strict=True):
        counts[(_index_bucket(idx), _content_bucket(body))] += 1

    buckets = [
        {"index": idx, "content": content, "count": counts[(idx, content)]}
        for idx in _INDEX_BUCKETS
        for content in _CONTENT_BUCKETS
    ]
    return {"buckets": buckets}


def _index_bucket(idx: int) -> str:
    if idx <= _FIRST_INDEX_MAX:
        return "first"
    if idx <= _MID_INDEX_MAX:
        return "2nd-3rd"
    return "4th+"


def _content_bucket(body: str | None) -> str:
    if matches_any(body, _QUOTE_PHRASES):
        return "after first quote"
    if matches_any(body, _PRICE_ASK_PHRASES):
        return "after price ask"
    if matches_any(body, _PERSONAL_DATA_PHRASES):
        return "after personal data ask"
    return "other"


# ---------------------------------------------------------------------------
# persona_outcome_correlation (LLM-flagged).
# ---------------------------------------------------------------------------

Ranker = Callable[[list[dict[str, Any]]], dict[str, Any] | None]


def build_persona_outcome_correlation(
    silver_lf: pl.LazyFrame,
    lead_profile_lf: pl.LazyFrame,
    *,
    ranker: Ranker | None = None,
) -> dict[str, Any]:
    """Crosstab of ``persona`` x ``conversation_outcome``.

    The optional ``ranker`` callable receives the matrix and returns
    the ``top_surprise`` record. Default ``None`` runs a deterministic
    "largest absolute deviation from the overall close rate" fallback
    so unit tests can assert the shape without an LLM. F3.14
    orchestrator can swap in an LLM-backed ranker; the determinism
    flag for this insight stays ``False`` regardless.
    """
    per_conv = (
        silver_lf.group_by(["lead_id", "conversation_id"])
        .agg(pl.col("conversation_outcome").drop_nulls().last().alias("outcome"))
        .filter(pl.col("outcome").is_not_null())
    )
    joined = per_conv.join(lead_profile_lf, on="lead_id", how="inner").filter(
        pl.col("persona").is_not_null()
    )
    matrix_df = (
        joined.group_by(["persona", "outcome"])
        .agg(pl.len().cast(pl.Int32).alias("count"))
        .sort(["persona", "outcome"])
        .collect()
    )

    if matrix_df.height == 0:
        return {"matrix": [], "top_surprise": None}

    matrix = _matrix_with_rates(matrix_df)
    top_surprise = _default_top_surprise(matrix) if ranker is None else ranker(matrix)
    return {"matrix": matrix, "top_surprise": top_surprise}


def _matrix_with_rates(matrix_df: pl.DataFrame) -> list[dict[str, Any]]:
    totals = matrix_df.group_by("persona").agg(pl.col("count").sum().alias("_persona_total"))
    enriched = (
        matrix_df.join(totals, on="persona", how="left")
        .with_columns((pl.col("count") / pl.col("_persona_total")).alias("rate"))
        .drop("_persona_total")
        .with_columns(pl.col("persona").cast(pl.String))
    )
    return enriched.to_dicts()


def _default_top_surprise(matrix: list[dict[str, Any]]) -> dict[str, Any] | None:
    closed_rows = [row for row in matrix if row["outcome"] == "venda_fechada"]
    if not closed_rows:
        return None
    overall_close_rate = sum(row["count"] for row in closed_rows) / sum(
        row["count"] for row in matrix
    )
    top = max(closed_rows, key=lambda row: abs(row["rate"] - overall_close_rate))
    return {
        "persona": top["persona"],
        "outcome": top["outcome"],
        "deviation": top["rate"] - overall_close_rate,
    }


# ---------------------------------------------------------------------------
# Top-level builder.
# ---------------------------------------------------------------------------


def build_insights(
    silver_lf: pl.LazyFrame,
    lead_profile_lf: pl.LazyFrame,
    *,
    persona_outcome_ranker: Ranker | None = None,
) -> dict[str, Any]:
    return {
        "determinism": dict(DETERMINISM_FLAGS),
        "ghosting_taxonomy": build_ghosting_taxonomy(silver_lf),
        "objections": build_objections(silver_lf),
        "disengagement_moment": build_disengagement_moment(silver_lf),
        "persona_outcome_correlation": build_persona_outcome_correlation(
            silver_lf, lead_profile_lf, ranker=persona_outcome_ranker
        ),
    }
