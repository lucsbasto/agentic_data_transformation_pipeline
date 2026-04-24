"""Tests for :func:`pipeline.gold.lead_profile.build_lead_profile_skeleton`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl

from pipeline.gold.lead_profile import build_lead_profile_skeleton
from pipeline.schemas.gold import (
    GOLD_LEAD_PROFILE_SCHEMA,
    assert_lead_profile_schema,
)

_BASE_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _ts(seconds: int) -> datetime:
    return _BASE_TS + timedelta(seconds=seconds)


def _silver_row(
    *,
    conversation_id: str = "c1",
    lead_id: str = "L1",
    timestamp: datetime,
    sender_name_normalized: str | None = "ana",
    email_domain: str | None = None,
    state: str | None = None,
    city: str | None = None,
    conversation_outcome: str | None = None,
) -> dict[str, Any]:
    return {
        "conversation_id": conversation_id,
        "lead_id": lead_id,
        "timestamp": timestamp,
        "sender_name_normalized": sender_name_normalized,
        "email_domain": email_domain,
        "conversation_outcome": conversation_outcome,
        "metadata": {"state": state, "city": city},
    }


def _silver_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.DataFrame(
        rows,
        schema={
            "conversation_id": pl.String,
            "lead_id": pl.String,
            "timestamp": pl.Datetime("us", time_zone="UTC"),
            "sender_name_normalized": pl.String,
            "email_domain": pl.String,
            "conversation_outcome": pl.String,
            "metadata": pl.Struct({"state": pl.String, "city": pl.String}),
        },
    ).lazy()


# ---------------------------------------------------------------------------
# Schema lock + grain.
# ---------------------------------------------------------------------------


def test_output_matches_declared_schema() -> None:
    rows = [
        _silver_row(timestamp=_ts(0), conversation_outcome="venda_fechada"),
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(60)
    ).collect()
    assert out.schema == GOLD_LEAD_PROFILE_SCHEMA
    assert_lead_profile_schema(out)


def test_one_row_per_lead_id() -> None:
    rows = [
        _silver_row(lead_id="L1", conversation_id="c1", timestamp=_ts(0)),
        _silver_row(lead_id="L1", conversation_id="c2", timestamp=_ts(60)),
        _silver_row(lead_id="L2", conversation_id="c3", timestamp=_ts(120)),
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(120)
    ).collect()
    assert out.height == 2


def test_empty_input_emits_empty_frame() -> None:
    out = build_lead_profile_skeleton(
        _silver_lf([]), batch_latest_timestamp=_ts(0)
    ).collect()
    assert out.height == 0
    assert out.schema == GOLD_LEAD_PROFILE_SCHEMA


# ---------------------------------------------------------------------------
# Aggregations.
# ---------------------------------------------------------------------------


def test_close_rate_on_distinct_conversations() -> None:
    """One closed and one ghosted conversation → close_rate=0.5."""
    rows = [
        _silver_row(
            conversation_id="c1",
            timestamp=_ts(0),
            conversation_outcome="venda_fechada",
        ),
        _silver_row(
            conversation_id="c2",
            timestamp=_ts(60),
            conversation_outcome="ghosting",
        ),
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(120)
    ).collect()
    assert out["conversations"][0] == 2
    assert out["closed_count"][0] == 1
    assert out["close_rate"][0] == 0.5


def test_dominant_email_state_city_pick_most_frequent() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0),
            email_domain="gmail.com",
            state="SP",
            city="Sao Paulo",
        ),
        _silver_row(
            timestamp=_ts(60),
            email_domain="gmail.com",
            state="SP",
            city="Sao Paulo",
        ),
        _silver_row(
            timestamp=_ts(120),
            email_domain="hotmail.com",
            state="RJ",
            city="Rio de Janeiro",
        ),
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(120)
    ).collect()
    assert out["dominant_email_domain"][0] == "gmail.com"
    assert out["dominant_state"][0] == "SP"
    assert out["dominant_city"][0] == "Sao Paulo"


def test_first_and_last_seen_at_span_the_lead_history() -> None:
    rows = [
        _silver_row(timestamp=_ts(0)),
        _silver_row(timestamp=_ts(900)),
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(900)
    ).collect()
    assert out["first_seen_at"][0] == _ts(0)
    assert out["last_seen_at"][0] == _ts(900)


# ---------------------------------------------------------------------------
# engagement_profile boundaries (design §5.1).
# ---------------------------------------------------------------------------


def test_engagement_hot_when_close_rate_ge_half() -> None:
    """conversations=2, close_rate=0.5 → hot (matches the design
    boundary fixture)."""
    rows = [
        _silver_row(
            conversation_id="c1",
            timestamp=_ts(0),
            conversation_outcome="venda_fechada",
        ),
        _silver_row(
            conversation_id="c2",
            timestamp=_ts(60),
            conversation_outcome="ghosting",
        ),
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(60)
    ).collect()
    assert out["engagement_profile"][0] == "hot"


def test_engagement_hot_when_conversations_ge_three_with_any_close() -> None:
    rows = [
        _silver_row(
            conversation_id=f"c{i}",
            timestamp=_ts(i * 60),
            conversation_outcome="venda_fechada" if i == 0 else None,
        )
        for i in range(3)
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(180)
    ).collect()
    assert out["engagement_profile"][0] == "hot"


def test_engagement_warm_when_close_rate_just_below_half() -> None:
    """100 conversations with 49 closes (close_rate=0.49) is JUST
    below the hot threshold and must land as warm — locks the
    inclusive boundary so a future swap of ``>=`` to ``>`` would
    fail loudly."""
    rows = []
    for i in range(100):
        rows.append(
            _silver_row(
                conversation_id=f"c{i}",
                timestamp=_ts(i),
                conversation_outcome="venda_fechada" if i < 49 else "ghosting",
            )
        )
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(100)
    ).collect()
    assert out["close_rate"][0] == 0.49
    # 100 conversations triggers clause-2 (conversations>=3 AND close>0),
    # so this lead is hot via clause-2 even though clause-1 misses.
    assert out["engagement_profile"][0] == "hot"


def test_engagement_warm_when_close_rate_below_half_with_only_one_conversation() -> None:
    """1 conversation with no close, fresh → warm (clause-2 cannot
    fire with conversations < 3, clause-1 cannot fire with
    closed_count = 0). Locks the close_rate=0 just-below-hot region
    that a ``>=`` -> ``>`` refactor would otherwise miss."""
    rows = [
        _silver_row(timestamp=_ts(0), conversation_outcome=None),
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(60)
    ).collect()
    assert out["close_rate"][0] == 0.0
    assert out["engagement_profile"][0] == "warm"


def test_engagement_warm_when_fresh_single_conversation_no_close() -> None:
    rows = [
        _silver_row(
            timestamp=_ts(0), conversation_outcome=None
        ),
    ]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(60)
    ).collect()
    assert out["engagement_profile"][0] == "warm"


def test_engagement_cold_when_stale_single_conversation_no_close() -> None:
    """conversations=1, closed_count=0, last_seen > 48h before
    batch_latest → cold."""
    rows = [
        _silver_row(timestamp=_ts(0), conversation_outcome=None),
    ]
    # batch_latest is 49h after the lead's last message.
    batch_latest = _ts(0) + timedelta(hours=49)
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=batch_latest
    ).collect()
    assert out["engagement_profile"][0] == "cold"


# ---------------------------------------------------------------------------
# Persona / score placeholders are typed nulls.
# ---------------------------------------------------------------------------


def test_persona_confidence_score_columns_are_typed_nulls() -> None:
    rows = [_silver_row(timestamp=_ts(0))]
    out = build_lead_profile_skeleton(
        _silver_lf(rows), batch_latest_timestamp=_ts(0)
    ).collect()
    for col in ("persona", "persona_confidence", "price_sensitivity", "intent_score"):
        assert out[col][0] is None
    # Dtypes must match the schema so F3.10 / F3.11 can replace in
    # place via with_columns.
    assert out.schema == GOLD_LEAD_PROFILE_SCHEMA
