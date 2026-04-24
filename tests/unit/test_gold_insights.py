"""Tests for F3.12 insights JSON builder — RED side of TDD.

The module under test (``pipeline.gold.insights``) does NOT exist yet.
Every test here is expected to fail with ``ModuleNotFoundError`` until
F3.12 lands.

Spec drivers
------------
- ``.specs/features/F3/spec.md`` §9 / F3-RF-11 — JSON contract.
- ``.specs/features/F3/design.md`` §8 — per-insight definitions.
"""

from __future__ import annotations

import itertools
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl
import pytest

from pipeline.gold.insights import (
    DETERMINISM_FLAGS,
    INSIGHT_KEYS,
    build_disengagement_moment,
    build_ghosting_taxonomy,
    build_insights,
    build_objections,
    build_persona_outcome_correlation,
)
from pipeline.schemas.gold import PERSONA_VALUES

_BASE_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _ts(seconds: int) -> datetime:
    return _BASE_TS + timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# Silver fixture builder.
# ---------------------------------------------------------------------------


def _silver_row(
    *,
    lead_id: str = "L1",
    conversation_id: str = "c1",
    timestamp: datetime,
    direction: str = "inbound",
    message_body_masked: str | None = None,
    conversation_outcome: str | None = None,
) -> dict[str, Any]:
    return {
        "lead_id": lead_id,
        "conversation_id": conversation_id,
        "timestamp": timestamp,
        "direction": direction,
        "message_body_masked": message_body_masked,
        "conversation_outcome": conversation_outcome,
    }


def _silver_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.DataFrame(
        rows,
        schema={
            "lead_id": pl.String,
            "conversation_id": pl.String,
            "timestamp": pl.Datetime("us", time_zone="UTC"),
            "direction": pl.String,
            "message_body_masked": pl.String,
            "conversation_outcome": pl.String,
        },
    ).lazy()


def _lead_profile_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    persona_dtype = pl.Enum(list(PERSONA_VALUES))
    return pl.DataFrame(
        rows,
        schema={
            "lead_id": pl.String,
            "persona": persona_dtype,
            "closed_count": pl.Int32,
            "conversations": pl.Int32,
        },
    ).lazy()


# ---------------------------------------------------------------------------
# 1. Top-level schema + determinism map exactness.
# ---------------------------------------------------------------------------


def test_determinism_flags_match_spec_exactly() -> None:
    assert DETERMINISM_FLAGS == {
        "ghosting_taxonomy": True,
        "objections": True,
        "disengagement_moment": True,
        "persona_outcome_correlation": False,
    }


def test_insight_keys_are_pinned() -> None:
    assert INSIGHT_KEYS == (
        "ghosting_taxonomy",
        "objections",
        "disengagement_moment",
        "persona_outcome_correlation",
    )


def test_build_insights_emits_required_top_level_keys() -> None:
    silver = _silver_lf([_silver_row(timestamp=_ts(0))])
    leads = _lead_profile_lf([])
    out = build_insights(silver, leads)
    assert set(out) >= {
        "determinism",
        "ghosting_taxonomy",
        "objections",
        "disengagement_moment",
        "persona_outcome_correlation",
    }


def test_build_insights_determinism_block_matches_flags() -> None:
    silver = _silver_lf([_silver_row(timestamp=_ts(0))])
    leads = _lead_profile_lf([])
    out = build_insights(silver, leads)
    assert out["determinism"] == DETERMINISM_FLAGS


def test_build_insights_serialises_to_json() -> None:
    silver = _silver_lf([_silver_row(timestamp=_ts(0))])
    leads = _lead_profile_lf([])
    out = build_insights(silver, leads)
    payload = json.dumps(out)
    round_tripped = json.loads(payload)
    assert round_tripped["determinism"] == DETERMINISM_FLAGS


# ---------------------------------------------------------------------------
# 2. Determinism per insight builder (same input → same output).
# ---------------------------------------------------------------------------


def test_ghosting_taxonomy_is_deterministic_across_runs() -> None:
    silver = _silver_lf(
        [
            _silver_row(lead_id="L1", timestamp=_ts(0), conversation_outcome=None),
            _silver_row(lead_id="L1", timestamp=_ts(60), conversation_outcome=None),
            _silver_row(
                lead_id="L2",
                timestamp=_ts(0),
                conversation_outcome="venda_fechada",
            ),
        ]
    )
    a = build_ghosting_taxonomy(silver)
    b = build_ghosting_taxonomy(silver)
    assert a == b


def test_objections_is_deterministic_across_runs() -> None:
    silver = _silver_lf(
        [
            _silver_row(
                lead_id="L1",
                timestamp=_ts(0),
                direction="inbound",
                message_body_masked="tá caro demais",
                conversation_outcome=None,
            ),
            _silver_row(
                lead_id="L2",
                timestamp=_ts(0),
                direction="inbound",
                message_body_masked="vou pensar",
                conversation_outcome="venda_fechada",
            ),
        ]
    )
    assert build_objections(silver) == build_objections(silver)


def test_disengagement_moment_is_deterministic_across_runs() -> None:
    silver = _silver_lf(
        [
            _silver_row(
                lead_id="L1",
                conversation_id="c1",
                timestamp=_ts(0),
                direction="outbound",
                message_body_masked="segue cotação R$ 1.500",
            ),
            _silver_row(
                lead_id="L1",
                conversation_id="c1",
                timestamp=_ts(60),
                direction="outbound",
                message_body_masked="me passa o CPF",
            ),
        ]
    )
    assert build_disengagement_moment(silver) == build_disengagement_moment(silver)


# ---------------------------------------------------------------------------
# 3. ghosting_taxonomy buckets (1-2, 3-4, 5-9, 10+).
# ---------------------------------------------------------------------------


def test_ghosting_taxonomy_buckets_message_counts() -> None:
    rows: list[dict[str, Any]] = []
    # L1: 2 messages, ghosted → bucket "1-2"
    rows += [
        _silver_row(lead_id="L1", timestamp=_ts(i), conversation_outcome=None) for i in range(2)
    ]
    # L2: 4 messages, ghosted → bucket "3-4"
    rows += [
        _silver_row(lead_id="L2", timestamp=_ts(i), conversation_outcome=None) for i in range(4)
    ]
    # L3: 7 messages, ghosted → bucket "5-9"
    rows += [
        _silver_row(lead_id="L3", timestamp=_ts(i), conversation_outcome=None) for i in range(7)
    ]
    # L4: 12 messages, ghosted → bucket "10+"
    rows += [
        _silver_row(lead_id="L4", timestamp=_ts(i), conversation_outcome=None) for i in range(12)
    ]
    # L5: closed (NOT ghosted) → not counted
    rows.append(_silver_row(lead_id="L5", timestamp=_ts(0), conversation_outcome="venda_fechada"))

    out = build_ghosting_taxonomy(_silver_lf(rows))
    by_label = {item["label"]: item for item in out}
    assert by_label["1-2"]["count"] == 1
    assert by_label["3-4"]["count"] == 1
    assert by_label["5-9"]["count"] == 1
    assert by_label["10+"]["count"] == 1
    # Rate: 4 ghosted leads / 5 total leads = 0.8 across all buckets summed.
    total_rate = sum(item["rate"] for item in out)
    assert total_rate == pytest.approx(4 / 5)


def test_ghosting_taxonomy_excludes_closed_leads() -> None:
    rows = [
        _silver_row(lead_id="L1", timestamp=_ts(0), conversation_outcome="venda_fechada"),
        _silver_row(lead_id="L1", timestamp=_ts(60), conversation_outcome="venda_fechada"),
    ]
    out = build_ghosting_taxonomy(_silver_lf(rows))
    assert all(item["count"] == 0 for item in out)


# ---------------------------------------------------------------------------
# 4. objections regex catalogue.
# ---------------------------------------------------------------------------


def test_objections_counts_lead_side_phrase_hits() -> None:
    rows = [
        _silver_row(
            lead_id="L1",
            timestamp=_ts(0),
            direction="inbound",
            message_body_masked="tá caro demais isso",
            conversation_outcome="ghosting",
        ),
        _silver_row(
            lead_id="L2",
            timestamp=_ts(0),
            direction="inbound",
            message_body_masked="ta caro mesmo",
            conversation_outcome="venda_fechada",
        ),
        _silver_row(
            lead_id="L3",
            timestamp=_ts(0),
            direction="outbound",
            message_body_masked="tá caro? nem tanto",
            conversation_outcome="venda_fechada",
        ),
    ]
    out = build_objections(_silver_lf(rows))
    by_label = {item["label"]: item for item in out}
    assert by_label["tá caro"]["count"] == 2
    # 1 of 2 leads with "tá caro" closed → close_rate_when_present = 0.5.
    assert by_label["tá caro"]["close_rate_when_present"] == pytest.approx(0.5)
    assert isinstance(by_label["tá caro"]["note"], str)


def test_objections_no_hits_emits_zero_count_and_null_rate() -> None:
    rows = [
        _silver_row(
            lead_id="L1",
            timestamp=_ts(0),
            direction="inbound",
            message_body_masked="oi tudo bem",
            conversation_outcome=None,
        ),
    ]
    out = build_objections(_silver_lf(rows))
    for item in out:
        assert item["count"] == 0
        assert item["close_rate_when_present"] is None


# ---------------------------------------------------------------------------
# 5. disengagement_moment two-axis breakdown.
# ---------------------------------------------------------------------------


def test_disengagement_moment_returns_object_with_buckets() -> None:
    silver = _silver_lf([_silver_row(timestamp=_ts(0))])
    out = build_disengagement_moment(silver)
    assert isinstance(out, dict)
    assert "buckets" in out
    assert isinstance(out["buckets"], list)


def test_disengagement_moment_buckets_indices_and_content() -> None:
    rows = [
        # Ghosted lead L1: 1 outbound msg, "cotação R$ 1.500" -> first x after first quote
        _silver_row(
            lead_id="L1",
            conversation_id="c1",
            timestamp=_ts(0),
            direction="outbound",
            message_body_masked="segue cotação R$ 1.500",
            conversation_outcome=None,
        ),
        # Ghosted lead L2: 3 outbound msgs, last one is a CPF ask -> 2nd-3rd x personal data ask
        *[
            _silver_row(
                lead_id="L2",
                conversation_id="c2",
                timestamp=_ts(i * 60),
                direction="outbound",
                message_body_masked="oi",
                conversation_outcome=None,
            )
            for i in range(2)
        ],
        _silver_row(
            lead_id="L2",
            conversation_id="c2",
            timestamp=_ts(180),
            direction="outbound",
            message_body_masked="me passa o seu CPF",
            conversation_outcome=None,
        ),
    ]
    out = build_disengagement_moment(_silver_lf(rows))
    buckets = {(b["index"], b["content"]): b for b in out["buckets"]}
    assert buckets[("first", "after first quote")]["count"] == 1
    assert buckets[("2nd-3rd", "after personal data ask")]["count"] == 1


# ---------------------------------------------------------------------------
# 6. persona_outcome_correlation — LLM insight.
# ---------------------------------------------------------------------------


def test_persona_outcome_correlation_returns_object_shape() -> None:
    silver = _silver_lf([_silver_row(timestamp=_ts(0))])
    leads = _lead_profile_lf([])
    out = build_persona_outcome_correlation(silver, leads)
    assert isinstance(out, dict)
    assert "matrix" in out
    assert "top_surprise" in out


def test_persona_outcome_correlation_crosstab_counts() -> None:
    silver = _silver_lf(
        [
            _silver_row(
                lead_id="L1",
                timestamp=_ts(0),
                conversation_outcome="venda_fechada",
            ),
            _silver_row(lead_id="L2", timestamp=_ts(0), conversation_outcome="ghosting"),
        ]
    )
    leads = _lead_profile_lf(
        [
            {
                "lead_id": "L1",
                "persona": "comprador_rapido",
                "closed_count": 1,
                "conversations": 1,
            },
            {
                "lead_id": "L2",
                "persona": "bouncer",
                "closed_count": 0,
                "conversations": 1,
            },
        ]
    )
    out = build_persona_outcome_correlation(silver, leads)
    matrix = out["matrix"]
    pairs = {(row["persona"], row["outcome"]): row for row in matrix}
    assert pairs[("comprador_rapido", "venda_fechada")]["count"] == 1
    assert pairs[("bouncer", "ghosting")]["count"] == 1


def test_persona_outcome_correlation_respects_injectable_callable() -> None:
    """The LLM-flagged insight builder accepts an injectable callable
    so callers can swap in a deterministic stub for tests / a real
    LLM ranker in production. Default ``None`` ⇒ deterministic
    crosstab (no callable invoked)."""
    silver = _silver_lf(
        [
            _silver_row(
                lead_id="L1",
                timestamp=_ts(0),
                conversation_outcome="venda_fechada",
            )
        ]
    )
    leads = _lead_profile_lf(
        [
            {
                "lead_id": "L1",
                "persona": "comprador_rapido",
                "closed_count": 1,
                "conversations": 1,
            }
        ]
    )
    captured: list[dict[str, Any]] = []

    def fake_ranker(matrix: list[dict[str, Any]]) -> dict[str, Any]:
        captured.append({"matrix": matrix})
        return {"persona": "comprador_rapido", "outcome": "venda_fechada", "deviation": 1.0}

    out = build_persona_outcome_correlation(silver, leads, ranker=fake_ranker)
    assert len(captured) == 1
    assert out["top_surprise"]["persona"] == "comprador_rapido"


# ---------------------------------------------------------------------------
# 7. Edge cases: empty / null persona / no objections / ties.
# ---------------------------------------------------------------------------


def test_build_insights_empty_silver_emits_zero_filled_payload() -> None:
    silver = _silver_lf([])
    leads = _lead_profile_lf([])
    out = build_insights(silver, leads)
    assert out["determinism"] == DETERMINISM_FLAGS
    assert all(item["count"] == 0 for item in out["ghosting_taxonomy"])
    assert all(item["count"] == 0 for item in out["objections"])
    assert out["disengagement_moment"]["buckets"] == [] or all(
        b["count"] == 0 for b in out["disengagement_moment"]["buckets"]
    )
    assert out["persona_outcome_correlation"]["matrix"] == []


def test_persona_outcome_correlation_handles_all_null_personas() -> None:
    silver = _silver_lf(
        [
            _silver_row(
                lead_id="L1",
                timestamp=_ts(0),
                conversation_outcome="venda_fechada",
            )
        ]
    )
    leads = pl.DataFrame(
        {
            "lead_id": ["L1"],
            "persona": [None],
            "closed_count": [1],
            "conversations": [1],
        },
        schema={
            "lead_id": pl.String,
            "persona": pl.Enum(list(PERSONA_VALUES)),
            "closed_count": pl.Int32,
            "conversations": pl.Int32,
        },
    ).lazy()
    out = build_persona_outcome_correlation(silver, leads)
    # All-null persona ⇒ empty crosstab + null top_surprise.
    assert out["matrix"] == []
    assert out["top_surprise"] is None


def test_objections_handles_no_lead_side_messages() -> None:
    rows = [
        _silver_row(
            lead_id="L1",
            timestamp=_ts(0),
            direction="outbound",
            message_body_masked="oi tudo bem?",
            conversation_outcome=None,
        ),
    ]
    out = build_objections(_silver_lf(rows))
    for item in out:
        assert item["count"] == 0


def test_disengagement_moment_resolves_index_ties_via_lex_sort() -> None:
    """Two ghosted leads, both stop after the FIRST outbound, identical
    content bucket. Tie must resolve to a single aggregated row, not
    two undefined-order rows."""
    rows = [
        _silver_row(
            lead_id="L1",
            conversation_id="c1",
            timestamp=_ts(0),
            direction="outbound",
            message_body_masked="segue cotação R$ 1.500",
            conversation_outcome=None,
        ),
        _silver_row(
            lead_id="L2",
            conversation_id="c2",
            timestamp=_ts(0),
            direction="outbound",
            message_body_masked="segue cotação R$ 2.000",
            conversation_outcome=None,
        ),
    ]
    out = build_disengagement_moment(_silver_lf(rows))
    buckets = {(b["index"], b["content"]): b for b in out["buckets"]}
    assert buckets[("first", "after first quote")]["count"] == 2


# ---------------------------------------------------------------------------
# 8. Property test: full builder is idempotent on deterministic insights.
# ---------------------------------------------------------------------------


def test_property_full_builder_idempotent_on_deterministic_insights() -> None:
    """Run build_insights twice; the three deterministic insight
    sub-trees must be byte-identical. The LLM-flagged
    ``persona_outcome_correlation`` is excluded from this property
    (it carries determinism=False on purpose)."""
    bodies = ["tá caro", "vou pensar", "depois", "me dá desconto", "outro lugar"]
    rows: list[dict[str, Any]] = []
    for i, body in enumerate(itertools.islice(itertools.cycle(bodies), 0, 8)):
        rows.append(
            _silver_row(
                lead_id=f"L{i % 4}",
                conversation_id=f"c{i % 4}",
                timestamp=_ts(i * 60),
                direction="inbound",
                message_body_masked=body,
                conversation_outcome=None if i % 2 else "venda_fechada",
            )
        )
    silver = _silver_lf(rows)
    leads = _lead_profile_lf([])
    a = build_insights(silver, leads)
    b = build_insights(silver, leads)
    for key in ("ghosting_taxonomy", "objections", "disengagement_moment"):
        assert a[key] == b[key], f"deterministic insight {key!r} drifted"
