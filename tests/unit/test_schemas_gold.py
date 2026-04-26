"""Lock the four Gold parquet schemas + their drift validators."""

from __future__ import annotations

import polars as pl
import pytest

from pipeline.errors import SchemaDriftError
from pipeline.schemas.gold import (
    ENGAGEMENT_PROFILE_VALUES,
    GOLD_AGENT_PERFORMANCE_SCHEMA,
    GOLD_COMPETITOR_INTEL_SCHEMA,
    GOLD_CONVERSATION_SCORES_SCHEMA,
    GOLD_LEAD_PROFILE_SCHEMA,
    OUTCOME_MIX_STRUCT,
    PERSONA_VALUES,
    PRICE_SENSITIVITY_VALUES,
    assert_agent_performance_schema,
    assert_competitor_intel_schema,
    assert_conversation_scores_schema,
    assert_lead_profile_schema,
)

# ---------------------------------------------------------------------------
# Closed-set enums (PRD §17 + F3-RF-16 + F3 design §5.1)
# ---------------------------------------------------------------------------


def test_persona_values_match_prd_section_18_2() -> None:
    """PRD §18.2 names exactly these eight personas, in this order."""
    assert PERSONA_VALUES == (
        "pesquisador_de_preco",
        "comprador_racional",
        "negociador_agressivo",
        "indeciso",
        "comprador_rapido",
        "refem_de_concorrente",
        "bouncer",
        "cacador_de_informacao",
    )
    # Tuple, not set — order is load-bearing for ``pl.Enum``.
    assert isinstance(PERSONA_VALUES, tuple)


def test_engagement_profile_values_are_three_buckets() -> None:
    assert ENGAGEMENT_PROFILE_VALUES == ("hot", "warm", "cold")


def test_price_sensitivity_values_are_three_buckets() -> None:
    assert PRICE_SENSITIVITY_VALUES == ("low", "medium", "high")


# ---------------------------------------------------------------------------
# outcome_mix struct shape (F3 D8 — drift-safe list of records)
# ---------------------------------------------------------------------------


def test_outcome_mix_is_list_of_outcome_count_struct() -> None:
    """Bronze can widen ``conversation_outcome`` later; the list shape
    keeps every observed value as its own element instead of dropping
    unknowns."""
    expected = pl.List(pl.Struct({"outcome": pl.String(), "count": pl.Int32()}))
    assert expected == OUTCOME_MIX_STRUCT


# ---------------------------------------------------------------------------
# Schema column / dtype locks — every Gold table.
# ---------------------------------------------------------------------------


def test_conversation_scores_schema_columns_in_declared_order() -> None:
    assert tuple(GOLD_CONVERSATION_SCORES_SCHEMA.names()) == (
        "conversation_id",
        "lead_id",
        "campaign_id",
        "agent_id",
        "msgs_inbound",
        "msgs_outbound",
        "first_message_at",
        "last_message_at",
        "duration_sec",
        "avg_lead_response_sec",
        "time_to_first_response_sec",
        "off_hours_msgs",
        "mencionou_concorrente",
        "concorrente_citado",
        "veiculo_marca",
        "veiculo_modelo",
        "veiculo_ano",
        "valor_pago_atual_brl",
        "sinistro_historico",
        "conversation_outcome",
    )


def test_conversation_scores_schema_dtypes() -> None:
    schema = GOLD_CONVERSATION_SCORES_SCHEMA
    assert schema["conversation_id"] == pl.String()
    assert schema["msgs_inbound"] == pl.Int32()
    assert schema["duration_sec"] == pl.Int64()
    assert schema["first_message_at"] == pl.Datetime("us", time_zone="UTC")
    assert schema["avg_lead_response_sec"] == pl.Float64()
    assert schema["mencionou_concorrente"] == pl.Boolean()
    assert schema["veiculo_ano"] == pl.Int32()
    assert schema["valor_pago_atual_brl"] == pl.Float64()
    assert schema["sinistro_historico"] == pl.Boolean()


def test_lead_profile_schema_columns_in_declared_order() -> None:
    assert tuple(GOLD_LEAD_PROFILE_SCHEMA.names()) == (
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
    )


def test_lead_profile_uses_closed_set_enums() -> None:
    schema = GOLD_LEAD_PROFILE_SCHEMA
    # Persona enum carries the eight PRD values in declared order.
    persona_dtype = schema["persona"]
    assert isinstance(persona_dtype, pl.Enum)
    assert tuple(persona_dtype.categories) == PERSONA_VALUES
    # Engagement / price-sensitivity enums share the same shape.
    eng_dtype = schema["engagement_profile"]
    assert isinstance(eng_dtype, pl.Enum)
    assert tuple(eng_dtype.categories) == ENGAGEMENT_PROFILE_VALUES
    price_dtype = schema["price_sensitivity"]
    assert isinstance(price_dtype, pl.Enum)
    assert tuple(price_dtype.categories) == PRICE_SENSITIVITY_VALUES
    # Numeric scalars.
    assert schema["intent_score"] == pl.Int32()
    assert schema["persona_confidence"] == pl.Float64()


def test_agent_performance_schema_columns_in_declared_order() -> None:
    assert tuple(GOLD_AGENT_PERFORMANCE_SCHEMA.names()) == (
        "agent_id",
        "conversations",
        "closed_count",
        "close_rate",
        "avg_response_time_sec",
        "outcome_mix",
        "top_persona_converted",
    )


def test_agent_performance_outcome_mix_is_drift_safe_list() -> None:
    schema = GOLD_AGENT_PERFORMANCE_SCHEMA
    assert schema["outcome_mix"] == OUTCOME_MIX_STRUCT
    assert isinstance(schema["top_persona_converted"], pl.Enum)


def test_competitor_intel_schema_columns_in_declared_order() -> None:
    assert tuple(GOLD_COMPETITOR_INTEL_SCHEMA.names()) == (
        "competitor",
        "mention_count",
        "avg_quoted_price_brl",
        "loss_rate",
        "top_states",
    )


def test_competitor_intel_top_states_is_string_list() -> None:
    assert GOLD_COMPETITOR_INTEL_SCHEMA["top_states"] == pl.List(pl.String())


# ---------------------------------------------------------------------------
# assert_<table>_schema — happy path + drift detection.
# ---------------------------------------------------------------------------


def _empty_conversation_scores_df() -> pl.DataFrame:
    return pl.DataFrame(schema=GOLD_CONVERSATION_SCORES_SCHEMA)


def _empty_lead_profile_df() -> pl.DataFrame:
    return pl.DataFrame(schema=GOLD_LEAD_PROFILE_SCHEMA)


def _empty_agent_performance_df() -> pl.DataFrame:
    return pl.DataFrame(schema=GOLD_AGENT_PERFORMANCE_SCHEMA)


def _empty_competitor_intel_df() -> pl.DataFrame:
    return pl.DataFrame(schema=GOLD_COMPETITOR_INTEL_SCHEMA)


def test_assert_conversation_scores_schema_accepts_matching_df() -> None:
    assert_conversation_scores_schema(_empty_conversation_scores_df())


def test_assert_lead_profile_schema_accepts_matching_df() -> None:
    assert_lead_profile_schema(_empty_lead_profile_df())


def test_assert_agent_performance_schema_accepts_matching_df() -> None:
    assert_agent_performance_schema(_empty_agent_performance_df())


def test_assert_competitor_intel_schema_accepts_matching_df() -> None:
    assert_competitor_intel_schema(_empty_competitor_intel_df())


def test_assert_schema_flags_missing_column() -> None:
    df = _empty_conversation_scores_df().drop("agent_id")
    with pytest.raises(SchemaDriftError, match="missing column 'agent_id'"):
        assert_conversation_scores_schema(df)


def test_assert_schema_flags_dtype_mismatch() -> None:
    # ``msgs_inbound`` is declared Int32; rebuild the column as Int64 to
    # simulate an upstream change that silently widened the dtype.
    base = _empty_conversation_scores_df()
    bad = base.with_columns(pl.col("msgs_inbound").cast(pl.Int64))
    with pytest.raises(SchemaDriftError, match="'msgs_inbound'"):
        assert_conversation_scores_schema(bad)


def test_assert_schema_flags_unexpected_column() -> None:
    df = _empty_lead_profile_df().with_columns(
        pl.lit(None, dtype=pl.String).alias("rogue")
    )
    with pytest.raises(SchemaDriftError, match="unexpected column 'rogue'"):
        assert_lead_profile_schema(df)


def test_assert_schema_names_the_table_in_the_error_message() -> None:
    """Each helper must label its message with the table it guards so a
    failing CI run identifies the offender without a stack walk."""
    df = _empty_competitor_intel_df().drop("competitor")
    with pytest.raises(SchemaDriftError, match=r"gold\.competitor_intel"):
        assert_competitor_intel_schema(df)


def test_assert_schema_flags_outcome_mix_inner_dtype_widening() -> None:
    """``outcome_mix`` is the spec D8 drift-safe column. If a future
    aggregator silently widens the inner ``count`` to ``Int64`` the
    drift validator must catch it — that's the whole point of pinning
    the inner Struct shape."""
    base = _empty_agent_performance_df()
    bad_dtype = pl.List(pl.Struct({"outcome": pl.String(), "count": pl.Int64()}))
    bad = base.with_columns(pl.col("outcome_mix").cast(bad_dtype))
    with pytest.raises(SchemaDriftError, match="'outcome_mix'"):
        assert_agent_performance_schema(bad)


def test_assert_schema_flags_persona_enum_category_drop() -> None:
    """A persona enum that loses a category must be flagged. PRD §18.2
    fixes the eight labels; dropping one (or reordering) silently
    would let downstream code emit values the writer cannot encode."""
    base = _empty_lead_profile_df()
    truncated_enum = pl.Enum(list(PERSONA_VALUES[:-1]))
    bad = base.with_columns(pl.col("persona").cast(truncated_enum))
    with pytest.raises(SchemaDriftError, match="'persona'"):
        assert_lead_profile_schema(bad)


@pytest.mark.parametrize(
    ("assert_fn", "drop_col", "expected_label"),
    [
        (assert_conversation_scores_schema, "agent_id", r"gold\.conversation_scores"),
        (assert_lead_profile_schema, "persona", r"gold\.lead_profile"),
        (assert_agent_performance_schema, "outcome_mix", r"gold\.agent_performance"),
        (assert_competitor_intel_schema, "competitor", r"gold\.competitor_intel"),
    ],
)
def test_every_assert_helper_labels_its_own_table(
    assert_fn: object,
    drop_col: str,
    expected_label: str,
) -> None:
    """Guard against a copy-paste bug where two helpers reuse the same
    table name — drop one column from the matching empty frame and
    confirm the helper raises with the correct label."""
    schema_map = {
        assert_conversation_scores_schema: GOLD_CONVERSATION_SCORES_SCHEMA,
        assert_lead_profile_schema: GOLD_LEAD_PROFILE_SCHEMA,
        assert_agent_performance_schema: GOLD_AGENT_PERFORMANCE_SCHEMA,
        assert_competitor_intel_schema: GOLD_COMPETITOR_INTEL_SCHEMA,
    }
    df = pl.DataFrame(schema=schema_map[assert_fn]).drop(drop_col)  # type: ignore[index]
    with pytest.raises(SchemaDriftError, match=expected_label):
        assert_fn(df)  # type: ignore[operator]
