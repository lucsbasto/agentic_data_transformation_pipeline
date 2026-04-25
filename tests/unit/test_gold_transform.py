"""Tests for :func:`pipeline.gold.transform.transform_gold`.

F3.14 — orchestrator that composes the four Gold table builders, runs
the persona lane, computes intent_score, and writes everything (4
parquet partitions + 1 insights JSON) atomically under the same
``batch_id``. Persona classification is dependency-injected so unit
tests can run without spinning up the real LLM thread-pool.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from pipeline.gold.persona import LeadAggregate, PersonaResult
from pipeline.gold.transform import GoldTransformResult, _persona_distribution, transform_gold
from pipeline.schemas.gold import (
    GOLD_AGENT_PERFORMANCE_SCHEMA,
    GOLD_COMPETITOR_INTEL_SCHEMA,
    GOLD_CONVERSATION_SCORES_SCHEMA,
    GOLD_LEAD_PROFILE_SCHEMA,
)

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _ts(seconds: int) -> datetime:
    return _BASE_TS + timedelta(seconds=seconds)


def _silver_row(
    *,
    message_id: str = "m1",
    conversation_id: str = "c1",
    lead_id: str = "L1",
    timestamp: datetime,
    direction: str = "inbound",
    agent_id: str | None = "a1",
    campaign_id: str | None = "camp1",
    sender_phone_masked: str | None = "***",
    sender_name_normalized: str | None = "ana",
    message_body_masked: str | None = "olá",
    has_content: bool = True,
    email_domain: str | None = None,
    has_cpf: bool = False,
    cep_prefix: str | None = None,
    has_phone_mention: bool = False,
    plate_format: str | None = None,
    audio_confidence: str | None = None,
    veiculo_marca: str | None = None,
    veiculo_modelo: str | None = None,
    veiculo_ano: int | None = None,
    concorrente_mencionado: str | None = None,
    valor_pago_atual_brl: float | None = None,
    sinistro_historico: bool | None = None,
    conversation_outcome: str | None = None,
    state: str | None = "SP",
    city: str | None = "Sao Paulo",
    response_time_sec: int | None = None,
    is_business_hours: bool | None = True,
    lead_source: str | None = None,
    device: str | None = None,
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "conversation_id": conversation_id,
        "lead_id": lead_id,
        "timestamp": timestamp,
        "direction": direction,
        "agent_id": agent_id,
        "campaign_id": campaign_id,
        "sender_phone_masked": sender_phone_masked,
        "sender_name_normalized": sender_name_normalized,
        "message_body_masked": message_body_masked,
        "has_content": has_content,
        "email_domain": email_domain,
        "has_cpf": has_cpf,
        "cep_prefix": cep_prefix,
        "has_phone_mention": has_phone_mention,
        "plate_format": plate_format,
        "audio_confidence": audio_confidence,
        "veiculo_marca": veiculo_marca,
        "veiculo_modelo": veiculo_modelo,
        "veiculo_ano": veiculo_ano,
        "concorrente_mencionado": concorrente_mencionado,
        "valor_pago_atual_brl": valor_pago_atual_brl,
        "sinistro_historico": sinistro_historico,
        "conversation_outcome": conversation_outcome,
        "metadata": {
            "device": device,
            "city": city,
            "state": state,
            "response_time_sec": response_time_sec,
            "is_business_hours": is_business_hours,
            "lead_source": lead_source,
        },
    }


_SILVER_SCHEMA: dict[str, pl.DataType] = {
    "message_id": pl.String(),
    "conversation_id": pl.String(),
    "lead_id": pl.String(),
    "timestamp": pl.Datetime("us", time_zone="UTC"),
    "direction": pl.String(),
    "agent_id": pl.String(),
    "campaign_id": pl.String(),
    "sender_phone_masked": pl.String(),
    "sender_name_normalized": pl.String(),
    "message_body_masked": pl.String(),
    "has_content": pl.Boolean(),
    "email_domain": pl.String(),
    "has_cpf": pl.Boolean(),
    "cep_prefix": pl.String(),
    "has_phone_mention": pl.Boolean(),
    "plate_format": pl.String(),
    "audio_confidence": pl.String(),
    "veiculo_marca": pl.String(),
    "veiculo_modelo": pl.String(),
    "veiculo_ano": pl.Int32(),
    "concorrente_mencionado": pl.String(),
    "valor_pago_atual_brl": pl.Float64(),
    "sinistro_historico": pl.Boolean(),
    "conversation_outcome": pl.String(),
    "metadata": pl.Struct(
        {
            "device": pl.String(),
            "city": pl.String(),
            "state": pl.String(),
            "response_time_sec": pl.Int32(),
            "is_business_hours": pl.Boolean(),
            "lead_source": pl.String(),
        }
    ),
}


def _silver_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.DataFrame(rows, schema=_SILVER_SCHEMA).lazy()


def _persona_classifier_factory(
    label: str = "comprador_racional",
    confidence: float = 0.8,
    source: str = "llm",
) -> tuple[
    Callable[[list[LeadAggregate], datetime], dict[str, PersonaResult]],
    list[list[LeadAggregate]],
]:
    """Return a fake persona classifier + a mutable call log.

    Tests that need to assert "the LLM lane was wired" inspect the
    log; tests that need a specific label set ``label``.
    """
    calls: list[list[LeadAggregate]] = []

    def _classifier(
        aggs: list[LeadAggregate], batch_latest_timestamp: datetime
    ) -> dict[str, PersonaResult]:
        calls.append(list(aggs))
        return {
            agg.lead_id: PersonaResult(
                persona=label,
                persona_confidence=confidence,
                persona_source=source,
            )
            for agg in aggs
        }

    return _classifier, calls


def _two_lead_silver_rows() -> list[dict[str, Any]]:
    """A small but realistic multi-lead, multi-conversation Silver
    sample. Two leads, two conversations, one mentions a competitor.
    Engineered so all four Gold tables have at least one row."""
    rows: list[dict[str, Any]] = []
    # Lead L1 conversation c1 — closed, with a competitor mention.
    rows.append(
        _silver_row(
            message_id="m1",
            conversation_id="c1",
            lead_id="L1",
            timestamp=_ts(0),
            direction="outbound",
            agent_id="a1",
            message_body_masked="bom dia",
            has_cpf=False,
            email_domain="gmail.com",
            response_time_sec=30,
        )
    )
    rows.append(
        _silver_row(
            message_id="m2",
            conversation_id="c1",
            lead_id="L1",
            timestamp=_ts(60),
            direction="inbound",
            agent_id="a1",
            message_body_masked="oi, o porto cobrou 1500",
            has_cpf=True,
            email_domain="gmail.com",
            concorrente_mencionado="Porto Seguro",
            valor_pago_atual_brl=1500.0,
            conversation_outcome="venda_fechada",
        )
    )
    # Lead L2 conversation c2 — single inbound, ghosted (no outcome).
    rows.append(
        _silver_row(
            message_id="m3",
            conversation_id="c2",
            lead_id="L2",
            timestamp=_ts(120),
            direction="inbound",
            agent_id="a1",
            message_body_masked="quero saber o preco",
            has_cpf=True,
            email_domain="hotmail.com",
        )
    )
    return rows


# ---------------------------------------------------------------------------
# Result struct.
# ---------------------------------------------------------------------------


def test_result_struct_carries_paths_and_row_counts(tmp_path: Path) -> None:
    """``GoldTransformResult`` exposes one path per Gold output and
    the row counts the CLI needs for the manifest."""
    classifier, _ = _persona_classifier_factory()
    result = transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-1",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    assert isinstance(result, GoldTransformResult)
    assert result.batch_id == "bid-1"
    assert result.conversation_scores_path.exists()
    assert result.lead_profile_path.exists()
    assert result.agent_performance_path.exists()
    assert result.competitor_intel_path.exists()
    assert result.insights_path.exists()
    # Row counts.
    assert result.conversation_scores_rows == 2  # c1, c2
    assert result.lead_profile_rows == 2  # L1, L2
    assert result.agent_performance_rows == 1  # a1
    assert result.competitor_intel_rows == 1  # porto seguro
    # Sum of the four parquet table row counts — F3-RF-13.
    assert result.rows_out == 2 + 2 + 1 + 1


# ---------------------------------------------------------------------------
# Output paths and partition layout (F3-RF-03).
# ---------------------------------------------------------------------------


def test_outputs_land_at_canonical_partition_paths(tmp_path: Path) -> None:
    classifier, _ = _persona_classifier_factory()
    result = transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-paths",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    expected = {
        "conversation_scores": tmp_path
        / "conversation_scores"
        / "batch_id=bid-paths"
        / "part-0.parquet",
        "lead_profile": tmp_path / "lead_profile" / "batch_id=bid-paths" / "part-0.parquet",
        "agent_performance": tmp_path
        / "agent_performance"
        / "batch_id=bid-paths"
        / "part-0.parquet",
        "competitor_intel": tmp_path / "competitor_intel" / "batch_id=bid-paths" / "part-0.parquet",
        "insights": tmp_path / "insights" / "batch_id=bid-paths" / "summary.json",
    }
    assert result.conversation_scores_path == expected["conversation_scores"]
    assert result.lead_profile_path == expected["lead_profile"]
    assert result.agent_performance_path == expected["agent_performance"]
    assert result.competitor_intel_path == expected["competitor_intel"]
    assert result.insights_path == expected["insights"]


# ---------------------------------------------------------------------------
# Schema conformance.
# ---------------------------------------------------------------------------


def test_each_table_matches_its_declared_schema(tmp_path: Path) -> None:
    classifier, _ = _persona_classifier_factory()
    result = transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-schema",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    cs = pl.read_parquet(result.conversation_scores_path)
    lp = pl.read_parquet(result.lead_profile_path)
    ap = pl.read_parquet(result.agent_performance_path)
    ci = pl.read_parquet(result.competitor_intel_path)
    assert cs.schema == GOLD_CONVERSATION_SCORES_SCHEMA
    assert lp.schema == GOLD_LEAD_PROFILE_SCHEMA
    assert ap.schema == GOLD_AGENT_PERFORMANCE_SCHEMA
    assert ci.schema == GOLD_COMPETITOR_INTEL_SCHEMA


# ---------------------------------------------------------------------------
# Persona lane wiring.
# ---------------------------------------------------------------------------


def test_persona_classifier_is_invoked_with_lead_aggregates(tmp_path: Path) -> None:
    """The orchestrator must hand every lead to the classifier
    exactly once. Without this wiring the LLM lane is dead code."""
    classifier, calls = _persona_classifier_factory()
    transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-llm",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    assert len(calls) == 1
    aggs = calls[0]
    assert {agg.lead_id for agg in aggs} == {"L1", "L2"}
    for agg in aggs:
        assert isinstance(agg, LeadAggregate)


def test_persona_labels_propagate_into_lead_profile(tmp_path: Path) -> None:
    """Whatever the classifier returns must land in ``lead_profile``."""
    classifier, _ = _persona_classifier_factory(label="indeciso", confidence=0.42, source="llm")
    result = transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-pers",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    lp = pl.read_parquet(result.lead_profile_path)
    assert set(lp["persona"].to_list()) == {"indeciso"}
    for value in lp["persona_confidence"].to_list():
        assert value == pytest.approx(0.42)


def test_lead_profile_intent_score_is_computed(tmp_path: Path) -> None:
    """``intent_score`` must be filled by the orchestrator (not left
    as the F3.7 typed-null placeholder)."""
    classifier, _ = _persona_classifier_factory()
    result = transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-intent",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    lp = pl.read_parquet(result.lead_profile_path)
    assert lp["intent_score"].dtype == pl.Int32
    for score in lp["intent_score"].to_list():
        assert score is not None
        assert 0 <= score <= 100


def test_price_sensitivity_buckets_by_haggling_hits(tmp_path: Path) -> None:
    """F3-RF-16 price_sensitivity: 0 hits=low, 1-2=medium, 3+=high."""
    classifier, _ = _persona_classifier_factory()
    rows = [
        _silver_row(
            message_id="low-1",
            lead_id="L_low",
            timestamp=_ts(1000),
            message_body_masked="bom dia",
        ),
        _silver_row(
            message_id="med-1",
            lead_id="L_med",
            timestamp=_ts(1100),
            message_body_masked="tem desconto?",
        ),
        _silver_row(
            message_id="high-1",
            lead_id="L_high",
            timestamp=_ts(1200),
            message_body_masked="quero desconto, abaixar o preço, vou em outro lugar",
        ),
    ]
    result = transform_gold(
        _silver_lf(rows),
        batch_id="bid-price",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    lp = pl.read_parquet(result.lead_profile_path).sort("lead_id")
    mapping = dict(zip(lp["lead_id"].to_list(), lp["price_sensitivity"].to_list(), strict=True))
    assert mapping["L_low"] == "low"
    assert mapping["L_med"] == "medium"
    assert mapping["L_high"] == "high"


def test_agent_performance_top_persona_uses_classifier_output(tmp_path: Path) -> None:
    """``top_persona_converted`` is wired from persona labels — the
    F3.6 builder accepts the lead_personas frame, the orchestrator
    must pass it."""
    rows = _two_lead_silver_rows()
    classifier, _ = _persona_classifier_factory(label="comprador_rapido")
    result = transform_gold(
        _silver_lf(rows),
        batch_id="bid-agent",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    ap = pl.read_parquet(result.agent_performance_path)
    # L1 has a closed conversation under agent a1, persona forced to
    # ``comprador_rapido`` by the fake classifier.
    assert ap["top_persona_converted"][0] == "comprador_rapido"


# ---------------------------------------------------------------------------
# Empty Silver batch.
# ---------------------------------------------------------------------------


def test_empty_silver_emits_empty_tables_and_valid_insights(tmp_path: Path) -> None:
    classifier, calls = _persona_classifier_factory()
    result = transform_gold(
        _silver_lf([]),
        batch_id="bid-empty",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    # Tables exist and are empty.
    assert pl.read_parquet(result.conversation_scores_path).height == 0
    assert pl.read_parquet(result.lead_profile_path).height == 0
    assert pl.read_parquet(result.agent_performance_path).height == 0
    assert pl.read_parquet(result.competitor_intel_path).height == 0
    # Insights JSON is well-formed and carries the four insight keys.
    payload = json.loads(result.insights_path.read_text(encoding="utf-8"))
    for key in (
        "ghosting_taxonomy",
        "objections",
        "disengagement_moment",
        "persona_outcome_correlation",
    ):
        assert key in payload
    # Classifier still gets called (with an empty list) so the
    # control-flow path is exercised.
    assert calls == [[]]
    assert result.rows_out == 0


# ---------------------------------------------------------------------------
# Insights wiring.
# ---------------------------------------------------------------------------


def test_insights_payload_carries_batch_id_and_determinism_block(
    tmp_path: Path,
) -> None:
    classifier, _ = _persona_classifier_factory()
    result = transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-ins",
        gold_root=tmp_path,
        persona_classifier=classifier,
    )
    payload = json.loads(result.insights_path.read_text(encoding="utf-8"))
    assert payload["batch_id"] == "bid-ins"
    # Determinism block per design §8.
    assert "determinism" in payload
    assert payload["determinism"]["persona_outcome_correlation"] is False
    assert payload["determinism"]["ghosting_taxonomy"] is True


# ---------------------------------------------------------------------------
# Idempotence / determinism (F3 §1).
# ---------------------------------------------------------------------------


def test_two_runs_produce_byte_identical_parquet(tmp_path: Path) -> None:
    """Re-running with identical Silver + identical persona labels
    must produce byte-identical parquet rows for every Gold table."""
    rows = _two_lead_silver_rows()
    classifier_a, _ = _persona_classifier_factory()
    first = transform_gold(
        _silver_lf(rows),
        batch_id="bid-idem",
        gold_root=tmp_path / "run_a",
        persona_classifier=classifier_a,
    )
    classifier_b, _ = _persona_classifier_factory()
    second = transform_gold(
        _silver_lf(rows),
        batch_id="bid-idem",
        gold_root=tmp_path / "run_b",
        persona_classifier=classifier_b,
    )
    for path_attr in (
        "conversation_scores_path",
        "lead_profile_path",
        "agent_performance_path",
        "competitor_intel_path",
    ):
        df_first = pl.read_parquet(getattr(first, path_attr)).sort(
            pl.all().exclude("outcome_mix") if path_attr == "agent_performance_path" else pl.all()
        )
        df_second = pl.read_parquet(getattr(second, path_attr)).sort(
            pl.all().exclude("outcome_mix") if path_attr == "agent_performance_path" else pl.all()
        )
        assert df_first.equals(df_second), f"{path_attr} differs across runs"


def test_two_runs_produce_equal_insights_modulo_envelope(tmp_path: Path) -> None:
    """Insights payload — including ``generated_at`` — must be
    byte-identical across runs, since ``generated_at`` is anchored to
    ``batch_latest_timestamp`` (Silver-derived) rather than wall-clock.
    """
    rows = _two_lead_silver_rows()
    classifier_a, _ = _persona_classifier_factory()
    first = transform_gold(
        _silver_lf(rows),
        batch_id="bid-idem-ins",
        gold_root=tmp_path / "run_a",
        persona_classifier=classifier_a,
    )
    classifier_b, _ = _persona_classifier_factory()
    second = transform_gold(
        _silver_lf(rows),
        batch_id="bid-idem-ins",
        gold_root=tmp_path / "run_b",
        persona_classifier=classifier_b,
    )
    payload_a = json.loads(first.insights_path.read_text(encoding="utf-8"))
    payload_b = json.loads(second.insights_path.read_text(encoding="utf-8"))
    assert payload_a == payload_b
    assert payload_a["generated_at"] == payload_b["generated_at"]


# ---------------------------------------------------------------------------
# Re-write semantics — orchestrator must overwrite a stale partition.
# ---------------------------------------------------------------------------


def test_rerun_overwrites_existing_partition(tmp_path: Path) -> None:
    """A second call against the same ``batch_id`` overwrites the
    parquet partition (the writer's atomic-replace pattern). The CLI
    layer (F3.15) owns the manifest-level idempotency check; the
    orchestrator stays I/O-pure on writes."""
    classifier_first, _ = _persona_classifier_factory(label="indeciso")
    transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-rerun",
        gold_root=tmp_path,
        persona_classifier=classifier_first,
    )
    classifier_second, _ = _persona_classifier_factory(label="comprador_racional")
    second = transform_gold(
        _silver_lf(_two_lead_silver_rows()),
        batch_id="bid-rerun",
        gold_root=tmp_path,
        persona_classifier=classifier_second,
    )
    lp = pl.read_parquet(second.lead_profile_path)
    assert set(lp["persona"].to_list()) == {"comprador_racional"}


def test_persona_distribution_buckets_null_under_sentinel() -> None:
    # Regression: structlog's sort_keys JSON encoder crashes with
    # "'<' not supported between instances of 'str' and 'NoneType'"
    # when an unclassified-lead null leaks into the breakdown dict.
    df = pl.DataFrame(
        {"persona": ["comprador_rapido", None, None, "indeciso"]},
        schema={"persona": pl.Utf8},
    )
    dist = _persona_distribution(df)
    assert dist == {"comprador_rapido": 1, "indeciso": 1, "_unclassified": 2}
    assert all(isinstance(k, str) for k in dist)
    json.dumps(dist, sort_keys=True)
