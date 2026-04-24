"""Tests for ``pipeline.gold.writer`` — atomic parquet + JSON writes."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.errors import PipelineError, SchemaDriftError
from pipeline.gold.writer import (
    write_gold_agent_performance,
    write_gold_competitor_intel,
    write_gold_conversation_scores,
    write_gold_insights,
    write_gold_lead_profile,
)
from pipeline.schemas.gold import (
    GOLD_AGENT_PERFORMANCE_SCHEMA,
    GOLD_COMPETITOR_INTEL_SCHEMA,
    GOLD_CONVERSATION_SCORES_SCHEMA,
    GOLD_LEAD_PROFILE_SCHEMA,
)

# ---------------------------------------------------------------------------
# Schema-shaped fixtures.  Every fixture builds the smallest valid frame for
# its target table so the writer's schema assertion accepts it.  Building
# from the canonical ``pl.Schema`` keeps the tests in lock-step with any
# future column / dtype change in ``schemas/gold.py``.
# ---------------------------------------------------------------------------


def _empty_df_for(schema: pl.Schema) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def _conv_scores_row() -> pl.DataFrame:
    """Single-row frame matching ``GOLD_CONVERSATION_SCORES_SCHEMA`` exactly.

    Built from the canonical schema dict so a future column addition
    surfaces here as a typed test failure rather than silently passing.
    """
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    row: dict[str, object] = {
        "conversation_id": "c1",
        "lead_id": "l1",
        "campaign_id": "camp",
        "agent_id": "a1",
        "msgs_inbound": 2,
        "msgs_outbound": 3,
        "first_message_at": ts,
        "last_message_at": ts,
        "duration_sec": 3600,
        "avg_lead_response_sec": 12.5,
        "time_to_first_response_sec": 4.0,
        "off_hours_msgs": 0,
        "mencionou_concorrente": False,
        "concorrente_citado": None,
        "veiculo_marca": None,
        "veiculo_modelo": None,
        "veiculo_ano": None,
        "valor_pago_atual_brl": None,
        "sinistro_historico": False,
        "conversation_outcome": "venda_fechada",
    }
    return pl.DataFrame([row], schema=GOLD_CONVERSATION_SCORES_SCHEMA)


def _lead_profile_row() -> pl.DataFrame:
    return _empty_df_for(GOLD_LEAD_PROFILE_SCHEMA)


def _agent_perf_row() -> pl.DataFrame:
    return _empty_df_for(GOLD_AGENT_PERFORMANCE_SCHEMA)


def _competitor_intel_row() -> pl.DataFrame:
    return _empty_df_for(GOLD_COMPETITOR_INTEL_SCHEMA)


# ---------------------------------------------------------------------------
# Parquet writers — happy path, layout, idempotency, atomicity.
# ---------------------------------------------------------------------------


def test_write_conversation_scores_creates_partition(tmp_path: Path) -> None:
    df = _empty_df_for(GOLD_CONVERSATION_SCORES_SCHEMA)
    result = write_gold_conversation_scores(df, gold_root=tmp_path, batch_id="b1")
    expected = tmp_path / "conversation_scores" / "batch_id=b1" / "part-0.parquet"
    assert result.gold_path == expected
    assert result.gold_path.is_file()
    assert result.rows_written == 0


def test_write_lead_profile_layout(tmp_path: Path) -> None:
    df = _lead_profile_row()
    result = write_gold_lead_profile(df, gold_root=tmp_path, batch_id="b2")
    assert result.gold_path == tmp_path / "lead_profile" / "batch_id=b2" / "part-0.parquet"
    assert result.gold_path.is_file()


def test_write_agent_performance_layout(tmp_path: Path) -> None:
    df = _agent_perf_row()
    result = write_gold_agent_performance(df, gold_root=tmp_path, batch_id="b3")
    assert result.gold_path == tmp_path / "agent_performance" / "batch_id=b3" / "part-0.parquet"


def test_write_competitor_intel_layout(tmp_path: Path) -> None:
    df = _competitor_intel_row()
    result = write_gold_competitor_intel(df, gold_root=tmp_path, batch_id="b4")
    assert result.gold_path == tmp_path / "competitor_intel" / "batch_id=b4" / "part-0.parquet"


def test_write_with_data_round_trips(tmp_path: Path) -> None:
    df = _conv_scores_row()
    result = write_gold_conversation_scores(df, gold_root=tmp_path, batch_id="b-rt")
    assert result.rows_written == 1
    reloaded = pl.scan_parquet(result.gold_path).collect()
    assert reloaded.height == 1
    assert reloaded.schema == GOLD_CONVERSATION_SCORES_SCHEMA


def test_tmp_dir_is_cleaned_after_success(tmp_path: Path) -> None:
    df = _conv_scores_row()
    write_gold_conversation_scores(df, gold_root=tmp_path, batch_id="b-clean")
    leftover = tmp_path / "conversation_scores" / ".tmp-batch_id=b-clean"
    assert not leftover.exists()


def test_idempotent_overwrite(tmp_path: Path) -> None:
    df1 = _conv_scores_row()
    write_gold_conversation_scores(df1, gold_root=tmp_path, batch_id="b-rw")
    # Second write with a (different) but still valid empty frame; the
    # contract is "second wins, no leftovers, no extra files".
    df2 = _empty_df_for(GOLD_CONVERSATION_SCORES_SCHEMA)
    result = write_gold_conversation_scores(df2, gold_root=tmp_path, batch_id="b-rw")
    reloaded = pl.scan_parquet(result.gold_path).collect()
    assert reloaded.height == 0
    # Final partition has exactly one parquet file (no .tmp leftovers).
    final_dir = tmp_path / "conversation_scores" / "batch_id=b-rw"
    files = sorted(p.name for p in final_dir.iterdir())
    assert files == ["part-0.parquet"]


def test_schema_drift_rejected(tmp_path: Path) -> None:
    bad = pl.DataFrame({"only": ["col"]})
    with pytest.raises(SchemaDriftError):
        write_gold_conversation_scores(bad, gold_root=tmp_path, batch_id="b-bad")


def test_no_partial_final_on_write_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    df = _conv_scores_row()

    def _boom(*_a: object, **_kw: object) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr("polars.DataFrame.write_parquet", _boom)
    with pytest.raises(PipelineError, match="failed to write Gold partition"):
        write_gold_conversation_scores(df, gold_root=tmp_path, batch_id="b-fail")
    # Neither the tmp dir nor the final dir should remain.
    assert not (tmp_path / "conversation_scores" / ".tmp-batch_id=b-fail").exists()
    assert not (tmp_path / "conversation_scores" / "batch_id=b-fail").exists()


def test_pre_existing_final_survives_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Atomicity: a failed re-write must not corrupt the previous good file."""
    good = _conv_scores_row()
    write_gold_conversation_scores(good, gold_root=tmp_path, batch_id="b-keep")
    final_file = tmp_path / "conversation_scores" / "batch_id=b-keep" / "part-0.parquet"
    original_bytes = final_file.read_bytes()

    def _boom(*_a: object, **_kw: object) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr("polars.DataFrame.write_parquet", _boom)
    with pytest.raises(PipelineError):
        write_gold_conversation_scores(good, gold_root=tmp_path, batch_id="b-keep")
    # Pre-existing final file is untouched.
    assert final_file.read_bytes() == original_bytes


# ---------------------------------------------------------------------------
# Insights JSON writer.
# ---------------------------------------------------------------------------


def test_write_insights_creates_summary_json(tmp_path: Path) -> None:
    payload = {
        "ghosting_taxonomy": [{"label": "no_reply", "count": 3}],
        "objections": [],
        "disengagement_moment": [],
        "persona_outcome_correlation": [],
        "generated_at": "2026-04-22T12:00:00+00:00",
    }
    result = write_gold_insights(payload, gold_root=tmp_path, batch_id="b-ins")
    expected = tmp_path / "insights" / "batch_id=b-ins" / "summary.json"
    assert result.insights_path == expected
    assert result.insights_path.is_file()
    loaded = json.loads(result.insights_path.read_text(encoding="utf-8"))
    assert loaded == payload


def test_write_insights_tmp_cleaned(tmp_path: Path) -> None:
    write_gold_insights({"k": 1}, gold_root=tmp_path, batch_id="b-itmp")
    leftover_dir = tmp_path / "insights" / ".tmp-batch_id=b-itmp"
    assert not leftover_dir.exists()


def test_write_insights_idempotent_overwrite(tmp_path: Path) -> None:
    write_gold_insights({"v": 1}, gold_root=tmp_path, batch_id="b-iw")
    result = write_gold_insights({"v": 2}, gold_root=tmp_path, batch_id="b-iw")
    loaded = json.loads(result.insights_path.read_text(encoding="utf-8"))
    assert loaded == {"v": 2}
    final_dir = tmp_path / "insights" / "batch_id=b-iw"
    files = sorted(p.name for p in final_dir.iterdir())
    assert files == ["summary.json"]


def test_write_insights_no_partial_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_replace = os.replace

    def _boom(src: object, dst: object) -> None:
        raise OSError("rename failed")

    monkeypatch.setattr("os.replace", _boom)
    with pytest.raises(PipelineError, match="failed to write Gold insights"):
        write_gold_insights({"v": 1}, gold_root=tmp_path, batch_id="b-ifail")
    # Restore so cleanup assertions work.
    monkeypatch.setattr("os.replace", real_replace)
    assert not (tmp_path / "insights" / "batch_id=b-ifail").exists()
    assert not (tmp_path / "insights" / ".tmp-batch_id=b-ifail").exists()


def test_write_insights_pre_existing_final_survives_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_gold_insights({"v": 1}, gold_root=tmp_path, batch_id="b-ikeep")
    final_file = tmp_path / "insights" / "batch_id=b-ikeep" / "summary.json"
    original_bytes = final_file.read_bytes()

    def _boom(*_a: object, **_kw: object) -> None:
        raise OSError("rename failed")

    monkeypatch.setattr("os.replace", _boom)
    with pytest.raises(PipelineError):
        write_gold_insights({"v": 2}, gold_root=tmp_path, batch_id="b-ikeep")
    assert final_file.read_bytes() == original_bytes
