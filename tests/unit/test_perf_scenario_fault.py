"""Unit tests for PERF.6 — fault-injection round-trip baseline scenario.

Covers:
* :func:`~pipeline.perf.harness.discover_scenarios` registry — ``"fault"``
  is present.
* Per-kind injection effect — each ``inject_*`` helper produces the
  expected mutation (mirroring assertions from
  ``tests/integration/test_inject_fault.py``).
* Happy path with monkeypatched
  :func:`~pipeline.agent.loop.run_once` returning a green
  :class:`~pipeline.agent.types.AgentResult` instantly: ``runs=2``
  yields exactly ``4 x 2 = 8`` :class:`~pipeline.perf.harness.RunRecord`
  objects whose ``layer`` values cover all four
  :class:`~pipeline.agent.types.ErrorKind` values.
* :func:`~pipeline.perf.scenarios.fault.summarize_fault_baselines`
  correctness — synthetic records, all four layers present, p50/p95
  sane, small-N (count=1) returns without raising.
* JSONL roundtrip via :class:`~pipeline.perf.harness.JsonlWriter`.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from pipeline.agent.types import AgentResult, ErrorKind, RunStatus
from pipeline.perf.harness import (
    JsonlWriter,
    RunRecord,
    ScenarioContext,
    discover_scenarios,
)
from pipeline.perf.scenarios.fault import (
    SCENARIO,
    FaultScenario,
    _inject,
    inject_out_of_range,
    inject_partition_missing,
    inject_regex_break,
    inject_schema_drift,
    summarize_fault_baselines,
)
from pipeline.schemas.bronze import SOURCE_COLUMNS

# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------

_FIXED_TS = "2026-04-25T00:00:00.000000+00:00"


def _make_ctx(tmp_path: Path, *, runs: int) -> ScenarioContext:
    return ScenarioContext(
        scenario_name="fault",
        runs=runs,
        out_path=tmp_path / "out.jsonl",
        work_root=tmp_path / "work",
        clock=lambda: _FIXED_TS,
    )


def _write_source_parquet(path: Path) -> None:
    """Write a minimal valid source parquet to *path* for injection tests."""
    rows: dict[str, list[str]] = {col: ["x"] for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"]
    rows["direction"] = ["inbound"]
    rows["message_type"] = ["text"]
    rows["status"] = ["delivered"]
    rows["message_body"] = ["original mensagem"]
    df = pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _make_record(layer: str, run_idx: int, wall_ms: float) -> RunRecord:
    return RunRecord(
        scenario="fault",
        layer=layer,
        run_idx=run_idx,
        wall_ms=wall_ms,
        peak_rss_mb=None,
        batch_id="abc123",
        ts=_FIXED_TS,
    )


def _green_agent_result() -> AgentResult:
    return AgentResult(
        agent_run_id="stub-run-id",
        batches_processed=1,
        failures_recovered=1,
        escalations=0,
        status=RunStatus.COMPLETED,
    )


# ---------------------------------------------------------------------------
# Registry.
# ---------------------------------------------------------------------------


def test_fault_scenario_in_registry() -> None:
    """``discover_scenarios()["fault"]`` must resolve to SCENARIO."""
    registry = discover_scenarios()
    assert "fault" in registry
    assert registry["fault"] is SCENARIO


# ---------------------------------------------------------------------------
# Per-kind injection effect.
# ---------------------------------------------------------------------------


def test_inject_schema_drift_adds_injected_col(tmp_path: Path) -> None:
    parquet = tmp_path / "part.parquet"
    _write_source_parquet(parquet)
    inject_schema_drift(parquet)
    df = pl.read_parquet(parquet)
    assert "injected_col" in df.columns


def test_inject_regex_break_replaces_first_message_body(tmp_path: Path) -> None:
    parquet = tmp_path / "raw.parquet"
    _write_source_parquet(parquet)
    inject_regex_break(parquet)
    df = pl.read_parquet(parquet)
    assert df["message_body"][0] == "❌ R$ 1.500,00"


def test_inject_partition_missing_removes_file(tmp_path: Path) -> None:
    parquet = tmp_path / "part.parquet"
    _write_source_parquet(parquet)
    inject_partition_missing(parquet)
    assert not parquet.exists()


def test_inject_out_of_range_appends_negative_value_row(tmp_path: Path) -> None:
    parquet = tmp_path / "raw.parquet"
    _write_source_parquet(parquet)
    inject_out_of_range(parquet)
    df = pl.read_parquet(parquet)
    assert df.height == 2
    assert df["message_body"][1] == "valor_pago_atual_brl=-999"


def test_inject_dispatcher_covers_all_kinds(tmp_path: Path) -> None:
    """_inject routes to a helper for every measurable ErrorKind."""
    for kind in (
        ErrorKind.SCHEMA_DRIFT,
        ErrorKind.REGEX_BREAK,
        ErrorKind.PARTITION_MISSING,
        ErrorKind.OUT_OF_RANGE,
    ):
        parquet = tmp_path / f"{kind.value}.parquet"
        _write_source_parquet(parquet)
        # Should not raise for any of the four kinds.
        _inject(kind, parquet)


# ---------------------------------------------------------------------------
# Happy path with monkeypatched run_once.
# ---------------------------------------------------------------------------


def test_fault_scenario_yields_8_records_for_runs_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``runs=2`` must yield exactly ``4 x 2 = 8`` RunRecords."""
    monkeypatch.setattr(
        "pipeline.perf.scenarios.fault.run_once",
        lambda **_kw: _green_agent_result(),
    )

    ctx = _make_ctx(tmp_path, runs=2)
    records = list(FaultScenario().run(ctx))

    assert len(records) == 8


def test_fault_scenario_layer_values_cover_all_4_kinds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All four ErrorKind values must appear as ``layer`` in the records."""
    monkeypatch.setattr(
        "pipeline.perf.scenarios.fault.run_once",
        lambda **_kw: _green_agent_result(),
    )

    ctx = _make_ctx(tmp_path, runs=1)
    records = list(FaultScenario().run(ctx))

    layers = {r.layer for r in records}
    expected = {
        k.value
        for k in (
            ErrorKind.SCHEMA_DRIFT,
            ErrorKind.REGEX_BREAK,
            ErrorKind.PARTITION_MISSING,
            ErrorKind.OUT_OF_RANGE,
        )
    }
    assert layers == expected


def test_fault_scenario_record_fields_match_expected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RunRecord fields are correctly populated."""
    monkeypatch.setattr(
        "pipeline.perf.scenarios.fault.run_once",
        lambda **_kw: _green_agent_result(),
    )

    ctx = _make_ctx(tmp_path, runs=1)
    records = list(FaultScenario().run(ctx))

    for rec in records:
        assert rec.scenario == "fault"
        assert rec.run_idx == 0
        assert rec.ts == _FIXED_TS
        assert rec.peak_rss_mb is None
        assert isinstance(rec.wall_ms, float)
        assert rec.batch_id is not None


def test_fault_scenario_run_idx_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """For runs=3, each kind should have run_idx values 0, 1, 2."""
    monkeypatch.setattr(
        "pipeline.perf.scenarios.fault.run_once",
        lambda **_kw: _green_agent_result(),
    )

    ctx = _make_ctx(tmp_path, runs=3)
    records = list(FaultScenario().run(ctx))

    assert len(records) == 4 * 3
    for kind_value in (
        k.value
        for k in (
            ErrorKind.SCHEMA_DRIFT,
            ErrorKind.REGEX_BREAK,
            ErrorKind.PARTITION_MISSING,
            ErrorKind.OUT_OF_RANGE,
        )
    ):
        kind_records = [r for r in records if r.layer == kind_value]
        assert [r.run_idx for r in kind_records] == [0, 1, 2]


# ---------------------------------------------------------------------------
# summarize_fault_baselines.
# ---------------------------------------------------------------------------


def test_summarize_returns_all_4_layers() -> None:
    records = [
        _make_record("schema_drift", 0, 100.0),
        _make_record("schema_drift", 1, 200.0),
        _make_record("regex_break", 0, 150.0),
        _make_record("regex_break", 1, 250.0),
        _make_record("partition_missing", 0, 80.0),
        _make_record("partition_missing", 1, 120.0),
        _make_record("out_of_range", 0, 90.0),
        _make_record("out_of_range", 1, 110.0),
    ]
    summary = summarize_fault_baselines(records)
    assert set(summary) == {
        "schema_drift",
        "regex_break",
        "partition_missing",
        "out_of_range",
    }


def test_summarize_p50_p95_sane() -> None:
    # 2 values: [100, 200]; p50 should be between them.
    records = [
        _make_record("schema_drift", 0, 100.0),
        _make_record("schema_drift", 1, 200.0),
    ]
    summary = summarize_fault_baselines(records)
    sd = summary["schema_drift"]
    assert sd["p50_ms"] <= sd["p95_ms"]
    assert sd["max_ms"] == 200.0
    assert sd["count"] == 2.0


def test_summarize_small_n_count_1_no_exception() -> None:
    """count=1 must not raise and must return p50 == p95 == the single value."""
    records = [_make_record("partition_missing", 0, 42.0)]
    summary = summarize_fault_baselines(records)
    pm = summary["partition_missing"]
    assert pm["p50_ms"] == 42.0
    assert pm["p95_ms"] == 42.0
    assert pm["max_ms"] == 42.0
    assert pm["count"] == 1.0


def test_summarize_max_is_correct() -> None:
    records = [
        _make_record("out_of_range", 0, 10.0),
        _make_record("out_of_range", 1, 30.0),
        _make_record("out_of_range", 2, 20.0),
    ]
    summary = summarize_fault_baselines(records)
    assert summary["out_of_range"]["max_ms"] == 30.0


# ---------------------------------------------------------------------------
# JSONL roundtrip via JsonlWriter.
# ---------------------------------------------------------------------------


def test_jsonl_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Records written through JsonlWriter round-trip to identical values."""
    monkeypatch.setattr(
        "pipeline.perf.scenarios.fault.run_once",
        lambda **_kw: _green_agent_result(),
    )

    ctx = _make_ctx(tmp_path, runs=1)
    records = list(FaultScenario().run(ctx))
    assert len(records) == 4  # 1 run x 4 kinds

    out_path = tmp_path / "perf.jsonl"
    with JsonlWriter(out_path) as writer:
        for rec in records:
            writer.write(rec)

    lines = out_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4

    parsed = [json.loads(line) for line in lines]
    for original, roundtripped in zip(records, parsed, strict=True):
        assert roundtripped["scenario"] == original.scenario
        assert roundtripped["layer"] == original.layer
        assert roundtripped["run_idx"] == original.run_idx
        assert roundtripped["wall_ms"] == original.wall_ms
