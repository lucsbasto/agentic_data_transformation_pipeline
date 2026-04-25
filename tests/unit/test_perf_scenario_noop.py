"""Unit tests for PERF.2 — noop scenario.

Covers:
* :func:`~pipeline.perf.harness.discover_scenarios` registry.
* :func:`~pipeline.perf.scenarios.noop._build_green_manifest` fixture.
* :class:`~pipeline.perf.scenarios.noop.NoopScenario`.run yield count
  and record shape.
* JSONL roundtrip via :class:`~pipeline.perf.harness.JsonlWriter`.
* :func:`~pipeline.perf.scenarios.noop.assert_noop_sla` helper.
* Performance smoke: all wall_ms < 5000 ms (CI-safe; strict 1000 ms
  gate lives in ``tests/integration/test_perf_noop_sla.py``).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.perf.harness import (
    JsonlWriter,
    RunRecord,
    ScenarioContext,
    discover_scenarios,
    now_utc,
)
from pipeline.perf.scenarios.noop import (
    NoopScenario,
    _build_green_manifest,
    assert_noop_sla,
)
from pipeline.state.manifest import ManifestDB

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_discover_scenarios_includes_noop() -> None:
    registry = discover_scenarios()
    assert "noop" in registry
    scenario = registry["noop"]
    assert isinstance(scenario, NoopScenario)


# ---------------------------------------------------------------------------
# Fixture builder
# ---------------------------------------------------------------------------


def test_build_green_manifest_creates_manifest_db(tmp_path: Path) -> None:
    paths = _build_green_manifest(tmp_path)

    assert paths["manifest_path"].exists()
    assert paths["source_root"].is_dir()

    # The manifest must open cleanly and report zero actionable batches.
    db = ManifestDB(paths["manifest_path"]).open()
    try:
        conn = db._require_conn()
        count = conn.execute("SELECT COUNT(*) FROM batches;").fetchone()[0]
        assert count == 0
    finally:
        db.close()


# ---------------------------------------------------------------------------
# NoopScenario.run — yield count and record shape
# ---------------------------------------------------------------------------


def _make_ctx(tmp_path: Path, *, runs: int) -> ScenarioContext:
    return ScenarioContext(
        scenario_name="noop",
        runs=runs,
        out_path=tmp_path / "out.jsonl",
        work_root=tmp_path / "work",
        clock=lambda: "2026-01-01T00:00:00.000000+00:00",
    )


def test_noop_scenario_yields_correct_count(tmp_path: Path) -> None:
    ctx = _make_ctx(tmp_path, runs=3)
    records = list(NoopScenario().run(ctx))
    assert len(records) == 3


def test_noop_scenario_run_idx_is_monotone(tmp_path: Path) -> None:
    ctx = _make_ctx(tmp_path, runs=4)
    records = list(NoopScenario().run(ctx))
    assert [r.run_idx for r in records] == list(range(4))


def test_noop_scenario_record_fields(tmp_path: Path) -> None:
    ctx = _make_ctx(tmp_path, runs=1)
    (rec,) = NoopScenario().run(ctx)
    assert rec.scenario == "noop"
    assert rec.layer is None
    assert rec.batch_id is None
    assert rec.peak_rss_mb is None
    assert rec.wall_ms > 0


# ---------------------------------------------------------------------------
# JSONL roundtrip via JsonlWriter
# ---------------------------------------------------------------------------


def test_noop_scenario_jsonl_roundtrip(tmp_path: Path) -> None:
    ctx = _make_ctx(tmp_path, runs=3)
    out = tmp_path / "noop.jsonl"

    with JsonlWriter(out) as writer:
        for rec in NoopScenario().run(ctx):
            writer.write(rec)

    lines = out.read_text().splitlines()
    assert len(lines) == 3
    for i, line in enumerate(lines):
        obj = json.loads(line)
        assert obj["scenario"] == "noop"
        assert obj["run_idx"] == i
        assert obj["wall_ms"] > 0


# ---------------------------------------------------------------------------
# assert_noop_sla helper
# ---------------------------------------------------------------------------


def _make_record(run_idx: int, wall_ms: float) -> RunRecord:
    return RunRecord(
        scenario="noop",
        layer=None,
        run_idx=run_idx,
        wall_ms=wall_ms,
        peak_rss_mb=None,
        batch_id=None,
        ts=now_utc(),
    )


def test_assert_noop_sla_passes_on_fast_records() -> None:
    records = [_make_record(i, 10.0) for i in range(5)]
    assert_noop_sla(records, threshold_ms=1000.0)  # must not raise


def test_assert_noop_sla_raises_on_violation() -> None:
    records = [_make_record(0, 500.0), _make_record(1, 1500.0)]
    with pytest.raises(AssertionError, match="NFR-01 violated"):
        assert_noop_sla(records, threshold_ms=1000.0)


def test_assert_noop_sla_raises_on_exact_threshold() -> None:
    # boundary: >= threshold should fail
    records = [_make_record(0, 1000.0)]
    with pytest.raises(AssertionError):
        assert_noop_sla(records, threshold_ms=1000.0)


# ---------------------------------------------------------------------------
# Performance smoke (CI-safe 5 s gate)
# ---------------------------------------------------------------------------


def test_noop_scenario_perf_smoke(tmp_path: Path) -> None:
    """All wall_ms must be < 5000 ms — relaxed gate for CI noise.

    The strict 1000 ms NFR-01 gate lives in the integration suite.
    """
    ctx = _make_ctx(tmp_path, runs=2)
    records = list(NoopScenario().run(ctx))
    assert_noop_sla(records, threshold_ms=5000.0)
