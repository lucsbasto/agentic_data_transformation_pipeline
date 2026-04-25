"""Unit tests for ``scripts/bench_agent.py`` (Click CLI)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipeline.perf.harness import RunRecord, Scenario

REPO_ROOT = Path(__file__).resolve().parents[2]
BENCH_PATH = REPO_ROOT / "scripts" / "bench_agent.py"


def _load_bench_module():
    spec = importlib.util.spec_from_file_location("bench_agent", BENCH_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["bench_agent"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def bench_module():
    return _load_bench_module()


class _StubScenario:
    """Yields a fixed pair of RunRecords for happy-path testing."""

    name = "stub"

    def run(self, ctx):
        yield RunRecord(
            scenario="stub",
            layer="silver",
            run_idx=0,
            wall_ms=1.5,
            peak_rss_mb=None,
            batch_id=None,
            ts="2026-01-01T00:00:00.000000+00:00",
        )
        yield RunRecord(
            scenario="stub",
            layer="silver",
            run_idx=1,
            wall_ms=2.5,
            peak_rss_mb=None,
            batch_id=None,
            ts="2026-01-01T00:00:00.000001+00:00",
        )


def test_help_exits_zero(bench_module):
    runner = CliRunner()
    result = runner.invoke(bench_module.main, ["--help"])
    assert result.exit_code == 0
    assert "--scenario" in result.output


def test_unknown_scenario_exits_two(bench_module, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(bench_module, "discover_scenarios", lambda: {})
    runner = CliRunner()
    result = runner.invoke(
        bench_module.main,
        ["--scenario", "ghost", "--out", str(tmp_path / "out.jsonl")],
    )
    assert result.exit_code == 2
    assert "unknown" in result.stderr.lower()


def test_malformed_opt_exits_two(bench_module, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(bench_module, "discover_scenarios", lambda: {"stub": _StubScenario()})
    runner = CliRunner()
    result = runner.invoke(
        bench_module.main,
        [
            "--scenario",
            "stub",
            "--out",
            str(tmp_path / "out.jsonl"),
            "--opt",
            "no_equals_sign",
        ],
    )
    assert result.exit_code == 2
    assert "malformed" in result.stderr.lower()


def test_happy_path_writes_jsonl(bench_module, tmp_path: Path, monkeypatch):
    stub = _StubScenario()
    assert isinstance(stub, Scenario)
    monkeypatch.setattr(bench_module, "discover_scenarios", lambda: {"stub": stub})
    out = tmp_path / "perf" / "out.jsonl"
    runner = CliRunner()
    result = runner.invoke(
        bench_module.main,
        [
            "--scenario",
            "stub",
            "--runs",
            "2",
            "--out",
            str(out),
            "--opt",
            "batch_id=demo01",
            "--opt",
            "layer=silver",
            "--work-root",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "wrote 2 records" in result.output
    assert out.exists()
    lines = out.read_text("utf-8").splitlines()
    assert len(lines) == 2
    payloads = [json.loads(line) for line in lines]
    assert payloads[0]["run_idx"] == 0
    assert payloads[1]["run_idx"] == 1
    assert payloads[0]["scenario"] == "stub"


def test_runs_must_be_positive(bench_module, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(bench_module, "discover_scenarios", lambda: {"stub": _StubScenario()})
    runner = CliRunner()
    result = runner.invoke(
        bench_module.main,
        [
            "--scenario",
            "stub",
            "--runs",
            "0",
            "--out",
            str(tmp_path / "out.jsonl"),
        ],
    )
    assert result.exit_code == 2
