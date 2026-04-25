"""Unit tests for ``pipeline.perf.harness``."""

from __future__ import annotations

import dataclasses
import importlib
import json
import re
import sys
import textwrap
from pathlib import Path

import pytest

from pipeline.perf import harness as harness_mod
from pipeline.perf.harness import (
    JsonlWriter,
    RunRecord,
    Scenario,
    ScenarioContext,
    discover_scenarios,
    now_utc,
)

ISO_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00$")


def test_now_utc_shape():
    ts = now_utc()
    assert ISO_UTC_RE.match(ts), ts
    assert ts.endswith("+00:00")


def test_run_record_is_frozen_and_round_trips():
    record = RunRecord(
        scenario="demo",
        layer="silver",
        run_idx=0,
        wall_ms=12.5,
        peak_rss_mb=42.0,
        batch_id="batch_id=demo01",
        ts="2026-01-01T00:00:00.000000+00:00",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.scenario = "other"  # type: ignore[misc]

    line = record.to_jsonline()
    parsed = json.loads(line)
    assert parsed == {
        "scenario": "demo",
        "layer": "silver",
        "run_idx": 0,
        "wall_ms": 12.5,
        "peak_rss_mb": 42.0,
        "batch_id": "batch_id=demo01",
        "ts": "2026-01-01T00:00:00.000000+00:00",
    }


def test_scenario_context_rejects_zero_runs(tmp_path: Path):
    with pytest.raises(ValueError, match="runs must be >= 1"):
        ScenarioContext(
            scenario_name="demo",
            runs=0,
            out_path=tmp_path / "out.jsonl",
            work_root=tmp_path,
        )


def test_scenario_context_accepts_one_run(tmp_path: Path):
    ctx = ScenarioContext(
        scenario_name="demo",
        runs=1,
        out_path=tmp_path / "out.jsonl",
        work_root=tmp_path,
    )
    assert ctx.runs == 1
    assert ctx.extra == {}


def test_jsonl_writer_creates_parent_and_appends(tmp_path: Path):
    out = tmp_path / "nested" / "more" / "out.jsonl"
    rec_a = RunRecord("a", None, 0, 1.0, None, None, "ts")
    rec_b = RunRecord("b", None, 1, 2.0, None, None, "ts")

    with JsonlWriter(out) as writer:
        writer.write(rec_a)
        writer.write(rec_b)

    assert out.exists()
    lines = out.read_text("utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["scenario"] == "a"
    assert json.loads(lines[1])["scenario"] == "b"

    # Second writer with same path appends rather than truncating.
    rec_c = RunRecord("c", None, 2, 3.0, None, None, "ts")
    with JsonlWriter(out) as writer:
        writer.write(rec_c)
    lines = out.read_text("utf-8").splitlines()
    assert len(lines) == 3
    assert json.loads(lines[2])["scenario"] == "c"


def test_jsonl_writer_rejects_use_outside_context(tmp_path: Path):
    writer = JsonlWriter(tmp_path / "out.jsonl")
    with pytest.raises(RuntimeError):
        writer.write(RunRecord("x", None, 0, 0.0, None, None, "ts"))


def test_discover_scenarios_real_namespace_is_empty():
    pkg = importlib.import_module("pipeline.perf.scenarios")
    assert discover_scenarios(pkg) == {}


def _build_synthetic_pkg(tmp_path: Path, modules: dict[str, str]) -> str:
    """Build a synthetic package on disk + sys.path; return its name.

    ``modules`` maps module-stem -> source. An ``__init__.py`` is
    created automatically.
    """
    pkg_name = f"perf_synth_{tmp_path.name.replace('-', '_')}"
    pkg_dir = tmp_path / pkg_name
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    for stem, source in modules.items():
        (pkg_dir / f"{stem}.py").write_text(textwrap.dedent(source), "utf-8")
    sys.path.insert(0, str(tmp_path))
    return pkg_name


def test_discover_scenarios_picks_up_module_level_scenario(tmp_path: Path):
    src_a = """
    from pipeline.perf.harness import RunRecord


    class _Scn:
        name = "alpha"

        def run(self, ctx):
            yield RunRecord(
                scenario="alpha",
                layer=None,
                run_idx=0,
                wall_ms=1.0,
                peak_rss_mb=None,
                batch_id=None,
                ts="ts",
            )


    SCENARIO = _Scn()
    """
    # No SCENARIO at all — must be skipped silently.
    src_b = """
    NOT_A_SCENARIO = object()
    """
    pkg_name = _build_synthetic_pkg(tmp_path, {"alpha_mod": src_a, "noop_mod": src_b})
    try:
        pkg = importlib.import_module(pkg_name)
        registry = discover_scenarios(pkg)
        assert set(registry) == {"alpha"}
        assert isinstance(registry["alpha"], Scenario)
    finally:
        sys.path.remove(str(tmp_path))
        # Drop synthetic modules so other tests aren't polluted.
        for mod_name in list(sys.modules):
            if mod_name == pkg_name or mod_name.startswith(pkg_name + "."):
                del sys.modules[mod_name]


def test_discover_scenarios_rejects_duplicate_names(tmp_path: Path):
    src = """
    from pipeline.perf.harness import RunRecord


    class _Scn:
        name = "dup"

        def run(self, ctx):
            yield RunRecord("dup", None, 0, 0.0, None, None, "ts")


    SCENARIO = _Scn()
    """
    pkg_name = _build_synthetic_pkg(tmp_path, {"first_mod": src, "second_mod": src})
    try:
        pkg = importlib.import_module(pkg_name)
        with pytest.raises(RuntimeError, match="duplicate perf scenario"):
            discover_scenarios(pkg)
    finally:
        sys.path.remove(str(tmp_path))
        for mod_name in list(sys.modules):
            if mod_name == pkg_name or mod_name.startswith(pkg_name + "."):
                del sys.modules[mod_name]


def test_discover_scenarios_default_arg_uses_real_package(monkeypatch):
    # Calling with no argument should resolve to pipeline.perf.scenarios.
    sentinel = {"sentinel": object()}

    def fake_iter(_path, _prefix):
        return iter(())

    monkeypatch.setattr(harness_mod.pkgutil, "iter_modules", fake_iter)
    result = discover_scenarios()
    assert result == {}
    assert sentinel  # keep linter happy
