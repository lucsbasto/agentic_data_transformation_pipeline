"""Integration test — PERF.2 / NFR-01 strict SLA gate.

Runs :class:`~pipeline.perf.scenarios.noop.NoopScenario` against a
real filesystem fixture and asserts that every run completes in under
1 000 ms (sub-second wall time).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.perf.harness import ScenarioContext
from pipeline.perf.scenarios.noop import NoopScenario, assert_noop_sla


@pytest.mark.integration
def test_noop_sla_strict(tmp_path: Path) -> None:
    """NFR-01: no-op agent run-once must complete in < 1000 ms."""
    ctx = ScenarioContext(
        scenario_name="noop",
        runs=3,
        out_path=tmp_path / "noop_sla.jsonl",
        work_root=tmp_path / "work",
    )
    records = list(NoopScenario().run(ctx))
    assert len(records) == 3
    assert_noop_sla(records, threshold_ms=1000.0)
