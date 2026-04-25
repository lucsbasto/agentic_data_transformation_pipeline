"""PERF.2 / NFR-01 — no-op agent run-once scenario.

Measures the wall-clock cost of ``agent.run_once`` when there is
nothing to do (empty manifest, empty source root).  The goal stated
in NFR-01 is sub-second wall time for this case.

Public surface
--------------
* :class:`NoopScenario` — :class:`~pipeline.perf.harness.Scenario`
  implementation; module-level constant :data:`SCENARIO`.
* :func:`assert_noop_sla` — SLA gate for test suites; raises
  :class:`AssertionError` on the first violation.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from pathlib import Path

from pipeline.agent._logging import AgentEventLogger
from pipeline.agent.executor import DEFAULT_RETRY_BUDGET
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.types import ErrorKind
from pipeline.perf.harness import RunRecord, ScenarioContext
from pipeline.state.manifest import ManifestDB

# ---------------------------------------------------------------------------
# Fixture builder
# ---------------------------------------------------------------------------


def _build_green_manifest(work_root: Path) -> dict[str, Path]:
    """Create a minimal no-op fixture under *work_root*.

    Returns a dict of named paths required by :func:`run_once`:

    ``manifest_path``
        SQLite file initialised via :class:`ManifestDB` (schema only,
        zero batches — empty manifest means nothing to process).
    ``source_root``
        Empty directory; ``observer.scan`` will find no parquet files.
    ``bronze_root``, ``silver_root``, ``gold_root``
        Empty directories (required to exist for path hygiene).
    """
    manifest_path = work_root / "manifest.sqlite"
    source_root = work_root / "source"
    bronze_root = work_root / "bronze"
    silver_root = work_root / "silver"
    gold_root = work_root / "gold"

    for d in (source_root, bronze_root, silver_root, gold_root):
        d.mkdir(parents=True, exist_ok=True)

    # Initialise schema only — no batches inserted, so run_once sees
    # nothing to do.
    db = ManifestDB(manifest_path).open()
    db.close()

    return {
        "manifest_path": manifest_path,
        "source_root": source_root,
        "bronze_root": bronze_root,
        "silver_root": silver_root,
        "gold_root": gold_root,
    }


# ---------------------------------------------------------------------------
# Single-run measurement
# ---------------------------------------------------------------------------


def _measure_run_once(paths: dict[str, Path]) -> float:
    """Call ``run_once`` with the fixture paths and return wall_ms.

    Opens its own :class:`ManifestDB` connection for the duration of
    the call, closing it immediately afterwards.  Re-raises any
    exception so callers see real failures.
    """
    manifest = ManifestDB(paths["manifest_path"]).open()
    try:
        t0 = time.perf_counter_ns()
        run_once(
            manifest=manifest,
            source_root=paths["source_root"],
            runners_for=lambda _batch_id: {},
            classify=lambda exc, layer, batch_id: ErrorKind.UNKNOWN,
            build_fix=lambda exc, kind, layer, batch_id: None,
            escalate=lambda exc, kind, layer, batch_id: None,
            lock=AgentLock(paths["source_root"] / ".agent.lock"),
            retry_budget=DEFAULT_RETRY_BUDGET,
            event_logger=AgentEventLogger(log_path=paths["source_root"] / ".agent_events.jsonl"),
        )
        t1 = time.perf_counter_ns()
    finally:
        manifest.close()

    return (t1 - t0) / 1_000_000


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------


class NoopScenario:
    """No-op agent run-once scenario (PERF.2 / NFR-01).

    Builds a green-manifest fixture once per :meth:`run` invocation,
    then measures :func:`run_once` ``ctx.runs`` times, yielding one
    :class:`~pipeline.perf.harness.RunRecord` per iteration.  The
    scenario is *observation-only* and never raises on SLA breach —
    use :func:`assert_noop_sla` for enforcement.
    """

    name: str = "noop"

    def run(self, ctx: ScenarioContext) -> Iterator[RunRecord]:
        fixture_root = ctx.work_root / "noop_fixture"
        fixture_root.mkdir(parents=True, exist_ok=True)
        paths = _build_green_manifest(fixture_root)

        for i in range(ctx.runs):
            ms = _measure_run_once(paths)
            yield RunRecord(
                scenario="noop",
                layer=None,
                run_idx=i,
                wall_ms=ms,
                peak_rss_mb=None,
                batch_id=None,
                ts=ctx.clock(),
            )


SCENARIO: NoopScenario = NoopScenario()
"""Module-level constant consumed by :func:`~pipeline.perf.harness.discover_scenarios`."""


# ---------------------------------------------------------------------------
# SLA helper
# ---------------------------------------------------------------------------


def assert_noop_sla(records: Iterable[RunRecord], threshold_ms: float = 1000.0) -> None:
    """Assert NFR-01: every record must have ``wall_ms < threshold_ms``.

    Raises :class:`AssertionError` on the first violation so the
    failing run index and measured time are visible in the test output.

    Parameters
    ----------
    records:
        Iterable of :class:`~pipeline.perf.harness.RunRecord` objects
        produced by :class:`NoopScenario`.
    threshold_ms:
        SLA ceiling in milliseconds (default ``1000.0`` = 1 second).
    """
    for r in records:
        if r.wall_ms >= threshold_ms:
            raise AssertionError(
                f"NFR-01 violated: run_idx={r.run_idx} wall_ms={r.wall_ms} >= {threshold_ms}"
            )
