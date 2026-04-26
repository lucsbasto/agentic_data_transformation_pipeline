"""PERF.5 — agent overhead delta scenario.

Measures the wall_ms cost of wrapping raw pipeline in agent loop
(manifest writes, lock acquire, observer scan, planner/executor).
Each iteration yields two RunRecords: layer="raw" (direct calls) and
layer="agent" (via run_once) for direct comparison.

SLA: overhead ratio ≤ 1.05 (≤5%). Gate: :func:`assert_overhead_sla`.

Manual invocation::

    python scripts/bench_agent.py --scenario overhead \\
        --runs 10 \\
        --out logs/perf/overhead.jsonl
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from pathlib import Path

from pipeline.agent._logging import AgentEventLogger
from pipeline.agent.diagnoser import classify as _classify_exc
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.types import AgentResult, Layer, RunStatus
from pipeline.gold.transform import build_gold_runner
from pipeline.perf.harness import RunRecord, ScenarioContext
from pipeline.silver.normalize import build_silver_runner
from pipeline.state.manifest import ManifestDB


def _build_raw_pipeline_runners(
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    batch_id: str,
) -> dict[Layer, Callable[[], None]]:
    """Build direct pipeline runners (no agent scaffolding)."""

    def _bronze_runner() -> None:
        # Raw bronze processing (no manifest/lock overhead)
        partition_path = bronze_root / f"batch_id={batch_id}" / "part-0.parquet"
        if not partition_path.exists():
            raise FileNotFoundError(partition_path)
        # Actual processing would happen here

    def _silver_runner() -> None:
        # Raw silver processing with build_silver_runner
        runner = build_silver_runner()
        runner(batch_id)

    def _gold_runner() -> None:
        # Raw gold processing with build_gold_runner
        runner = build_gold_runner()
        runner(batch_id)

    return {
        Layer.BRONZE: _bronze_runner,
        Layer.SILVER: _silver_runner,
        Layer.GOLD: _gold_runner,
    }


def _run_raw_pipeline(runners: dict[Layer, Callable[[], None]]) -> float:
    """Execute raw pipeline stages and return wall time in ms."""
    t0 = time.perf_counter_ns()
    for runner in runners.values():
        runner()
    return (time.perf_counter_ns() - t0) / 1_000_000


def _run_agent_pipeline(
    manifest: ManifestDB,
    source_root: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    batch_id: str,
) -> float:
    """Execute via agent.run_once and return wall time in ms."""

    def _runners_for(bid: str) -> dict[Layer, Callable[[], None]]:
        if bid == batch_id:
            return {
                Layer.BRONZE: _build_raw_pipeline_runners(
                    bronze_root, silver_root, gold_root, bid
                )[Layer.BRONZE],
                Layer.SILVER: _build_raw_pipeline_runners(
                    bronze_root, silver_root, gold_root, bid
                )[Layer.SILVER],
                Layer.GOLD: _build_raw_pipeline_runners(
                    bronze_root, silver_root, gold_root, bid
                )[Layer.GOLD],
            }
        return {}

    t0 = time.perf_counter_ns()
    result: AgentResult = run_once(
        manifest=manifest,
        source_root=source_root,
        runners_for=_runners_for,
        classify=lambda exc, layer, bid: _classify_exc(exc, layer=layer, batch_id=bid),
        build_fix=lambda exc, kind, layer, bid: None,
        escalate=lambda exc, kind, layer, bid: None,
        lock=AgentLock(source_root / ".agent.lock"),
        event_logger=AgentEventLogger(log_path=source_root / ".agent_events.jsonl"),
    )
    wall_ns = time.perf_counter_ns() - t0

    if result.status is not RunStatus.COMPLETED:
        raise AssertionError(f"agent pipeline failed with status={result.status!r}")

    return wall_ns / 1_000_000


class OverheadScenario:
    """Agent overhead measurement scenario (PERF.5).

    Measures difference between direct pipeline calls (raw) and
    agent.run_once wrapper (agent). Yields two records per run:
    one for raw, one for agent timing.
    """

    name: str = "overhead"

    def run(self, ctx: ScenarioContext) -> Iterator[RunRecord]:
        """Run paired measurements and yield raw/agent records."""

        # Create fixture paths
        work_dir = ctx.work_root / "overhead_fixture"
        work_dir.mkdir(parents=True, exist_ok=True)

        source_root = work_dir / "source"
        bronze_root = work_dir / "bronze"
        silver_root = work_dir / "silver"
        gold_root = work_dir / "gold"
        manifest_path = work_dir / "manifest.sqlite"

        source_root.mkdir(exist_ok=True)
        bronze_root.mkdir(exist_ok=True)
        silver_root.mkdir(exist_ok=True)
        gold_root.mkdir(exist_ok=True)

        # Initialize manifest schema only — no pre-inserted batches
        db = ManifestDB(manifest_path).open()
        batch_id = "overhead_test_batch"
        db.close()

        for i in range(ctx.runs):
            # Reload fresh manifest for each run
            manifest = ManifestDB(manifest_path).open()

            try:
                # Run raw pipeline (direct calls)
                raw_runners = _build_raw_pipeline_runners(
                    bronze_root, silver_root, gold_root, batch_id
                )
                raw_ms = _run_raw_pipeline(raw_runners)

                # Run agent-wrapped pipeline
                agent_ms = _run_agent_pipeline(
                    manifest=manifest,
                    source_root=source_root,
                    bronze_root=bronze_root,
                    silver_root=silver_root,
                    gold_root=gold_root,
                    batch_id=batch_id,
                )
            finally:
                manifest.close()

            # Yield both measurements with same run_idx for pairing
            yield RunRecord(
                scenario="overhead",
                layer="raw",
                run_idx=i,
                wall_ms=raw_ms,
                peak_rss_mb=None,
                batch_id=batch_id,
                ts=ctx.clock(),
            )

            yield RunRecord(
                scenario="overhead",
                layer="agent",
                run_idx=i,
                wall_ms=agent_ms,
                peak_rss_mb=None,
                batch_id=batch_id,
                ts=ctx.clock(),
            )


SCENARIO: OverheadScenario = OverheadScenario()
"""Module-level constant consumed by discover_scenarios()."""


def assert_overhead_sla(
    records: Iterator[RunRecord], max_overhead_pct: float = 5.0
) -> None:
    """Assert overhead ratio <= max_overhead_pct (default 5%).

    Groups records by run_idx and verifies (agent - raw) / raw <= threshold.
    """
    by_idx_layer: dict[tuple[int, str], RunRecord] = {}
    for rec in records:
        key = (rec.run_idx, rec.layer or "")
        if key in by_idx_layer:
            raise ValueError(f"duplicate record for run_idx={rec.run_idx}, layer={rec.layer}")
        by_idx_layer[key] = rec

    max_ratio = 1.0 + (max_overhead_pct / 100.0)  # e.g., 1.05 for 5%

    for run_idx in {idx for idx, _ in by_idx_layer}:
        raw_rec = by_idx_layer.get((run_idx, "raw"))
        agent_rec = by_idx_layer.get((run_idx, "agent"))

        if not raw_rec or not agent_rec:
            continue  # Skip incomplete pairs

        if raw_rec.wall_ms <= 0:
            raise ValueError(f"raw measurement <= 0 for run_idx={run_idx}: {raw_rec.wall_ms}")

        ratio = agent_rec.wall_ms / raw_rec.wall_ms
        if ratio > max_ratio:
            raise AssertionError(
                f"overhead SLA violated: run_idx={run_idx}, "
                f"raw={raw_rec.wall_ms:.1f}ms, agent={agent_rec.wall_ms:.1f}ms, "
                f"ratio={ratio:.3f} > {max_ratio:.3f} ({max_overhead_pct}% limit)"
            )
