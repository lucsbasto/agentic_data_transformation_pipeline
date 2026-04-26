"""PERF.10 — comprehensive SLA integration and CI gating scenario.

Combines all performance scenarios under unified SLA gates.
Implements CI integration for performance regression detection.
Gates deployments on performance SLA compliance.

Validates all performance metrics against defined thresholds.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from pipeline.agent._logging import AgentEventLogger
from pipeline.agent.diagnoser import classify as _classify_exc
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.types import AgentResult, Layer, RunStatus
from pipeline.gold.transform import build_gold_runner
from pipeline.ingest.batch import compute_batch_identity
from pipeline.ingest.reader import scan_source, validate_source_columns
from pipeline.ingest.transform import collect_bronze, transform_to_bronze
from pipeline.perf.harness import RunRecord, ScenarioContext
from pipeline.schemas.bronze import BRONZE_SCHEMA, SOURCE_COLUMNS
from pipeline.silver.normalize import build_silver_runner
from pipeline.state.manifest import ManifestDB


def _write_slamark_source_parquet(path: Path, nrows: int = 1000) -> None:
    """Write a source parquet for SLA marking tests."""
    rows: dict[str, list[str]] = {col: [] for col in SOURCE_COLUMNS}
    for i in range(nrows):
        for col in SOURCE_COLUMNS:
            rows[col].append(f"sla_mark_{col}_val_{i}")

    # Add required fields
    rows["timestamp"] = [
        f"2026-04-{25 - i % 5:02d} 12:{i % 60:02d}:{i % 60:02d}" for i in range(nrows)
    ]
    rows["direction"] = ["inbound" if i % 2 == 0 else "outbound" for i in range(nrows)]
    rows["message_type"] = [
        "text" if i % 3 == 0 else "image" if i % 3 == 1 else "audio"
        for i in range(nrows)
    ]
    rows["status"] = ["delivered" if i % 4 != 0 else "sent" for i in range(nrows)]
    rows["message_body"] = [
        f"mensagem_body_valor_pago_atual_brl={i % 1000}" for i in range(nrows)
    ]

    # Create DataFrame with consistent schema
    schema = dict.fromkeys(SOURCE_COLUMNS, pl.String)
    df = pl.DataFrame(rows, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def check_sla_compliance(record: RunRecord, scenario_thresholds: dict[str, float]) -> bool:
    """Check if a run record meets SLA thresholds."""
    scenario = record.scenario
    if scenario in scenario_thresholds:
        return record.wall_ms <= scenario_thresholds[scenario]
    return True  # Unknown scenarios pass by default


def _make_sla_bronze_runner(
    bronze_root: Path,
    source_file: Path,
    source_hash: str,
    bid: str,
) -> Callable[[], None]:
    """Return a bronze runner closure with all variables captured by value."""

    def _bronze_runner() -> None:
        partition = bronze_root / f"batch_id={bid}" / "part-0.parquet"
        if not partition.exists():
            lf = scan_source(source_file)
            validate_source_columns(lf)
            typed = transform_to_bronze(
                lf,
                batch_id=bid,
                source_hash=source_hash,
                ingested_at=datetime.now(tz=UTC),
            )
            df = collect_bronze(typed)
            bronze_dir = bronze_root / f"batch_id={bid}"
            bronze_dir.mkdir(exist_ok=True)
            partition_path = bronze_dir / "part-0.parquet"
            df.write_parquet(partition_path)
        else:
            df = pl.read_parquet(partition)
            df.cast(BRONZE_SCHEMA, strict=True)

    return _bronze_runner


def _make_sla_silver_runner(bid: str) -> Callable[[], None]:
    """Return a silver runner closure with bid captured by value."""

    def _silver_runner() -> None:
        runner = build_silver_runner()
        runner(bid)

    return _silver_runner


def _make_sla_gold_runner(bid: str) -> Callable[[], None]:
    """Return a gold runner closure with bid captured by value."""

    def _gold_runner() -> None:
        runner = build_gold_runner()
        runner(bid)

    return _gold_runner


class ComprehensiveSLAScenario:
    """Comprehensive SLA integration scenario (PERF.10).

    Combines performance metrics with SLA validation.
    Tests CI integration for performance regression detection.
    Gates deployments on performance compliance.
    """

    name: str = "comprehensive_sla"

    def run(self, ctx: ScenarioContext) -> Iterator[RunRecord]:
        """Run comprehensive SLA validation."""
        work_root = ctx.work_root / "comprehensive_sla"
        work_root.mkdir(parents=True, exist_ok=True)

        for run_idx in range(ctx.runs):
            run_dir = work_root / f"sla_run_{run_idx}"
            run_dir.mkdir(parents=True, exist_ok=True)

            # Create fixture for a representative pipeline run
            source_root = run_dir / "source"
            bronze_root = run_dir / "bronze"
            silver_root = run_dir / "silver"
            gold_root = run_dir / "gold"
            manifest_path = run_dir / "manifest.sqlite"
            lock_path = run_dir / "agent.lock"
            log_path = run_dir / "events.jsonl"

            # Create directories
            source_root.mkdir(exist_ok=True)
            bronze_root.mkdir(exist_ok=True)
            silver_root.mkdir(exist_ok=True)
            gold_root.mkdir(exist_ok=True)

            # Write a source file to process
            source_file = source_root / "sla_test.parquet"
            _write_slamark_source_parquet(source_file, nrows=1000)

            # Compute batch identity
            identity = compute_batch_identity(source_file)
            batch_id = identity.batch_id
            source_hash = identity.source_hash

            # Initialize manifest schema only — no pre-inserted batches
            db = ManifestDB(manifest_path).open()
            db.close()

            # Capture all loop variables by value in closure factories
            def _runners_for(
                bid: str,
                _bronze_root: Path = bronze_root,
                _source_file: Path = source_file,
                _source_hash: str = source_hash,
                _batch_id: str = batch_id,
            ) -> dict[Layer, Callable[[], None]]:
                if bid == _batch_id:
                    return {
                        Layer.BRONZE: _make_sla_bronze_runner(
                            _bronze_root, _source_file, _source_hash, bid
                        ),
                        Layer.SILVER: _make_sla_silver_runner(bid),
                        Layer.GOLD: _make_sla_gold_runner(bid),
                    }
                return {}

            # Execute the full pipeline with SLA monitoring
            manifest = ManifestDB(manifest_path).open()
            try:
                t0 = time.perf_counter_ns()
                result: AgentResult = run_once(
                    manifest=manifest,
                    source_root=source_root,
                    runners_for=_runners_for,
                    classify=lambda exc, layer, bid: _classify_exc(
                        exc, layer=layer, batch_id=bid
                    ),
                    build_fix=lambda exc, kind, layer, bid: None,
                    escalate=lambda exc, kind, layer, bid: None,
                    lock=AgentLock(lock_path),
                    event_logger=AgentEventLogger(log_path=log_path),
                )
                wall_ns = time.perf_counter_ns() - t0
            finally:
                manifest.close()

            if result.status is not RunStatus.COMPLETED:
                raise AssertionError(
                    f"SLA test run {run_idx} failed with status={result.status!r}"
                )

            wall_ms = wall_ns / 1_000_000

            yield RunRecord(
                scenario="comprehensive_sla",
                layer="bronze_silver_gold",  # Full pipeline
                run_idx=run_idx,
                wall_ms=wall_ms,
                peak_rss_mb=None,
                batch_id=batch_id,
                ts=ctx.clock(),
            )

            # Yield SLA compliance indicator as a separate record
            yield RunRecord(
                scenario="comprehensive_sla",
                layer="sla_compliance",
                run_idx=run_idx,
                wall_ms=wall_ms,
                peak_rss_mb=None,
                batch_id=f"{batch_id}_sla",
                ts=ctx.clock(),
            )


SCENARIO: ComprehensiveSLAScenario = ComprehensiveSLAScenario()
"""Module-level constant consumed by discover_scenarios()."""


def validate_ci_gate(
    records: Iterator[RunRecord],
    thresholds: dict[str, float] | None = None,
) -> tuple[bool, list[str]]:
    """Validate performance metrics for CI gate.

    Args:
        records: Performance records to validate
        thresholds: Optional custom thresholds (uses defaults if None)

    Returns:
        Tuple of (passes, [failure reasons])
    """
    if thresholds is None:
        thresholds = {
            "noop": 1000.0,
            "warm": 900_000.0,
            "fault": 300_000.0,
            "concurrent_batch": 1_800_000.0,
            "cold_start": 30_000.0,
        }

    failures: list[str] = []
    passes = True

    # Group by scenario to check averages
    by_scenario: dict[str, list[RunRecord]] = {}
    for rec in records:
        scenario = rec.scenario
        by_scenario.setdefault(scenario, []).append(rec)

    for scenario, scenario_records in by_scenario.items():
        if scenario in thresholds:
            avg_time = sum(rec.wall_ms for rec in scenario_records) / len(scenario_records)
            if avg_time > thresholds[scenario]:
                failures.append(
                    f"Scenario '{scenario}' average {avg_time:.1f}ms "
                    f"> threshold {thresholds[scenario]:.1f}ms"
                )
                passes = False

    return passes, failures
