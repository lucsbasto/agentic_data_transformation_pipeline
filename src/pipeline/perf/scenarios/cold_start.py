"""PERF.9 — cold-start performance baseline scenario.

Measures initial startup time with empty caches and uninitialized
resources. Tests fixture loading, cache initialization, and cold-path
performance. Establishes baseline for new deployment scenarios.

Measures cold-path performance with empty caches.
"""

from __future__ import annotations

import os
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


def _write_cold_start_source_parquet(path: Path, nrows: int = 100) -> None:
    """Write a small source parquet for cold-start testing."""
    rows: dict[str, list[str]] = {col: [] for col in SOURCE_COLUMNS}
    for i in range(nrows):
        for col in SOURCE_COLUMNS:
            rows[col].append(f"{col}_val_{i}")

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


def _make_bronze_runner(
    bronze_root: Path,
    source_file: Path,
    identity: object,
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
                source_hash=identity.source_hash,  # type: ignore[attr-defined]
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


def _make_silver_runner(bid: str) -> Callable[[], None]:
    """Return a silver runner closure with bid captured by value."""

    def _silver_runner() -> None:
        runner = build_silver_runner()
        runner(bid)

    return _silver_runner


def _make_gold_runner(bid: str) -> Callable[[], None]:
    """Return a gold runner closure with bid captured by value."""

    def _gold_runner() -> None:
        runner = build_gold_runner()
        runner(bid)

    return _gold_runner


def _build_fixture(
    run_dir: Path,
    source_file: Path,
) -> tuple[Path, Path, Path, Path, Path, Path, Path, str, object]:
    """Set up all paths and write source parquet. Returns fixture tuple."""
    source_root = run_dir / "source"
    bronze_root = run_dir / "bronze"
    silver_root = run_dir / "silver"
    gold_root = run_dir / "gold"
    manifest_path = run_dir / "manifest.sqlite"
    lock_path = run_dir / "agent.lock"
    log_path = run_dir / "events.jsonl"

    source_root.mkdir(exist_ok=True)
    bronze_root.mkdir(exist_ok=True)
    silver_root.mkdir(exist_ok=True)
    gold_root.mkdir(exist_ok=True)

    _write_cold_start_source_parquet(source_file, nrows=50)
    identity = compute_batch_identity(source_file)
    batch_id = identity.batch_id

    # Initialize manifest schema only — no pre-inserted batches
    db = ManifestDB(manifest_path).open()
    db.close()

    return (
        source_root, bronze_root, silver_root, gold_root,
        manifest_path, lock_path, log_path, batch_id, identity,
    )


def _run_cold_iteration(
    run_idx: int,
    source_root: Path,
    bronze_root: Path,
    manifest_path: Path,
    lock_path: Path,
    log_path: Path,
    batch_id: str,
    identity: object,
    source_file: Path,
    ctx: ScenarioContext,
) -> RunRecord:
    """Execute one cold-start iteration and return its RunRecord."""

    def _runners_for(bid: str) -> dict[Layer, Callable[[], None]]:
        if bid == batch_id:
            return {
                Layer.BRONZE: _make_bronze_runner(bronze_root, source_file, identity, bid),
                Layer.SILVER: _make_silver_runner(bid),
                Layer.GOLD: _make_gold_runner(bid),
            }
        return {}

    manifest = ManifestDB(manifest_path).open()
    try:
        t0 = time.perf_counter_ns()
        result: AgentResult = run_once(
            manifest=manifest,
            source_root=source_root,
            runners_for=_runners_for,
            classify=lambda exc, layer, bid: _classify_exc(exc, layer=layer, batch_id=bid),
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
            f"cold start run {run_idx} failed with status={result.status!r}"
        )

    return RunRecord(
        scenario="cold_start",
        layer="bronze_silver_gold",
        run_idx=run_idx,
        wall_ms=wall_ns / 1_000_000,
        peak_rss_mb=None,
        batch_id=batch_id,
        ts=ctx.clock(),
    )


class ColdStartScenario:
    """Cold-start performance baseline scenario (PERF.9).

    Measures initial startup time with empty caches and uninitialized
    resources. Tests cold-path performance for new deployments.
    """

    name: str = "cold_start"

    def run(self, ctx: ScenarioContext) -> Iterator[RunRecord]:
        """Run cold-start performance measurement."""
        work_root = ctx.work_root / "cold_start"
        work_root.mkdir(parents=True, exist_ok=True)

        # Clear any cache environment variables to ensure truly cold start
        env_backup: dict[str, str] = {}
        for key in list(os.environ.keys()):
            if "CACHE" in key.upper() or "LLM" in key.upper():
                val = os.environ.pop(key, None)
                if val is not None:
                    env_backup[key] = val

        try:
            for run_idx in range(ctx.runs):
                run_dir = work_root / f"cold_run_{run_idx}"
                run_dir.mkdir(parents=True, exist_ok=True)

                source_file = run_dir / "source" / "cold_start.parquet"
                (
                    source_root,
                    bronze_root,
                    _silver_root,
                    _gold_root,
                    manifest_path,
                    lock_path,
                    log_path,
                    batch_id,
                    identity,
                ) = _build_fixture(run_dir, source_file)

                yield _run_cold_iteration(
                    run_idx=run_idx,
                    source_root=source_root,
                    bronze_root=bronze_root,
                    manifest_path=manifest_path,
                    lock_path=lock_path,
                    log_path=log_path,
                    batch_id=batch_id,
                    identity=identity,
                    source_file=source_file,
                    ctx=ctx,
                )
        finally:
            # Restore environment
            for key, value in env_backup.items():
                os.environ[key] = value


SCENARIO: ColdStartScenario = ColdStartScenario()
"""Module-level constant consumed by discover_scenarios()."""
