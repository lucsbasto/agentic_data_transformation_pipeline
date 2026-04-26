"""PERF.7 — concurrent batch throughput scenario.

Measures throughput with multiple concurrent batches being processed
simultaneously. Evaluates scaling characteristics and resource utilization
under various concurrency levels. Tracks how performance degrades or
improves with increasing batch concurrency.

Measures concurrent processing performance under controlled load.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from pipeline.agent._logging import AgentEventLogger
from pipeline.agent.diagnoser import classify as _classify_exc
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.types import AgentResult, Layer, RunStatus
from pipeline.gold.transform import build_gold_runner
from pipeline.perf.harness import RunRecord, ScenarioContext
from pipeline.schemas.bronze import BRONZE_SCHEMA, SOURCE_COLUMNS
from pipeline.silver.normalize import build_silver_runner
from pipeline.state.manifest import ManifestDB


@dataclass
class _BatchFixture:
    """Paths and identifiers for a single concurrent batch fixture."""

    batch_id: str
    source_root: Path
    bronze_root: Path
    silver_root: Path
    gold_root: Path
    manifest_path: Path
    lock_path: Path
    log_path: Path


def _write_concurrent_source_parquet(path: Path, batch_id: str, nrows: int = 1000) -> None:
    """Write a source parquet for concurrent batch with unique identifier."""
    rows: dict[str, list[str]] = {col: [] for col in SOURCE_COLUMNS}
    for i in range(nrows):
        for col in SOURCE_COLUMNS:
            rows[col].append(f"{batch_id}_{col}_val_{i}")

    # Add required fields with batch-specific patterns
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


async def _process_single_batch(
    manifest_path: Path,
    source_root: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    batch_id: str,
    lock_path: Path,
    log_path: Path,
) -> float:
    """Process a single batch asynchronously."""

    def _runners_for(bid: str) -> dict[Layer, Callable[[], None]]:
        if bid == batch_id:

            def _bronze_runner() -> None:
                partition = bronze_root / f"batch_id={bid}" / "part-0.parquet"
                if not partition.exists():
                    raise FileNotFoundError(partition)
                df = pl.read_parquet(partition)
                df.cast(BRONZE_SCHEMA, strict=True)

            def _silver_runner() -> None:
                runner = build_silver_runner()
                runner(bid)

            def _gold_runner() -> None:
                runner = build_gold_runner()
                runner(bid)

            return {
                Layer.BRONZE: _bronze_runner,
                Layer.SILVER: _silver_runner,
                Layer.GOLD: _gold_runner,
            }
        return {}

    # Load fresh manifest for this batch
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
        raise AssertionError(f"batch {batch_id} failed with status={result.status!r}")

    return wall_ns / 1_000_000  # Convert to milliseconds


class ConcurrentBatchScenario:
    """Concurrent batch throughput scenario (PERF.7).

    Measures performance when multiple batches are processed simultaneously.
    Tests scaling characteristics and resource utilization under load.
    """

    name: str = "concurrent_batch"

    def run(self, ctx: ScenarioContext) -> Iterator[RunRecord]:
        """Run concurrent batch processing and yield per-batch timing."""
        work_root = ctx.work_root / "concurrent_batches"
        work_root.mkdir(parents=True, exist_ok=True)

        # Number of concurrent batches to process
        concurrency_level = int(ctx.extra.get("concurrency", 3))

        for run_idx in range(ctx.runs):
            run_dir = work_root / f"run_{run_idx}"
            run_dir.mkdir(parents=True, exist_ok=True)

            # Create multiple batch fixtures
            batches: list[_BatchFixture] = []
            for i in range(concurrency_level):
                batch_id = f"concurrent_batch_{run_idx}_{i}"

                # Create fixture paths for this batch
                batch_dir = run_dir / batch_id
                batch_dir.mkdir(exist_ok=True)

                source_root = batch_dir / "source"
                bronze_root = batch_dir / "bronze"
                silver_root = batch_dir / "silver"
                gold_root = batch_dir / "gold"
                manifest_path = batch_dir / "manifest.sqlite"
                lock_path = batch_dir / "agent.lock"
                log_path = batch_dir / "events.jsonl"

                # Create directories
                source_root.mkdir(exist_ok=True)
                bronze_root.mkdir(exist_ok=True)
                silver_root.mkdir(exist_ok=True)
                gold_root.mkdir(exist_ok=True)

                # Write source parquet for this batch
                source_file = source_root / f"{batch_id}.parquet"
                _write_concurrent_source_parquet(source_file, batch_id)

                # Initialize manifest schema only
                db = ManifestDB(manifest_path).open()
                db.close()

                batches.append(
                    _BatchFixture(
                        batch_id=batch_id,
                        source_root=source_root,
                        bronze_root=bronze_root,
                        silver_root=silver_root,
                        gold_root=gold_root,
                        manifest_path=manifest_path,
                        lock_path=lock_path,
                        log_path=log_path,
                    )
                )

            # Process all batches concurrently
            async def _run_concurrent(
                _batches: list[_BatchFixture] = batches,
            ) -> list[float]:
                tasks = [
                    _process_single_batch(
                        b.manifest_path,
                        b.source_root,
                        b.bronze_root,
                        b.silver_root,
                        b.gold_root,
                        b.batch_id,
                        b.lock_path,
                        b.log_path,
                    )
                    for b in _batches
                ]
                return list(await asyncio.gather(*tasks))

            # Measure total time for all concurrent batches
            t0 = time.perf_counter_ns()
            batch_times: list[float] = asyncio.run(_run_concurrent())
            total_time_ms = (time.perf_counter_ns() - t0) / 1_000_000

            # Yield individual batch timing records
            for batch_info, batch_time in zip(batches, batch_times, strict=True):
                yield RunRecord(
                    scenario="concurrent_batch",
                    layer="bronze_silver_gold",  # Full pipeline
                    run_idx=run_idx,
                    wall_ms=batch_time,  # Time for this individual batch
                    peak_rss_mb=None,
                    batch_id=batch_info.batch_id,
                    ts=ctx.clock(),
                )

            # Also yield a total run record
            yield RunRecord(
                scenario="concurrent_batch",
                layer="total_run",
                run_idx=run_idx,
                wall_ms=total_time_ms,  # Total time for all batches
                peak_rss_mb=None,
                batch_id=f"run_{run_idx}_total",
                ts=ctx.clock(),
            )


SCENARIO: ConcurrentBatchScenario = ConcurrentBatchScenario()
"""Module-level constant consumed by discover_scenarios()."""
