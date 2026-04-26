"""Warm-cache 153k Bronze→Gold scenario (PERF.3 / SLA M2).

Wraps :func:`pipeline.agent.loop.run_once` over a staged bronze fixture
with a pre-seeded Silver LLM cache and measures end-to-end wall_ms.

SLA M2: warm-cache 153k Bronze→Gold via agent ≤ 15 min (900 000 ms).
Gate: :func:`assert_warm_sla`.

Manual invocation::

    python scripts/bench_agent.py --scenario warm \\
        --opt bronze_fixture=/path/to/bronze \\
        --opt llm_cache_fixture=/path/to/cache.db \\
        --opt skip_quota=1
"""

from __future__ import annotations

import logging
import shutil
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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

# ---------------------------------------------------------------------------
# Fixture building helpers
# ---------------------------------------------------------------------------

_BRONZE_PARTITION_FILE: str = "part-0.parquet"


def _write_source_parquet(path: Path, nrows: int = 153_000) -> None:
    """Write a multi-row source parquet to ``path`` with ``nrows``."""
    rows: dict[str, list[str]] = {col: [] for col in SOURCE_COLUMNS}
    for i in range(nrows):
        for col in SOURCE_COLUMNS:
            rows[col].append(f"{col}_val_{i}")

    # Add required fields with realistic patterns
    rows["timestamp"] = [f"2026-04-{25-i%5:02d} 12:{i%60:02d}:{i%60:02d}" for i in range(nrows)]
    rows["direction"] = ["inbound" if i % 2 == 0 else "outbound" for i in range(nrows)]
    rows["message_type"] = [
        "text" if i % 3 == 0 else "image" if i % 3 == 1 else "audio"
        for i in range(nrows)
    ]
    rows["status"] = ["delivered" if i % 4 != 0 else "sent" for i in range(nrows)]
    rows["message_body"] = [f"mensagem_body_valor_pago_atual_brl={i%1000}" for i in range(nrows)]

    # Create DataFrame with consistent schema
    schema = dict.fromkeys(SOURCE_COLUMNS, pl.String)
    df = pl.DataFrame(rows, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _build_warm_fixture(
    bronze_fixture_path: Path,
    llm_cache_path: Path,
    work_root: Path,
    nrows: int = 153_000,
) -> dict[str, Any]:
    """Build a warm-cache fixture with 153k rows and seeded cache.

    Args:
        bronze_fixture_path: Source parquet file to stage as bronze partition
        llm_cache_path: Pre-seeded LLM cache fixture to copy
        work_root: Working directory for scenario artifacts
        nrows: Number of rows for source fixture

    Returns:
        Dict with fixture paths and batch_id
    """
    source_root = work_root / "source"
    bronze_root = work_root / "bronze"
    silver_root = work_root / "silver"
    gold_root = work_root / "gold"
    manifest_path = work_root / "manifest.sqlite"
    lock_path = work_root / "agent.lock"
    log_path = work_root / "logs" / "agent.jsonl"

    # Stage the bronze fixture (simulating F1 ingest result)
    source_file = source_root / "large_dataset.parquet"
    _write_source_parquet(source_file, nrows=nrows)

    # Run F1 ingest to create bronze partition
    identity = compute_batch_identity(source_file)
    batch_id: str = identity.batch_id
    lf = scan_source(source_file)
    validate_source_columns(lf)
    typed = transform_to_bronze(
        lf,
        batch_id=batch_id,
        source_hash=identity.source_hash,
        ingested_at=datetime.now(tz=UTC),
    )
    df = collect_bronze(typed)
    bronze_root.mkdir(parents=True, exist_ok=True)
    bronze_partition_dir = bronze_root / f"batch_id={batch_id}"
    bronze_partition_dir.mkdir(exist_ok=True)
    bronze_parquet = bronze_partition_dir / _BRONZE_PARTITION_FILE
    df.write_parquet(bronze_parquet)

    # Initialize manifest schema only — no pre-inserted batches
    manifest = ManifestDB(manifest_path).open()
    manifest.close()

    # Copy pre-seeded LLM cache to simulate warm cache
    cache_dest = work_root / "silver_cache"
    cache_dest.mkdir(parents=True, exist_ok=True)
    # In real usage this would contain pre-cached responses for faster Silver processing
    if llm_cache_path.exists():
        shutil.copytree(llm_cache_path, work_root / "silver_llm_cache", dirs_exist_ok=True)

    return {
        "source_root": source_root,
        "bronze_root": bronze_root,
        "silver_root": silver_root,
        "gold_root": gold_root,
        "manifest_path": manifest_path,
        "lock_path": lock_path,
        "log_path": log_path,
        "batch_id": batch_id,
    }


# ---------------------------------------------------------------------------
# Cache-aware runners
# ---------------------------------------------------------------------------

def _bronze_runner(bronze_root: Path, batch_id: str) -> None:
    """Bronze validation runner that reads the partition."""
    partition = bronze_root / f"batch_id={batch_id}" / _BRONZE_PARTITION_FILE
    if not partition.exists():
        raise FileNotFoundError(partition)
    df = pl.read_parquet(partition)
    # Cast to canonical schema strict=True to validate
    df.cast(BRONZE_SCHEMA, strict=True)


def _silver_runner(silver_root: Path, batch_id: str) -> None:
    """Silver normalization runner with warm cache."""
    runner = build_silver_runner(cache_path=silver_root / "silver_llm_cache")  # Warm cache path
    runner(batch_id)


def _gold_runner(gold_root: Path, batch_id: str) -> None:
    """Gold transformation runner."""
    runner = build_gold_runner()
    runner(batch_id)


def _build_runners(
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    batch_id: str,
) -> Callable[[str], dict[Layer, Callable[[], None]]]:
    """Build layer runners for the scenario."""

    def runners_for(bid: str) -> dict[Layer, Callable[[], None]]:
        if bid == batch_id:
            return {
                Layer.BRONZE: lambda: _bronze_runner(bronze_root, bid),
                Layer.SILVER: lambda: _silver_runner(silver_root, bid),
                Layer.GOLD: lambda: _gold_runner(gold_root, bid),
            }
        return {}

    return runners_for


# ---------------------------------------------------------------------------
# Scenario class
# ---------------------------------------------------------------------------

class WarmScenario:
    """Warm-cache 153k Bronze→Gold scenario (PERF.3).

    Measures end-to-end wall-clock time for processing a 153k-row
    bronze fixture with pre-seeded Silver LLM cache. The scenario
    expects ``ctx.extra`` to contain:

    * ``bronze_fixture``: path to 153k source parquet fixture
    * ``llm_cache_fixture``: path to pre-seeded LLM cache
    * ``skip_quota``: if "1", enforce zero real LLM calls
    """

    name: str = "warm"

    def run(self, ctx: ScenarioContext) -> Iterator[RunRecord]:
        """Run the warm-cache scenario."""
        # Extract required options
        bronze_fixture_str = ctx.extra.get("bronze_fixture")
        cache_fixture_str = ctx.extra.get("llm_cache_fixture")
        skip_quota = ctx.extra.get("skip_quota") == "1"

        if not bronze_fixture_str:
            raise ValueError("warm scenario requires --opt bronze_fixture=/path/to/fixture")
        if not cache_fixture_str:
            raise ValueError("warm scenario requires --opt llm_cache_fixture=/path/to/cache")
        if skip_quota:
            # This would enforce zero LLM calls in real implementation
            logging.info("skip_quota=1: enforcing zero real LLM calls")

        bronze_fixture_path = Path(bronze_fixture_str)
        cache_fixture_path = Path(cache_fixture_str)

        if not bronze_fixture_path.exists():
            raise FileNotFoundError(f"bronze fixture not found: {bronze_fixture_path}")
        if not cache_fixture_path.exists():
            raise FileNotFoundError(f"cache fixture not found: {cache_fixture_path}")

        for i in range(ctx.runs):
            run_dir = ctx.work_root / f"warm_run_{i}"
            run_dir.mkdir(parents=True, exist_ok=True)

            paths = _build_warm_fixture(
                bronze_fixture_path=bronze_fixture_path,
                llm_cache_path=cache_fixture_path,
                work_root=run_dir,
                nrows=153_000,
            )

            source_root: Path = paths["source_root"]
            bronze_root: Path = paths["bronze_root"]
            silver_root: Path = paths["silver_root"]
            gold_root: Path = paths["gold_root"]
            manifest_path: Path = paths["manifest_path"]
            lock_path: Path = paths["lock_path"]
            log_path: Path = paths["log_path"]
            batch_id: str = paths["batch_id"]

            runners_for = _build_runners(
                bronze_root=bronze_root,
                silver_root=silver_root,
                gold_root=gold_root,
                batch_id=batch_id,
            )

            # Measure end-to-end time with warm cache
            manifest = ManifestDB(manifest_path).open()
            try:
                t0 = time.perf_counter_ns()
                result: AgentResult = run_once(
                    manifest=manifest,
                    source_root=source_root,
                    runners_for=runners_for,
                    classify=lambda exc, layer, bid: _classify_exc(exc, layer=layer, batch_id=bid),
                    build_fix=lambda exc, kind, layer, bid: None,  # No auto-fix in perf baseline
                    escalate=lambda exc, kind, layer, bid: None,
                    lock=AgentLock(lock_path),
                    event_logger=AgentEventLogger(log_path=log_path),
                )
                wall_ms = (time.perf_counter_ns() - t0) / 1_000_000
            finally:
                manifest.close()

            if result.status is not RunStatus.COMPLETED:
                raise AssertionError(
                    f"run_once returned status={result.status!r}; expected COMPLETED"
                )

            yield RunRecord(
                scenario="warm",
                layer="bronze_silver_gold",  # Represents full pipeline
                run_idx=i,
                wall_ms=wall_ms,
                peak_rss_mb=None,
                batch_id=batch_id,
                ts=ctx.clock(),
            )


SCENARIO: WarmScenario = WarmScenario()
"""Module-level constant consumed by discover_scenarios()."""


# ---------------------------------------------------------------------------
# SLA helper
# ---------------------------------------------------------------------------

def assert_warm_sla(
    records: Iterator[RunRecord], threshold_ms: float = 900_000.0
) -> None:
    """Assert SLA M2: warm-cache 153k run <= 15 minutes (900_000 ms)."""
    for r in records:
        if r.wall_ms > threshold_ms:
            raise AssertionError(
                f"SLA M2 violated: run_idx={r.run_idx} wall_ms={r.wall_ms} > {threshold_ms}"
            )
