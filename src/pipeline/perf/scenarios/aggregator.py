"""PERF.4 — throughput aggregator p50/p95 quantiles.

Statistical aggregation of performance measurements across runs.
Computes quantiles (p50=median, p95=95th percentile) and summary stats
from existing performance records. Not a runnable scenario but provides
aggregation utilities for downstream analysis.

Provides p50/p95 quantiles for throughput characterization.
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable

from pipeline.perf.harness import RunRecord

# Minimum sample count for quantiles; below this we return the single value
_QUANTILE_MIN_N: int = 2


def aggregate_quantiles(records: Iterable[RunRecord]) -> dict[str, dict[str, float]]:
    """Aggregate performance records and compute quantiles.

    Groups records by scenario and computes:
    - p50 (median), p95 (95th percentile)
    - min, max, mean
    - count of samples

    Args:
        records: Iterable of RunRecord objects to aggregate

    Returns:
        Dict mapping scenario names to their statistical summaries
    """
    by_scenario: dict[str, list[float]] = {}
    for rec in records:
        scenario = rec.scenario
        by_scenario.setdefault(scenario, []).append(rec.wall_ms)

    result: dict[str, dict[str, float]] = {}
    for scenario, wall_times in by_scenario.items():
        if len(wall_times) < _QUANTILE_MIN_N:
            # Single value fallback
            value = wall_times[0] if wall_times else 0.0
            result[scenario] = {
                "p50_ms": value,
                "p95_ms": value,
                "min_ms": value,
                "max_ms": value,
                "mean_ms": value,
                "count": float(len(wall_times)),
            }
        else:
            # Calculate statistical measures
            sorted_times = sorted(wall_times)

            # Calculate quantiles using statistics.quantiles
            # n=100 returns 99 values representing 1%-99% percentiles
            # Index 49 = 50th percentile (median), index 94 = 95th percentile
            try:
                quantiles = statistics.quantiles(sorted_times, n=100)
                p50 = quantiles[49]  # 50th percentile (p50/median)
                p95 = quantiles[94]  # 95th percentile
            except statistics.StatisticsError:
                # Fallback if insufficient data for quantiles
                p50 = statistics.median(sorted_times)
                idx = min(int(len(sorted_times) * 0.95), len(sorted_times) - 1)
                p95 = sorted_times[idx] if sorted_times else 0.0

            result[scenario] = {
                "p50_ms": p50,
                "p95_ms": p95,
                "min_ms": min(sorted_times),
                "max_ms": max(sorted_times),
                "mean_ms": statistics.mean(sorted_times),
                "stddev_ms": statistics.stdev(sorted_times) if len(sorted_times) > 1 else 0.0,
                "count": float(len(sorted_times)),
            }

    return result


def calculate_batch_throughput(records: Iterable[RunRecord]) -> dict[str, float]:
    """Calculate throughput as batches per second.

    Args:
        records: Performance records to analyze

    Returns:
        Dict mapping scenarios to throughput rates (batches per second)
    """
    by_scenario: dict[str, list[RunRecord]] = {}
    for rec in records:
        scenario = rec.scenario
        by_scenario.setdefault(scenario, []).append(rec)

    result = {}
    for scenario, scenario_records in by_scenario.items():
        total_batches = len(scenario_records)
        total_time_ms = sum(rec.wall_ms for rec in scenario_records)

        throughput = (total_batches * 1000.0) / total_time_ms if total_time_ms > 0 else 0.0

        result[scenario] = throughput

    return result
