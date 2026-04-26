# F4 Perf Tasks

> Performance scenarios for agent loop. Each task = 1 commit `conventional-commit`. Tests verdes obrigatórios antes de fechar a task.
> Ordem é dependência-respeitada; tasks paralelas marcadas como tal.
> Status: ⚪ pending · 🟡 in-progress · ✅ done.

## PERF.1 — bench harness scaffold
- `feat(perf): bench harness scaffold + scenario registry + Click CLI`
- Implements discovery registry via `pkgutil.iter_modules` 
- Exposes CLI via `scripts/bench_agent.py`
- Defines `Scenario` protocol and `RunRecord` format
- Tests: registry discovery, CLI args parsing, JSONL output

## PERF.2 — noop scenario + NFR-01 sub-second SLA
- `feat(perf): noop scenario + NFR-01 sub-second SLA gate`
- Measures empty `agent.run_once` wall-clock time
- SLA: < 1000ms for no-op run
- Tests: fixture creation, timing measurement, SLA assertion

## PERF.3 — warm-cache 153k Bronze→Gold scenario
- `feat(perf): warm-cache 153k Bronze→Gold scenario + SLA M2 gate`
- Measures end-to-end with 153k row bronze fixture + pre-seeded cache
- SLA: ≤ 900_000ms (15 min) for warm-cache run
- Tests: fixture staging, cache seeding, timing measurement

## PERF.4 — throughput aggregator p50/p95
- `feat(perf): throughput aggregator with p50/p95 quantiles`
- Implements statistical aggregation of wall-clock measurements
- Exposes quantile helpers: p50 (median), p95 (95th percentile)
- Aggregates across runs and scenarios for throughput metrics
- Tests: quantile calculations, statistical accuracy, edge cases

## PERF.5 — agent overhead delta scenario
- `feat(perf): agent overhead delta scenario + ≤5% SLA gate`
- Measures overhead of agent scaffolding vs raw pipeline
- Compares direct LayerRunner calls vs agent.run_once
- SLA: ≤ 5% overhead for agent wrapping
- Tests: paired measurements, threshold logic, overhead calculation

## PERF.6 — fault-injection round-trip baseline
- `feat(perf): fault-injection round-trip baseline scenario`
- Baseline measurements for each auto-correctable ErrorKind
- Measures recovery time for schema_drift, regex_break, partition_missing, out_of_range
- Tests: fixture injection, recovery measurement, error kind classification

## PERF.7 — concurrent batch throughput
- `feat(perf): concurrent batch throughput under load`
- Measures throughput with multiple concurrent batches
- Tests scaling characteristics under various concurrency levels
- Tracks resource utilization during concurrent runs
- Tests: fixture setup, concurrency control, resource measurement

## PERF.8 — peak RSS + ManifestDB IO counters
- `feat(perf): peak RSS + ManifestDB IO counter`
- Instruments peak memory usage measurement
- Counts ManifestDB read/write operations
- Provides resource utilization metrics
- Tests: memory tracking, IO counting, instrumentation integration

## PERF.9 — cold-start performance baseline
- `feat(perf): cold-start performance baseline`
- Measures initial startup time with empty caches
- Tests fixture loading, cache initialization, cold-path performance
- Establishes baseline for new deployment scenarios
- Tests: fixture preparation, cold-path measurement, startup timing

## PERF.10 — comprehensive SLA integration
- `feat(perf): comprehensive SLA integration and CI gating`
- Combines all perf scenarios under unified SLA gates
- Implements CI integration for performance regression detection
- Gates deployments on performance SLA compliance
- Tests: end-to-end SLA validation, CI pipeline integration