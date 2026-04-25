"""PERF.6 fault-injection round-trip baseline.

For each of the four auto-correctable :class:`~pipeline.agent.types.ErrorKind`
values (``schema_drift``, ``regex_break``, ``partition_missing``,
``out_of_range``) this scenario:

1. Builds a green, single-batch Bronze fixture.
2. Applies the corresponding ``inject_*`` helper to introduce exactly
   one fault.
3. Runs :func:`~pipeline.agent.loop.run_once` with a retry budget of 2
   (one failure attempt + one fix-and-retry attempt).
4. Verifies the loop terminates with ``RunStatus.COMPLETED``.
5. Yields one :class:`~pipeline.perf.harness.RunRecord` per
   ``(kind x run_idx)`` pair.

Helpers copied from scripts/inject_fault.py; consolidating to a single
source of truth is a future refactor.
"""

from __future__ import annotations

import shutil
import statistics
import time
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import polars as pl

from pipeline.agent._logging import AgentEventLogger
from pipeline.agent.diagnoser import classify as _classify_exc
from pipeline.agent.fixes import partition_missing as _fix_pm
from pipeline.agent.fixes import schema_drift as _fix_sd
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.types import AgentResult, ErrorKind, Fix, Layer, RunStatus
from pipeline.ingest.batch import compute_batch_identity
from pipeline.ingest.reader import scan_source, validate_source_columns
from pipeline.ingest.transform import collect_bronze, transform_to_bronze
from pipeline.ingest.writer import write_bronze
from pipeline.perf.harness import RunRecord, ScenarioContext
from pipeline.schemas.bronze import BRONZE_SCHEMA, SOURCE_COLUMNS
from pipeline.state.manifest import ManifestDB

# ---------------------------------------------------------------------------
# inject_* helpers — copied verbatim from scripts/inject_fault.py.
# Consolidation to a single source of truth is deferred (see module docstring).
# ---------------------------------------------------------------------------

_BRONZE_PARTITION_FILE: Final[str] = "part-0.parquet"


def inject_schema_drift(parquet_path: Path) -> None:
    """Add an unexpected column to a Bronze parquet so the next read
    triggers a ``polars.SchemaError`` (or simply diverges from
    :data:`pipeline.schemas.bronze.BRONZE_SCHEMA`)."""
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    df = pl.read_parquet(parquet_path)
    drifted = df.with_columns(pl.lit("injected", dtype=pl.String).alias("injected_col"))
    drifted.write_parquet(parquet_path)


def inject_regex_break(parquet_path: Path) -> None:
    """Replace the body of the first row of a source parquet with a
    novel currency format (``❌ R$ 1.500,00``) that the F2 regex
    won't match."""
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    df = pl.read_parquet(parquet_path)
    if df.height == 0:
        raise ValueError("source parquet is empty; nothing to mutate")
    df = df.with_columns(
        pl.when(pl.int_range(0, df.height) == 0)
        .then(pl.lit("❌ R$ 1.500,00"))
        .otherwise(pl.col("message_body"))
        .alias("message_body")
    )
    df.write_parquet(parquet_path)


def inject_partition_missing(target: Path) -> None:
    """Delete a Bronze partition directory (or single parquet file)
    so the next layer hits :class:`FileNotFoundError`."""
    if not target.exists():
        raise FileNotFoundError(target)
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()


def inject_out_of_range(parquet_path: Path) -> None:
    """Append one row whose ``message_body`` carries a negative
    monetary value — out of range for the F2 quarantine guard."""
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    df = pl.read_parquet(parquet_path)
    if df.height == 0:
        raise ValueError("source parquet is empty; nothing to append")
    template = df.row(0, named=True)
    template["message_body"] = "valor_pago_atual_brl=-999"
    if "message_id" in template:
        template["message_id"] = (template.get("message_id") or "m") + "_oor"
    new_row = pl.DataFrame([template], schema=df.schema)
    pl.concat([df, new_row]).write_parquet(parquet_path)


# ---------------------------------------------------------------------------
# Fixture building.
# ---------------------------------------------------------------------------


def _write_source_parquet(path: Path) -> None:
    """Write a minimal valid source parquet (1 row) to ``path``."""
    rows: dict[str, list[str]] = {col: ["x"] for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"]
    rows["direction"] = ["inbound"]
    rows["message_type"] = ["text"]
    rows["status"] = ["delivered"]
    rows["message_body"] = ["valor_pago_atual_brl=100"]
    df = pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _build_green_fixture_for_kind(kind: ErrorKind, work_root: Path) -> dict[str, Any]:
    """Build a fully-processed, green Bronze fixture for *kind*.

    Runs the F1 ingest pipeline to produce the Bronze partition so that
    the injection helpers have a real file to mutate.

    Returns a paths dict with keys:

    ``bronze_root``, ``silver_root``, ``gold_root``
        Layer data roots.
    ``manifest_path``
        SQLite file with schema only (no pre-inserted batches — the
        observer will discover the source parquet as pending).
    ``lock_path``
        Path for the :class:`~pipeline.agent.lock.AgentLock`.
    ``log_path``
        Path for the :class:`~pipeline.agent._logging.AgentEventLogger`.
    ``target``
        The path the corresponding :func:`_inject` helper should
        receive:

        * ``schema_drift`` / ``partition_missing`` → Bronze parquet
        * ``regex_break`` / ``out_of_range`` → source parquet
    ``source_root``
        Directory containing the source parquet (used by
        :func:`run_once` as ``source_root``).
    ``batch_id``
        Deterministic batch identifier (string) for the written parquet.
    """
    source_root = work_root / "raw"
    bronze_root = work_root / "bronze"
    silver_root = work_root / "silver"
    gold_root = work_root / "gold"
    manifest_path = work_root / "manifest.sqlite"
    lock_path = work_root / "agent.lock"
    log_path = work_root / "logs" / "agent.jsonl"

    source_file = source_root / "conversations.parquet"
    _write_source_parquet(source_file)

    # Run F1 ingest to produce the Bronze partition.
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
    write_bronze(df, bronze_root=bronze_root, batch_id=batch_id)

    bronze_parquet = bronze_root / f"batch_id={batch_id}" / _BRONZE_PARTITION_FILE

    # Initialise manifest schema only — no batches pre-inserted so that
    # run_once's observer finds the source parquet as pending.
    db = ManifestDB(manifest_path).open()
    db.close()

    # Determine the injection target.
    if kind in (ErrorKind.SCHEMA_DRIFT, ErrorKind.PARTITION_MISSING):
        target: Path = bronze_parquet
    else:
        # REGEX_BREAK and OUT_OF_RANGE: mutate the source parquet so the
        # next Bronze ingest reproduces the bad data.
        target = source_file

    return {
        "bronze_root": bronze_root,
        "silver_root": silver_root,
        "gold_root": gold_root,
        "manifest_path": manifest_path,
        "lock_path": lock_path,
        "log_path": log_path,
        "target": target,
        "source_root": source_root,
        "batch_id": batch_id,
    }


# ---------------------------------------------------------------------------
# Inject dispatcher.
# ---------------------------------------------------------------------------


def _inject(kind: ErrorKind, target: Path) -> None:
    """Dispatch to the correct ``inject_*`` helper."""
    if kind is ErrorKind.SCHEMA_DRIFT:
        inject_schema_drift(target)
    elif kind is ErrorKind.REGEX_BREAK:
        inject_regex_break(target)
    elif kind is ErrorKind.PARTITION_MISSING:
        inject_partition_missing(target)
    elif kind is ErrorKind.OUT_OF_RANGE:
        inject_out_of_range(target)
    else:
        raise ValueError(f"no inject helper for kind={kind!r}")


# ---------------------------------------------------------------------------
# Recovery measurement.
# ---------------------------------------------------------------------------

_PERF_RETRY_BUDGET: Final[int] = 2
"""Budget for the perf run: one failure + one fix-and-retry is sufficient."""


def _bronze_runner(bronze_root: Path, batch_id: str) -> None:
    """Minimal Bronze validation runner — reads the partition and casts
    to BRONZE_SCHEMA to surface schema errors.  Raises
    :class:`FileNotFoundError` when the partition is absent so the
    ``partition_missing`` diagnoser path fires correctly."""
    partition = bronze_root / f"batch_id={batch_id}" / _BRONZE_PARTITION_FILE
    if not partition.exists():
        raise FileNotFoundError(partition)
    df = pl.read_parquet(partition)
    # Cast to canonical schema strict=True to surface extra / missing columns.
    df.cast(BRONZE_SCHEMA, strict=True)


def _make_fix_builder(
    source_root: Path,
    bronze_root: Path,
) -> Any:
    """Return a ``build_fix`` callable covering ``schema_drift`` and
    ``partition_missing``. ``regex_break`` and ``out_of_range`` return
    ``None`` (no deterministic fix for the bronze-only validation loop;
    those kinds will exhaust budget → escalation, which is acceptable for
    an observation-only baseline)."""

    def _dispatch(
        _exc: BaseException,
        kind: ErrorKind,
        _layer: Layer,
        batch_id: str,
    ) -> Fix | None:
        bronze_parquet = bronze_root / f"batch_id={batch_id}" / _BRONZE_PARTITION_FILE
        if kind is ErrorKind.SCHEMA_DRIFT:
            return _fix_sd.build_fix(bronze_parquet)
        if kind is ErrorKind.PARTITION_MISSING:
            for src_candidate in source_root.rglob("*.parquet"):
                try:
                    if compute_batch_identity(src_candidate).batch_id == batch_id:
                        return _fix_pm.build_fix(
                            source=src_candidate,
                            bronze_root=bronze_root,
                            batch_id=batch_id,
                        )
                except Exception:
                    continue
            return None
        return None

    return _dispatch


def _measure_recovery(paths: dict[str, Any]) -> float:
    """Run ``run_once`` against the faulted fixture and return wall_ms.

    Uses the deterministic diagnoser (no LLM) and the fix dispatcher.
    The loop must terminate with ``RunStatus.COMPLETED``; raises
    :class:`AssertionError` if it does not.
    """
    source_root: Path = paths["source_root"]
    bronze_root: Path = paths["bronze_root"]
    manifest_path: Path = paths["manifest_path"]
    lock_path: Path = paths["lock_path"]
    log_path: Path = paths["log_path"]
    batch_id: str = paths["batch_id"]

    build_fix = _make_fix_builder(source_root=source_root, bronze_root=bronze_root)

    manifest = ManifestDB(manifest_path).open()
    try:
        t0 = time.perf_counter_ns()
        result: AgentResult = run_once(
            manifest=manifest,
            source_root=source_root,
            runners_for=lambda bid: (
                {Layer.BRONZE: lambda: _bronze_runner(bronze_root, bid)} if bid == batch_id else {}
            ),
            classify=lambda exc, layer, bid: _classify_exc(exc, layer=layer, batch_id=bid),
            build_fix=build_fix,
            escalate=lambda exc, kind, layer, bid: None,
            lock=AgentLock(lock_path),
            retry_budget=_PERF_RETRY_BUDGET,
            event_logger=AgentEventLogger(log_path=log_path),
        )
        wall_ms = (time.perf_counter_ns() - t0) / 1_000_000
    finally:
        manifest.close()

    if result.status is not RunStatus.COMPLETED:
        raise AssertionError(f"run_once returned status={result.status!r}; expected COMPLETED")
    return wall_ms


# ---------------------------------------------------------------------------
# Scenario class.
# ---------------------------------------------------------------------------

# Only the 4 auto-correctable kinds — UNKNOWN is not a recovery scenario.
_MEASURABLE_KINDS: Final[tuple[ErrorKind, ...]] = (
    ErrorKind.SCHEMA_DRIFT,
    ErrorKind.REGEX_BREAK,
    ErrorKind.PARTITION_MISSING,
    ErrorKind.OUT_OF_RANGE,
)


class FaultScenario:
    """PERF.6 fault-injection round-trip baseline scenario.

    For each of the four auto-correctable
    :class:`~pipeline.agent.types.ErrorKind` values, builds a green
    Bronze fixture, applies the corresponding injection helper, runs
    :func:`~pipeline.agent.loop.run_once` with a retry budget of 2, and
    yields one :class:`~pipeline.perf.harness.RunRecord`.
    """

    name: str = "fault"

    def run(self, ctx: ScenarioContext) -> Iterator[RunRecord]:
        """Yield one :class:`~pipeline.perf.harness.RunRecord` per
        ``(kind x run_idx)`` pair."""
        for kind in _MEASURABLE_KINDS:
            for i in range(ctx.runs):
                run_dir = ctx.work_root / kind.value / f"run{i}"
                run_dir.mkdir(parents=True, exist_ok=True)

                paths = _build_green_fixture_for_kind(kind, run_dir)
                batch_id: str = paths["batch_id"]

                _inject(kind, paths["target"])
                ms = _measure_recovery(paths)

                yield RunRecord(
                    scenario="fault",
                    layer=kind.value,
                    run_idx=i,
                    wall_ms=ms,
                    peak_rss_mb=None,
                    batch_id=batch_id,
                    ts=ctx.clock(),
                )


SCENARIO: FaultScenario = FaultScenario()
"""Module-level constant consumed by
:func:`~pipeline.perf.harness.discover_scenarios`."""


# ---------------------------------------------------------------------------
# Aggregator.
# ---------------------------------------------------------------------------

_QUANTILE_MIN_N: Final[int] = 2
"""Minimum sample count for ``statistics.quantiles``; below this we fall back
to returning the single value for both p50 and p95."""


def summarize_fault_baselines(
    records: Iterable[RunRecord],
) -> dict[str, dict[str, float]]:
    """Group *records* by ``layer`` (i.e.
    :class:`~pipeline.agent.types.ErrorKind` value) and compute p50,
    p95, max, count.

    ``statistics.quantiles`` requires at least 2 data points; a small-N
    fallback handles ``count == 1`` by setting both p50 and p95 to the
    single value without raising.
    """
    by_layer: dict[str, list[float]] = {}
    for rec in records:
        layer_key: str = rec.layer or "unknown"
        by_layer.setdefault(layer_key, []).append(rec.wall_ms)

    result: dict[str, dict[str, float]] = {}
    for layer, values in by_layer.items():
        count = len(values)
        if count < _QUANTILE_MIN_N:
            p50 = p95 = values[0]
        else:
            # statistics.quantiles(n=100) returns 99 cut points at indices
            # 0…98 corresponding to the 1st…99th percentiles.
            qs = statistics.quantiles(values, n=100)
            p50 = qs[49]  # 50th percentile
            p95 = qs[94]  # 95th percentile
        result[layer] = {
            "p50_ms": p50,
            "p95_ms": p95,
            "max_ms": max(values),
            "count": float(count),
        }
    return result
