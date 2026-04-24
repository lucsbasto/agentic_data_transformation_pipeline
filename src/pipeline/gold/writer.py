"""Atomic parquet + JSON writers for the Gold layer.

Mirrors :mod:`pipeline.silver.writer`: every output is staged in a
``.tmp-...`` directory next to its final partition, then promoted with
:func:`os.replace` (POSIX ``rename(2)``) once the bytes are flushed.
``rename`` is atomic within the same filesystem — readers see either
the previous good file or the new one, never a half-written byte
range. The temp directory lives under the same ``gold_root`` so the
swap stays on one filesystem.

Path layout (per F3-RF-03):

    <gold_root>/<table>/batch_id=<id>/part-0.parquet     # 4 tables
    <gold_root>/insights/batch_id=<id>/summary.json      # insights JSON

Every parquet write re-asserts the canonical Gold schema before any
bytes touch disk; a drift surfaces as :class:`SchemaDriftError`
instead of a corrupt parquet that only fails downstream.
"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import polars as pl

from pipeline.errors import PipelineError
from pipeline.schemas.gold import (
    assert_agent_performance_schema,
    assert_competitor_intel_schema,
    assert_conversation_scores_schema,
    assert_lead_profile_schema,
)

_COMPRESSION: Literal["zstd"] = "zstd"


@dataclass(frozen=True, slots=True)
class GoldWriteResult:
    """Outcome of a single Gold parquet partition write."""

    gold_path: Path
    rows_written: int


@dataclass(frozen=True, slots=True)
class GoldInsightsWriteResult:
    """Outcome of a single Gold insights JSON write."""

    insights_path: Path


def _write_parquet_atomic(
    df: pl.DataFrame,
    *,
    gold_root: Path,
    batch_id: str,
    table: str,
) -> GoldWriteResult:
    """Stage ``df`` under ``.tmp-batch_id=<id>`` then swap into place.

    Caller-supplied schema assertion has already run before we touch
    disk. The temp dir is wiped on any failure so the next attempt
    starts from a clean slate; a pre-existing valid final partition
    is left untouched until the swap actually succeeds.
    """
    table_root = gold_root / table
    table_root.mkdir(parents=True, exist_ok=True)
    final_dir = table_root / f"batch_id={batch_id}"
    tmp_dir = table_root / f".tmp-batch_id={batch_id}"

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=False)

    tmp_file = tmp_dir / "part-0.parquet"
    final_file = final_dir / "part-0.parquet"

    try:
        df.write_parquet(tmp_file, compression=_COMPRESSION, statistics=True)
        # The final dir must be removed first because ``Path.replace``
        # (a.k.a. POSIX ``rename``) is only atomic when the target is
        # absent or is itself a single empty directory; replacing a
        # non-empty directory is not portable.
        if final_dir.exists():
            shutil.rmtree(final_dir)
        tmp_dir.replace(final_dir)
    except Exception as exc:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise PipelineError(
            f"failed to write Gold partition {table!r} for batch {batch_id!r}: {exc}"
        ) from exc

    rows_written = pl.scan_parquet(final_file).select(pl.len()).collect().item()
    if rows_written != df.height:
        raise PipelineError(
            f"Gold {table!r} write roundtrip mismatch: wrote {df.height} rows, "
            f"read back {rows_written} for batch {batch_id!r}"
        )

    return GoldWriteResult(gold_path=final_file, rows_written=rows_written)


def write_gold_conversation_scores(
    df: pl.DataFrame,
    *,
    gold_root: Path,
    batch_id: str,
) -> GoldWriteResult:
    """Atomically write ``gold.conversation_scores`` for ``batch_id``."""
    assert_conversation_scores_schema(df)
    return _write_parquet_atomic(
        df, gold_root=gold_root, batch_id=batch_id, table="conversation_scores"
    )


def write_gold_lead_profile(
    df: pl.DataFrame,
    *,
    gold_root: Path,
    batch_id: str,
) -> GoldWriteResult:
    """Atomically write ``gold.lead_profile`` for ``batch_id``."""
    assert_lead_profile_schema(df)
    return _write_parquet_atomic(df, gold_root=gold_root, batch_id=batch_id, table="lead_profile")


def write_gold_agent_performance(
    df: pl.DataFrame,
    *,
    gold_root: Path,
    batch_id: str,
) -> GoldWriteResult:
    """Atomically write ``gold.agent_performance`` for ``batch_id``."""
    assert_agent_performance_schema(df)
    return _write_parquet_atomic(
        df, gold_root=gold_root, batch_id=batch_id, table="agent_performance"
    )


def write_gold_competitor_intel(
    df: pl.DataFrame,
    *,
    gold_root: Path,
    batch_id: str,
) -> GoldWriteResult:
    """Atomically write ``gold.competitor_intel`` for ``batch_id``."""
    assert_competitor_intel_schema(df)
    return _write_parquet_atomic(
        df, gold_root=gold_root, batch_id=batch_id, table="competitor_intel"
    )


def write_gold_insights(
    payload: Mapping[str, object],
    *,
    gold_root: Path,
    batch_id: str,
) -> GoldInsightsWriteResult:
    """Atomically write ``gold/insights/batch_id=<id>/summary.json``.

    Same temp-then-rename pattern as the parquet writers but at file
    granularity: the JSON is serialised to a sibling ``.tmp.<pid>``
    file, ``fsync``'d, then promoted via :func:`os.replace`. POSIX
    guarantees readers see the previous file or the new one — never a
    truncated half-write — even if the process crashes mid-flush.
    """
    insights_root = gold_root / "insights"
    final_dir = insights_root / f"batch_id={batch_id}"
    tmp_dir = insights_root / f".tmp-batch_id={batch_id}"

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Track whether ``final_dir`` existed before this call so we know
    # whether to remove it on a rename failure (a failed first-time
    # write must leave no trace; a failed re-write must leave the
    # previous good file alone — and that lives inside ``final_dir``).
    final_dir_pre_existed = final_dir.exists()
    final_file = final_dir / "summary.json"
    tmp_file = tmp_dir / f"summary.json.tmp.{os.getpid()}"

    try:
        with tmp_file.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, sort_keys=False)
            fh.flush()
            os.fsync(fh.fileno())
        final_dir.mkdir(parents=True, exist_ok=True)
        # ``Path.replace`` delegates to POSIX ``rename(2)`` — atomic on
        # the same filesystem. We staged ``tmp_file`` under
        # ``insights_root`` so we never cross filesystems.
        tmp_file.replace(final_file)
    except Exception as exc:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if not final_dir_pre_existed and final_dir.exists():
            shutil.rmtree(final_dir, ignore_errors=True)
        raise PipelineError(f"failed to write Gold insights for batch {batch_id!r}: {exc}") from exc
    finally:
        # Clean the temp dir on every path; a successful ``os.replace``
        # leaves the dir empty, a failure leaves it dirty — either way
        # we want it gone.
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return GoldInsightsWriteResult(insights_path=final_file)


__all__ = [
    "GoldInsightsWriteResult",
    "GoldWriteResult",
    "write_gold_agent_performance",
    "write_gold_competitor_intel",
    "write_gold_conversation_scores",
    "write_gold_insights",
    "write_gold_lead_profile",
]
