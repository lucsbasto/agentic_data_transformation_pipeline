"""Atomic partitioned parquet writer for Bronze.

Writes go to a temporary directory alongside the target partition, and
only switch to the final location via ``os.replace`` after the file is
flushed. ``os.replace`` is atomic within the same filesystem, so the
temp dir must live under ``bronze_root``.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import polars as pl

from pipeline.errors import IngestError
from pipeline.ingest.transform import assert_bronze_schema

_COMPRESSION: Literal["zstd"] = "zstd"


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of a single Bronze partition write."""

    bronze_path: Path
    rows_written: int


def write_bronze(
    df: pl.DataFrame,
    *,
    bronze_root: Path,
    batch_id: str,
) -> WriteResult:
    """Write ``df`` atomically to ``bronze_root/batch_id=<id>/part-0.parquet``.

    - Schema is re-asserted against :data:`BRONZE_SCHEMA` before writing.
    - A previous partition for the same ``batch_id`` is replaced: the
      temp dir is created next to the final dir, written to, then
      renamed over the target. A leftover temp dir from a prior crash
      is cleaned before the new write.
    - Callers who hold a manifest row for this ``batch_id`` should first
      flip that row to ``IN_PROGRESS``; the writer itself is stateless.
    """
    assert_bronze_schema(df)

    bronze_root.mkdir(parents=True, exist_ok=True)
    final_dir = bronze_root / f"batch_id={batch_id}"
    tmp_dir = bronze_root / f".tmp-batch_id={batch_id}"

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=False)

    tmp_file = tmp_dir / "part-0.parquet"
    final_file = final_dir / "part-0.parquet"

    try:
        df.write_parquet(tmp_file, compression=_COMPRESSION, statistics=True)
        # Atomic swap: remove stale final dir (re-run scenario), then move.
        if final_dir.exists():
            shutil.rmtree(final_dir)
        tmp_dir.replace(final_dir)
    except Exception as exc:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise IngestError(
            f"failed to write Bronze partition for batch {batch_id!r}: {exc}"
        ) from exc

    # Re-scan the partition to confirm round-trip integrity.
    rows_written = pl.scan_parquet(final_file).select(pl.len()).collect().item()
    if rows_written != df.height:
        raise IngestError(
            f"Bronze write roundtrip mismatch: wrote {df.height} rows, "
            f"read back {rows_written} for batch {batch_id!r}"
        )

    return WriteResult(bronze_path=final_file, rows_written=rows_written)


__all__ = ["WriteResult", "write_bronze"]
