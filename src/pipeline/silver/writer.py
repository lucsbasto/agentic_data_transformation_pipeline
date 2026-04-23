"""Atomic partitioned parquet writer for Silver.

Mirrors :mod:`pipeline.ingest.writer`: write to a temp dir next to the
target, swap into place via ``os.replace`` after the file flushes.
Atomic within the same filesystem, so the temp dir lives under
``silver_root``.

Why a separate module instead of reusing Bronze's writer? Two reasons:

1. Silver writes against :data:`SILVER_SCHEMA`; Bronze writes against
   :data:`BRONZE_SCHEMA`. Re-asserting the correct schema is the
   last cheap drift check we have before parquet bytes hit disk —
   sharing a writer would force a schema parameter and blur intent.
2. Silver's output path name (``silver_path`` on :class:`WriteResult`)
   surfaces in operator logs and manifest rows. Keeping a dedicated
   dataclass field avoids a ``bronze_path``-labelled field being
   reused for a Silver file and confusing operators triaging a run.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import polars as pl

from pipeline.errors import SilverError
from pipeline.silver.transform import assert_silver_schema

_COMPRESSION: Literal["zstd"] = "zstd"


@dataclass(frozen=True, slots=True)
class SilverWriteResult:
    """Outcome of a single Silver partition write."""

    silver_path: Path
    rows_written: int


def write_silver(
    df: pl.DataFrame,
    *,
    silver_root: Path,
    batch_id: str,
) -> SilverWriteResult:
    """Write ``df`` atomically to ``silver_root/batch_id=<id>/part-0.parquet``.

    - Schema is re-asserted against :data:`SILVER_SCHEMA` before writing.
    - A previous Silver partition for the same ``batch_id`` is replaced:
      temp dir → final dir via directory rename. The temp dir is
      cleaned on any failure so repeat runs start from a clean slate.
    - Row-count round-trip is checked by re-scanning the written file.
    """
    assert_silver_schema(df)

    silver_root.mkdir(parents=True, exist_ok=True)
    final_dir = silver_root / f"batch_id={batch_id}"
    tmp_dir = silver_root / f".tmp-batch_id={batch_id}"

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=False)

    tmp_file = tmp_dir / "part-0.parquet"
    final_file = final_dir / "part-0.parquet"

    try:
        df.write_parquet(tmp_file, compression=_COMPRESSION, statistics=True)
        if final_dir.exists():
            shutil.rmtree(final_dir)
        tmp_dir.replace(final_dir)
    except Exception as exc:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise SilverError(
            f"failed to write Silver partition for batch {batch_id!r}: {exc}"
        ) from exc

    rows_written = pl.scan_parquet(final_file).select(pl.len()).collect().item()
    if rows_written != df.height:
        raise SilverError(
            f"Silver write roundtrip mismatch: wrote {df.height} rows, "
            f"read back {rows_written} for batch {batch_id!r}"
        )

    return SilverWriteResult(silver_path=final_file, rows_written=rows_written)


__all__ = ["SilverWriteResult", "write_silver"]
