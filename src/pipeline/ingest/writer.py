"""Atomic partitioned parquet writer for Bronze.

Writes go to a temporary directory alongside the target partition, and
only switch to the final location via ``os.replace`` after the file is
flushed. ``os.replace`` is atomic within the same filesystem, so the
temp dir must live under ``bronze_root``.
"""

from __future__ import annotations

# LEARN: two stdlib pieces worth knowing here:
#   - ``shutil`` = high-level filesystem ops (``rmtree`` = recursive
#     delete). Stdlib layering: ``os`` for low-level, ``pathlib`` for
#     object-oriented paths, ``shutil`` for batch tree work.
#   - ``Literal["zstd"]`` = typing trick that narrows the allowed
#     value to the exact string ``"zstd"``. A typo like ``"ztsd"``
#     fails mypy instead of silently defaulting.
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
    # LEARN: re-assert the schema just BEFORE the write. The transform
    # step already cast to the Bronze schema, but belt-and-braces: if a
    # future refactor slips in a column drop/add, this crashes loudly.
    assert_bronze_schema(df)

    # LEARN: ``mkdir(parents=True, exist_ok=True)`` = ``mkdir -p``. It
    # creates ``bronze_root`` and any missing ancestors without error
    # if they already exist.
    bronze_root.mkdir(parents=True, exist_ok=True)
    # LEARN: Hive-style partition directory name: ``batch_id=<id>``.
    # Polars, DuckDB, Spark, and friends all recognize this convention
    # and expose ``batch_id`` as a virtual column when reading.
    final_dir = bronze_root / f"batch_id={batch_id}"
    # LEARN: the leading ``.`` hides the temp dir on most filesystem
    # listings. Sibling-to-final placement matters because ``rename``
    # is only atomic on the SAME filesystem.
    tmp_dir = bronze_root / f".tmp-batch_id={batch_id}"

    # LEARN: if a previous run crashed mid-write, a stale temp dir is
    # lying around. Wipe it before we start, not after — leftover
    # ``.tmp-*`` directories would otherwise grow unbounded.
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    # LEARN: ``exist_ok=False`` makes this raise if the dir we just
    # removed somehow reappeared (race condition, concurrent writer).
    # Loud-fail is the right default here.
    tmp_dir.mkdir(parents=True, exist_ok=False)

    tmp_file = tmp_dir / "part-0.parquet"
    final_file = final_dir / "part-0.parquet"

    try:
        # LEARN: Polars writes parquet in one call. ``statistics=True``
        # tells it to embed row-group min/max metadata, which speeds
        # future filters (``WHERE timestamp > X``) by skipping row
        # groups whose range does not overlap the predicate.
        df.write_parquet(tmp_file, compression=_COMPRESSION, statistics=True)
        # Atomic swap: remove stale final dir (re-run scenario), then move.
        # LEARN: POSIX ``rename`` (what ``Path.replace`` calls) is atomic
        # for a single file across the same filesystem. Directory
        # replacement is NOT atomic if the target exists — we have to
        # delete first, then move. F1.5 review tracks this as a known
        # M3 gap; see REVIEWS/PENDING-FIXES.md Tier C.
        if final_dir.exists():
            shutil.rmtree(final_dir)
        tmp_dir.replace(final_dir)
    except Exception as exc:
        # LEARN: on ANY failure, nuke the temp dir so repeat runs start
        # clean. ``ignore_errors=True`` means the cleanup itself cannot
        # mask the original failure with a secondary error.
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise IngestError(
            f"failed to write Bronze partition for batch {batch_id!r}: {exc}"
        ) from exc

    # Re-scan the partition to confirm round-trip integrity.
    # LEARN: belt-and-braces verification: scan the file we just wrote
    # and count rows. ``pl.len()`` is Polars' row-count expression;
    # ``.item()`` pulls the scalar out of the 1x1 result frame.
    rows_written = pl.scan_parquet(final_file).select(pl.len()).collect().item()
    if rows_written != df.height:
        raise IngestError(
            f"Bronze write roundtrip mismatch: wrote {df.height} rows, "
            f"read back {rows_written} for batch {batch_id!r}"
        )

    return WriteResult(bronze_path=final_file, rows_written=rows_written)


__all__ = ["WriteResult", "write_bronze"]
