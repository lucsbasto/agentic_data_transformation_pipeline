"""``PARTITION_MISSING`` fix — re-emit a Bronze partition that vanished
from disk (F4 design §8.3).

The repair re-runs the F1 ingest pipeline (scan -> transform -> write)
against the original source parquet so the Bronze partition is
restored byte-equivalent to its original landing. The fix is
idempotent: when the partition already exists at apply time it
returns a no-op marker, lets the executor retry the layer, and
disappears.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import structlog

from pipeline.agent.types import Fix
from pipeline.errors import AgentError
from pipeline.ingest.batch import compute_batch_identity
from pipeline.ingest.reader import scan_source, validate_source_columns
from pipeline.ingest.transform import collect_bronze, transform_to_bronze
from pipeline.ingest.writer import write_bronze

_LOG = structlog.get_logger(__name__)

_FIX_KIND: str = "recreate_partition"


class PartitionMissingFixError(AgentError):
    """Raised when the partition cannot be recreated — typically a
    source mismatch between the path supplied to the fix and the
    ``batch_id`` the executor expects."""


@dataclass(frozen=True)
class PartitionMissingResult:
    """Outcome of one repair attempt."""

    batch_id: str
    bronze_path: Path
    rows_written: int
    no_op: bool
    """``True`` when the partition already existed and the fix did
    not need to rewrite anything."""


def recreate_partition(
    *,
    source: Path,
    bronze_root: Path,
    batch_id: str,
    now: datetime | None = None,
) -> PartitionMissingResult:
    """Re-emit ``bronze_root/batch_id=<id>/part-0.parquet`` from
    ``source``.

    Idempotent: if the parquet file already exists the function
    returns immediately with ``no_op=True``. Raises
    :class:`PartitionMissingFixError` if the supplied ``source`` does
    not produce the requested ``batch_id`` — refusing to mix data
    sources is the entire reason the original ingest derived the
    batch_id from the source hash.
    """
    final_path = bronze_root / f"batch_id={batch_id}" / "part-0.parquet"
    if final_path.exists():
        return PartitionMissingResult(
            batch_id=batch_id,
            bronze_path=final_path,
            rows_written=0,
            no_op=True,
        )

    identity = compute_batch_identity(source)
    if identity.batch_id != batch_id:
        raise PartitionMissingFixError(
            f"source {source!s} produces batch_id={identity.batch_id!r}, "
            f"refusing to recreate {batch_id!r} from a different file"
        )

    ingested_at = now or datetime.now(tz=UTC)
    lf = scan_source(source)
    validate_source_columns(lf)
    typed = transform_to_bronze(
        lf,
        batch_id=batch_id,
        source_hash=identity.source_hash,
        ingested_at=ingested_at,
    )
    df = collect_bronze(typed)
    result = write_bronze(df, bronze_root=bronze_root, batch_id=batch_id)
    _LOG.info(
        "agent.fix.partition_missing.recreated",
        batch_id=batch_id,
        bronze_path=str(result.bronze_path),
        rows_written=result.rows_written,
    )
    return PartitionMissingResult(
        batch_id=batch_id,
        bronze_path=result.bronze_path,
        rows_written=result.rows_written,
        no_op=False,
    )


def build_fix(
    *,
    source: Path,
    bronze_root: Path,
    batch_id: str,
) -> Fix:
    """Wrap :func:`recreate_partition` in a :class:`Fix` the executor
    can apply directly. ``apply()`` returns ``None`` to keep the
    callable contract."""
    return Fix(
        kind=_FIX_KIND,
        description=(
            f"recreate Bronze partition batch_id={batch_id!r} from {source!s}"
        ),
        apply=lambda: _apply_fix(
            source=source, bronze_root=bronze_root, batch_id=batch_id
        ),
        requires_llm=False,
    )


def _apply_fix(*, source: Path, bronze_root: Path, batch_id: str) -> None:
    recreate_partition(source=source, bronze_root=bronze_root, batch_id=batch_id)
