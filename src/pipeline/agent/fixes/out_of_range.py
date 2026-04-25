"""``OUT_OF_RANGE`` fix — acknowledge a quarantined Silver row
(F4 design §8.4).

Out-of-range failures already have F1's quarantine machinery routing
the offending row to ``silver_root/batch_id=<id>/rejected/part-0.parquet``.
This fix does NOT try to "fix" the value; it only confirms that the
quarantine evidence exists for the batch and lets the executor mark
the layer complete with the quarantine acknowledgment recorded as
the fix kind on ``agent_failures.last_fix_kind``.

Note: design §8.4 also calls for a ``had_quarantine`` flag on the
`runs` table. That requires extending the F1 manifest schema (DDL +
migration + writer) and is intentionally deferred to a follow-up
task — the validation + acknowledgment surface here is sufficient
for the executor (F4.11) and the demo (F4.17) to drive an
end-to-end recovery.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import structlog

from pipeline.agent.types import Fix
from pipeline.errors import AgentError

_LOG = structlog.get_logger(__name__)

_FIX_KIND: str = "acknowledge_quarantine"


class OutOfRangeFixError(AgentError):
    """Raised when no quarantine evidence exists for the batch — the
    fix refuses to acknowledge what isn't there."""


@dataclass(frozen=True)
class QuarantineAck:
    """Outcome of one acknowledgment attempt."""

    batch_id: str
    rejected_path: Path
    rejected_rows: int


def quarantine_partition_path(silver_root: Path, batch_id: str) -> Path:
    """Canonical layout for the per-batch quarantine parquet, mirrored
    from :mod:`pipeline.silver.quarantine`."""
    return silver_root / f"batch_id={batch_id}" / "rejected" / "part-0.parquet"


def quarantine_row_count(silver_root: Path, batch_id: str) -> int:
    """Count rows in the quarantine partition. Returns ``0`` when the
    file is missing — the caller decides whether that is an error."""
    path = quarantine_partition_path(silver_root, batch_id)
    if not path.exists():
        return 0
    return int(pl.scan_parquet(path).select(pl.len()).collect().item())


def acknowledge_quarantine(silver_root: Path, batch_id: str) -> QuarantineAck:
    """Validate that quarantine evidence exists, then return the
    :class:`QuarantineAck` record. Raises :class:`OutOfRangeFixError`
    when no rejected rows are on disk for the batch — refusing to
    acknowledge a missing quarantine keeps the executor from marking
    a real failure as recovered."""
    path = quarantine_partition_path(silver_root, batch_id)
    rows = quarantine_row_count(silver_root, batch_id)
    if rows == 0:
        raise OutOfRangeFixError(
            f"no quarantine evidence for batch_id={batch_id!r} at {path!s}; "
            "refusing to acknowledge"
        )
    _LOG.info(
        "agent.fix.out_of_range.acknowledged",
        batch_id=batch_id,
        rejected_path=str(path),
        rejected_rows=rows,
    )
    return QuarantineAck(batch_id=batch_id, rejected_path=path, rejected_rows=rows)


def build_fix(*, silver_root: Path, batch_id: str) -> Fix:
    """Wrap :func:`acknowledge_quarantine` in a :class:`Fix` the
    executor can apply directly. ``apply()`` raises
    :class:`OutOfRangeFixError` when the quarantine is absent so the
    executor treats it as a fix failure and re-enters its retry loop."""
    return Fix(
        kind=_FIX_KIND,
        description=(
            f"acknowledge Silver quarantine for batch_id={batch_id!r}"
        ),
        apply=lambda: _apply_fix(silver_root=silver_root, batch_id=batch_id),
        requires_llm=False,
    )


def _apply_fix(*, silver_root: Path, batch_id: str) -> None:
    acknowledge_quarantine(silver_root, batch_id)
