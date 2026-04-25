"""Source vs manifest delta detector for the F4 agent loop
(design §4).

Walks the raw-source directory, derives a deterministic ``batch_id``
for each file, and returns the subset that still needs work — ie.
batches that are absent from the ``batches`` manifest table OR
recorded as ``FAILED`` OR stuck in ``IN_PROGRESS`` longer than the
configured staleness window.

The output is sorted lexicographically so the planner consumes
batches in a predictable order across runs.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog

from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.manifest import (
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_FAILED,
    BATCH_STATUS_IN_PROGRESS,
)
from pipeline.state.manifest import BatchRow, ManifestDB

_LOG = structlog.get_logger(__name__)

DEFAULT_STALE_AFTER_S: float = 3600.0
"""``IN_PROGRESS`` rows older than this are treated as crash debris
and re-scheduled for processing (design §4)."""


def discover_source_batches(source_root: Path) -> list[tuple[str, Path]]:
    """Enumerate ``source_root/*.parquet`` and return
    ``[(batch_id, path), ...]`` sorted by ``batch_id``.

    Missing directories collapse to an empty list so the agent can
    spin up against a fresh checkout without crashing on first scan.
    """
    if not source_root.exists():
        return []
    pairs: list[tuple[str, Path]] = []
    for path in sorted(source_root.glob("*.parquet")):
        identity = compute_batch_identity(path)
        pairs.append((identity.batch_id, path))
    pairs.sort(key=lambda pair: pair[0])
    return pairs


def _is_stale(
    row: BatchRow,
    *,
    now: datetime,
    stale_after: timedelta,
) -> bool:
    """True if ``row`` is ``IN_PROGRESS`` AND its ``started_at`` is
    older than ``now - stale_after``."""
    if row.status != BATCH_STATUS_IN_PROGRESS:
        return False
    try:
        started = datetime.fromisoformat(row.started_at)
    except ValueError:
        # Malformed timestamp — be conservative, treat as stale so a
        # human notices on the next run rather than the loop wedging.
        return True
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return (now - started) > stale_after


def is_pending(
    row: BatchRow | None,
    *,
    now: datetime,
    stale_after: timedelta,
) -> bool:
    """Decide whether a single ``batches`` row needs work.

    Pending iff: missing entirely, terminal-failed, or
    stale-in-progress (crash debris). A live ``IN_PROGRESS`` row
    means another process is actively driving the batch — leave it
    alone.
    """
    if row is None:
        return True
    if row.status == BATCH_STATUS_FAILED:
        return True
    if row.status == BATCH_STATUS_COMPLETED:
        return False
    return _is_stale(row, now=now, stale_after=stale_after)


def scan(
    manifest: ManifestDB,
    source_root: Path,
    *,
    now: datetime | None = None,
    stale_after_s: float = DEFAULT_STALE_AFTER_S,
) -> list[str]:
    """Return the list of pending ``batch_id`` values, sorted
    lexicographically.

    Parameters
    ----------
    manifest:
        Open :class:`ManifestDB` instance — the observer consults
        ``batches`` only; ``agent_runs`` / ``agent_failures`` are
        owned by the executor.
    source_root:
        Directory containing the raw parquet files. One parquet =
        one prospective batch.
    now:
        Injectable wall-clock for deterministic tests. Defaults to
        ``datetime.now(tz=UTC)``.
    stale_after_s:
        Window after which an ``IN_PROGRESS`` row is treated as
        crash debris and re-scheduled. Defaults to 1h (design §4).
    """
    wall = now or datetime.now(tz=UTC)
    stale_after = timedelta(seconds=stale_after_s)
    pending: list[str] = []
    for batch_id, path in discover_source_batches(source_root):
        row = manifest.get_batch(batch_id)
        if is_pending(row, now=wall, stale_after=stale_after):
            pending.append(batch_id)
            _LOG.debug(
                "agent.observer.pending",
                batch_id=batch_id,
                source=str(path),
                reason=("missing" if row is None else row.status),
            )
    pending.sort()
    return pending
