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
    batch_id: str = "",
    required_roots: list[Path] | None = None,
    *,
    now: datetime,
    stale_after: timedelta,
) -> bool:
    """Decide whether a single ``batches`` row needs work.

    Pending iff: missing entirely, terminal-failed, or
    stale-in-progress (crash debris).

    Self-healing (F4): when ``required_roots`` is non-empty, also flag
    a ``COMPLETED`` row as pending if any expected layer parquet is
    missing on disk. Callers that do not need this check (older tests,
    perf scenarios) pass an empty list / omit the kwarg and the row's
    status alone decides.
    """
    if row is None:
        return True

    if row.status == BATCH_STATUS_FAILED:
        return True

    if row.status == BATCH_STATUS_COMPLETED and required_roots:
        for root in required_roots:
            # Monta o caminho padrão: root / batch_id=xxx / part-0.parquet
            expected_file = root / f"batch_id={batch_id}" / "part-0.parquet"
            if not expected_file.exists():
                _LOG.warning(
                    "agent.observer.healing",
                    batch_id=batch_id,
                    layer_root=root.name,
                    reason="physical_file_missing"
                )
                return True
        return False

    return _is_stale(row, now=now, stale_after=stale_after)

def scan(
    manifest: ManifestDB,
    source_root: Path,
    silver_root: Path | None = None,
    gold_root: Path | None = None,
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
    silver_root:
        Directory for silver layer partitions to verify physical existence.
    gold_root:
        Directory for gold layer partitions to verify physical existence.
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

    required_roots = [r for r in (silver_root, gold_root) if r is not None]

    for batch_id, path in discover_source_batches(source_root):
        row = manifest.get_batch(batch_id)

        # Chamada atualizada passando o ID e as raízes para validação física
        if is_pending(row, batch_id, required_roots, now=wall, stale_after=stale_after):
            pending.append(batch_id)

            # Determina a razão para o log (se é healing ou status do banco)
            reason = "missing" if row is None else row.status
            if row and row.status == BATCH_STATUS_COMPLETED:
                reason = "self_healing_missing_files"

            _LOG.debug(
                "agent.observer.pending",
                batch_id=batch_id,
                source=str(path),
                reason=reason,
            )

    pending.sort()
    return pending
