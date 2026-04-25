"""Production wiring for the F4 agent loop (WIRING-1 + WIRING-2).

`run_once` accepts a `runners_for(batch_id)` callable and a
`build_fix(...)` callable. The CLI ships placeholder implementations
that do nothing; this module replaces them with real adapters that
invoke the F1/F2/F3 layer entrypoints (`_run_ingest`, `_run_silver`,
`_run_gold`) and dispatch to the four `pipeline.agent.fixes.<kind>`
modules.

The adapters are intentionally thin — the layer entrypoints already
own manifest writes, idempotency guards, and structured logging
context, so the runner just generates a `run_id` per call and
delegates.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Final

from pipeline.agent.executor import FixBuilder
from pipeline.agent.fixes import out_of_range, partition_missing, schema_drift
from pipeline.agent.observer import discover_source_batches
from pipeline.agent.planner import LayerRunner
from pipeline.agent.types import ErrorKind, Fix, Layer
from pipeline.cli.gold import _run_gold
from pipeline.cli.ingest import _run_ingest
from pipeline.cli.silver import _run_silver
from pipeline.errors import AgentError
from pipeline.settings import Settings

_BRONZE_PARTITION_FILE: Final[str] = "part-0.parquet"


class RunnerWiringError(AgentError):
    """Raised when the agent cannot resolve the source parquet for a
    requested ``batch_id`` — happens when the source file vanished or
    a manifest entry is orphaned."""


def _bronze_partition_path(bronze_root: Path, batch_id: str) -> Path:
    return bronze_root / f"batch_id={batch_id}" / _BRONZE_PARTITION_FILE


def _resolve_source(source_root: Path, batch_id: str) -> Path:
    """Find the parquet under ``source_root`` whose
    :func:`compute_batch_identity` produces ``batch_id``. Mirrors
    the observer's discovery so `run_once` and the
    `partition_missing` fix agree on which file owns the batch."""
    for candidate_id, path in discover_source_batches(source_root):
        if candidate_id == batch_id:
            return path
    raise RunnerWiringError(
        f"no source parquet under {source_root!s} produces batch_id={batch_id!r}"
    )


# ---------------------------------------------------------------------------
# WIRING-1 — runners_for adapter.
# ---------------------------------------------------------------------------


def make_runners_for(
    *,
    source_root: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    settings: Settings,
) -> Callable[[str], Mapping[Layer, LayerRunner]]:
    """Return the `runners_for(batch_id)` callable that
    :func:`pipeline.agent.loop.run_once` consumes.

    Each layer is wrapped in a zero-arg closure that:

    1. resolves the source parquet for the batch (Bronze only),
    2. mints a fresh `run_id` so each layer attempt gets its own
       row in the `runs` table,
    3. delegates to the existing `_run_<layer>` private CLI helper.

    The layer helpers own context binding, manifest writes, and
    idempotency — the runner adapter is intentionally thin.
    """
    def _runners_for(batch_id: str) -> Mapping[Layer, LayerRunner]:
        def _bronze() -> None:
            source = _resolve_source(source_root, batch_id)
            _run_ingest(
                source=source, bronze_root=bronze_root, settings=settings
            )

        def _silver() -> None:
            _run_silver(
                run_id=uuid.uuid4().hex,
                batch_id=batch_id,
                bronze_root=bronze_root,
                silver_root=silver_root,
                settings=settings,
            )

        def _gold() -> None:
            _run_gold(
                run_id=uuid.uuid4().hex,
                batch_id=batch_id,
                silver_root=silver_root,
                gold_root=gold_root,
                settings=settings,
            )

        return {
            Layer.BRONZE: _bronze,
            Layer.SILVER: _silver,
            Layer.GOLD: _gold,
        }

    return _runners_for


# ---------------------------------------------------------------------------
# WIRING-2 — fix dispatcher.
# ---------------------------------------------------------------------------


def make_fix_builder(
    *,
    source_root: Path,
    bronze_root: Path,
    silver_root: Path,
) -> FixBuilder:
    """Return the `build_fix(exc, kind, layer, batch_id)` dispatcher.

    Maps each :class:`ErrorKind` to the corresponding fix module's
    `build_fix(...)` factory. ``REGEX_BREAK`` returns ``None``
    because the `regex_break` fix needs sample messages + a
    baseline that the executor's exception context does not carry —
    that wiring is a follow-up that requires harvesting samples
    from the failing Silver batch.
    """
    def _dispatch(
        _exc: BaseException,
        kind: ErrorKind,
        _layer: Layer,
        batch_id: str,
    ) -> Fix | None:
        if kind is ErrorKind.SCHEMA_DRIFT:
            return schema_drift.build_fix(
                _bronze_partition_path(bronze_root, batch_id)
            )
        if kind is ErrorKind.PARTITION_MISSING:
            try:
                source = _resolve_source(source_root, batch_id)
            except RunnerWiringError:
                # Source itself is gone — nothing to recreate from.
                return None
            return partition_missing.build_fix(
                source=source, bronze_root=bronze_root, batch_id=batch_id
            )
        if kind is ErrorKind.OUT_OF_RANGE:
            return out_of_range.build_fix(
                silver_root=silver_root, batch_id=batch_id
            )
        # REGEX_BREAK + UNKNOWN -> no deterministic fix from this dispatcher.
        return None

    return _dispatch
