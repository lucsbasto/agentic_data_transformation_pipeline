"""Sequential plan builder for the F4 agent loop (design §5).

Given a single ``batch_id`` and a mapping of layer -> runner callable
(supplied by ``loop.py`` so each runner is already bound to its
configuration), the planner consults the manifest to determine which
layers still need work and returns the subset in the canonical
``bronze -> silver -> gold`` order.

The planner does NOT invoke runners — that is the executor's job. By
keeping the planner pure (no side effects, no logging beyond debug)
the executor can wrap each step in retry / fix logic without the
planner observing the results.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Final

import structlog

from pipeline.agent.types import Layer
from pipeline.schemas.manifest import RUN_STATUS_COMPLETED
from pipeline.state.manifest import ManifestDB

_LOG = structlog.get_logger(__name__)

LAYER_ORDER: Final[tuple[Layer, ...]] = (Layer.BRONZE, Layer.SILVER, Layer.GOLD)
"""Canonical execution order. Pinned here so a future reorder
surfaces as a test failure."""

LayerRunner = Callable[[], None]
"""Zero-arg callable the executor invokes for a layer. Bound to its
``batch_id`` and configuration by the caller (``loop.py``)."""


def is_layer_completed(
    manifest: ManifestDB, *, batch_id: str, layer: Layer
) -> bool:
    """``True`` when the most recent ``runs`` row for
    ``(batch_id, layer)`` is ``COMPLETED``. Earlier ``FAILED`` /
    ``IN_PROGRESS`` attempts are ignored — only the latest matters
    because the layer entrypoints are idempotent."""
    row = manifest.get_latest_run(batch_id=batch_id, layer=layer.value)
    return row is not None and row.status == RUN_STATUS_COMPLETED


def plan(
    batch_id: str,
    *,
    manifest: ManifestDB,
    runners: Mapping[Layer, LayerRunner],
) -> list[tuple[Layer, LayerRunner]]:
    """Return the subset of ``runners`` that still needs to run for
    ``batch_id``, in canonical layer order. Missing runners for
    pending layers are silently skipped — the caller is expected to
    supply every layer it intends the agent to drive.
    """
    steps: list[tuple[Layer, LayerRunner]] = []
    for layer in LAYER_ORDER:
        if layer not in runners:
            continue
        if is_layer_completed(manifest, batch_id=batch_id, layer=layer):
            _LOG.debug(
                "agent.planner.skip_completed",
                batch_id=batch_id,
                layer=layer.value,
            )
            continue
        steps.append((layer, runners[layer]))
    return steps
