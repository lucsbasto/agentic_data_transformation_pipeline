"""Escalation sink for the F4 agent loop (design §9).

When the executor exhausts its retry budget — or hits an
``UNKNOWN`` failure that has no deterministic fix — it asks this
module to:

1. Append one JSON event to ``logs/agent.jsonl``.
2. Emit a short human-readable line to stdout via ``structlog``.
3. (Optional) flip the latest ``runs`` row for
   ``(batch_id, layer)`` to ``FAILED`` so downstream tooling sees
   the layer as terminally broken.

The flip on ``agent_failures.escalated`` itself is owned by the
executor (`mark_agent_failure_escalated`) — this module is the
human-visible side of the escalation.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import structlog

from pipeline.agent.types import ErrorKind, Layer
from pipeline.schemas.manifest import RUN_STATUS_FAILED
from pipeline.state.manifest import ManifestDB

_LOG = structlog.get_logger(__name__)

DEFAULT_LOG_PATH: Final[Path] = Path("logs/agent.jsonl")
"""Default sink for escalation events (design §9)."""

_LAST_ERROR_MSG_MAX: Final[int] = 512


SUGGESTED_FIX: Final[Mapping[ErrorKind, str]] = {
    ErrorKind.SCHEMA_DRIFT: (
        "Verifique delta de schema em logs/agent.jsonl; ajuste "
        "schemas/bronze.py se delta for legítimo."
    ),
    ErrorKind.REGEX_BREAK: (
        "Inspecione state/regex_overrides.json; rode "
        "pytest tests/unit/test_silver_regex_overrides.py."
    ),
    ErrorKind.PARTITION_MISSING: (
        "Cheque permissões em data/bronze/; rode "
        "python -m pipeline ingest --batch-id <id>."
    ),
    ErrorKind.OUT_OF_RANGE: (
        "Inspecione bronze.rejected; ajuste range em "
        "pipeline.silver.range."
    ),
    ErrorKind.UNKNOWN: (
        "Anexe last_error_msg ao ticket; agent não classificou "
        "esta falha."
    ),
}


def suggest_fix(kind: ErrorKind) -> str:
    """Return the human-readable next step for ``kind`` (design §9
    suggested-fix table). Always returns a string — every member of
    :class:`ErrorKind` has an entry."""
    return SUGGESTED_FIX[kind]


def build_payload(
    *,
    exc: BaseException,
    kind: ErrorKind,
    layer: Layer,
    batch_id: str,
    now: datetime | None = None,
) -> dict[str, str]:
    """Materialize the JSON event the escalator writes to
    ``agent.jsonl``. ``now`` is injectable so tests can pin ``ts``
    deterministically."""
    ts = (now or datetime.now(tz=UTC)).isoformat()
    return {
        "event": "escalation",
        "batch_id": batch_id,
        "layer": layer.value,
        "error_class": kind.value,
        "last_error_msg": str(exc)[:_LAST_ERROR_MSG_MAX],
        "suggested_fix": suggest_fix(kind),
        "ts": ts,
    }


def write_event(payload: Mapping[str, str], log_path: Path) -> None:
    """Append a single JSON event line to ``log_path``. Creates
    parent directories on first use; never opens the file across
    process boundaries (CLI is single-writer)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _set_run_failed(
    manifest: ManifestDB, *, batch_id: str, layer: Layer, now: datetime | None = None
) -> None:
    """Flip the latest ``runs`` row for ``(batch_id, layer)`` to
    ``FAILED`` so downstream tooling sees the layer as terminally
    broken. No-op when there is no ``runs`` row yet (the failure
    happened before insert_run could fire)."""
    row = manifest.get_latest_run(batch_id=batch_id, layer=layer.value)
    if row is None or row.status == RUN_STATUS_FAILED:
        return
    finished_at = (now or datetime.now(tz=UTC)).isoformat()
    manifest.mark_run_failed(
        run_id=row.run_id,
        finished_at=finished_at,
        duration_ms=0,
        error_type="EscalatedByAgent",
        error_message="agent loop exhausted retry budget",
    )


def escalate(
    *,
    exc: BaseException,
    kind: ErrorKind,
    layer: Layer,
    batch_id: str,
    log_path: Path = DEFAULT_LOG_PATH,
    manifest: ManifestDB | None = None,
    now: datetime | None = None,
) -> dict[str, str]:
    """Run the full escalation: build payload, append JSON event,
    emit stdout summary, optionally flip the latest run to FAILED.
    Returns the payload so callers (logger, tests) can inspect what
    was emitted."""
    payload = build_payload(
        exc=exc, kind=kind, layer=layer, batch_id=batch_id, now=now
    )
    write_event(payload, log_path)
    _LOG.warning(
        "agent.escalation",
        batch_id=batch_id,
        layer=layer.value,
        error_class=kind.value,
        suggested_fix=payload["suggested_fix"],
    )
    if manifest is not None:
        _set_run_failed(manifest, batch_id=batch_id, layer=layer, now=now)
    return payload


def make_escalator(
    *,
    log_path: Path = DEFAULT_LOG_PATH,
    manifest: ManifestDB | None = None,
    now: Callable[[], datetime] | None = None,
) -> Callable[[BaseException, ErrorKind, Layer, str], None]:
    """Convenience adapter that returns a curried escalator matching
    the :data:`pipeline.agent.executor.Escalator` signature so the
    loop module can wire it into :class:`Executor` directly."""
    def _escalator(
        exc: BaseException, kind: ErrorKind, layer: Layer, batch_id: str
    ) -> None:
        escalate(
            exc=exc,
            kind=kind,
            layer=layer,
            batch_id=batch_id,
            log_path=log_path,
            manifest=manifest,
            now=now() if now is not None else None,
        )

    return _escalator
