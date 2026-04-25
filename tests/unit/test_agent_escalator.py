"""Coverage for the F4 agent escalator (F4.12)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.agent.escalator import (
    DEFAULT_LOG_PATH,
    SUGGESTED_FIX,
    build_payload,
    escalate,
    make_escalator,
    suggest_fix,
    write_event,
)
from pipeline.agent.types import ErrorKind, Layer
from pipeline.state.manifest import ManifestDB

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def db() -> Iterator[ManifestDB]:
    manifest = ManifestDB(":memory:").open()
    try:
        yield manifest
    finally:
        manifest.close()


_FIXED_NOW = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


def _seed_run(db: ManifestDB, *, batch_id: str, layer: str) -> str:
    db.insert_batch(
        batch_id=batch_id,
        source_path="src",
        source_hash="h",
        source_mtime=0,
        started_at="2026-04-25T11:00:00+00:00",
    )
    run_id = f"{batch_id}-{layer}"
    db.insert_run(
        run_id=run_id,
        batch_id=batch_id,
        layer=layer,
        started_at="2026-04-25T11:01:00+00:00",
    )
    return run_id


# ---------------------------------------------------------------------------
# suggest_fix / SUGGESTED_FIX table.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", list(ErrorKind))
def test_suggest_fix_has_entry_for_every_error_kind(kind: ErrorKind) -> None:
    """Every member of :class:`ErrorKind` must map to a non-empty
    operator hint — otherwise a future enum addition will surface
    silently as missing escalation guidance."""
    assert isinstance(suggest_fix(kind), str)
    assert suggest_fix(kind).strip() != ""


def test_suggested_fix_table_size_matches_error_kind() -> None:
    assert set(SUGGESTED_FIX) == set(ErrorKind)


# ---------------------------------------------------------------------------
# build_payload.
# ---------------------------------------------------------------------------


def test_build_payload_carries_canonical_fields() -> None:
    payload = build_payload(
        exc=RuntimeError("boom"),
        kind=ErrorKind.REGEX_BREAK,
        layer=Layer.SILVER,
        batch_id="bid01",
        now=_FIXED_NOW,
    )
    assert payload == {
        "event": "escalation",
        "batch_id": "bid01",
        "layer": "silver",
        "error_class": "regex_break",
        "last_error_msg": "boom",
        "suggested_fix": SUGGESTED_FIX[ErrorKind.REGEX_BREAK],
        "ts": "2026-04-25T12:00:00+00:00",
    }


def test_build_payload_truncates_long_error_msg() -> None:
    """Same 512-char cap as ``agent_failures.last_error_msg`` so a
    runaway traceback never bloats the JSONL file."""
    payload = build_payload(
        exc=RuntimeError("x" * 5000),
        kind=ErrorKind.UNKNOWN,
        layer=Layer.GOLD,
        batch_id="bid01",
        now=_FIXED_NOW,
    )
    assert len(payload["last_error_msg"]) == 512


# ---------------------------------------------------------------------------
# write_event.
# ---------------------------------------------------------------------------


def test_write_event_appends_one_jsonl_line(tmp_path: Path) -> None:
    log_path = tmp_path / "logs" / "agent.jsonl"
    write_event({"event": "escalation", "batch_id": "bid01"}, log_path)
    write_event({"event": "escalation", "batch_id": "bid02"}, log_path)
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"event": "escalation", "batch_id": "bid01"}
    assert json.loads(lines[1]) == {"event": "escalation", "batch_id": "bid02"}


def test_write_event_creates_parent_directories(tmp_path: Path) -> None:
    log_path = tmp_path / "deep" / "nested" / "agent.jsonl"
    write_event({"event": "escalation"}, log_path)
    assert log_path.exists()


# ---------------------------------------------------------------------------
# escalate end-to-end.
# ---------------------------------------------------------------------------


def test_escalate_writes_jsonl_and_returns_payload(tmp_path: Path) -> None:
    log_path = tmp_path / "agent.jsonl"
    payload = escalate(
        exc=RuntimeError("boom"),
        kind=ErrorKind.REGEX_BREAK,
        layer=Layer.SILVER,
        batch_id="bid01",
        log_path=log_path,
        now=_FIXED_NOW,
    )
    assert payload["event"] == "escalation"
    line = log_path.read_text(encoding="utf-8").strip()
    assert json.loads(line) == payload


def test_escalate_flips_latest_run_to_failed(db: ManifestDB, tmp_path: Path) -> None:
    """When a `manifest` is supplied the latest run for
    ``(batch_id, layer)`` becomes FAILED so downstream tooling sees
    the layer as terminally broken."""
    run_id = _seed_run(db, batch_id="bid01", layer="silver")
    escalate(
        exc=RuntimeError("boom"),
        kind=ErrorKind.REGEX_BREAK,
        layer=Layer.SILVER,
        batch_id="bid01",
        log_path=tmp_path / "agent.jsonl",
        manifest=db,
        now=_FIXED_NOW,
    )
    row = db.get_run(run_id)
    assert row is not None
    assert row.status == "FAILED"
    assert row.error_type == "EscalatedByAgent"


def test_escalate_is_no_op_on_runs_when_no_run_exists(db: ManifestDB, tmp_path: Path) -> None:
    """Failure happened before `insert_run` could fire — escalating
    must not crash, just skip the runs flip."""
    db.insert_batch(
        batch_id="bid01",
        source_path="src",
        source_hash="h",
        source_mtime=0,
        started_at="2026-04-25T11:00:00+00:00",
    )
    payload = escalate(
        exc=RuntimeError("boom"),
        kind=ErrorKind.UNKNOWN,
        layer=Layer.SILVER,
        batch_id="bid01",
        log_path=tmp_path / "agent.jsonl",
        manifest=db,
        now=_FIXED_NOW,
    )
    assert payload["event"] == "escalation"


def test_escalate_does_not_double_flip_an_already_failed_run(
    db: ManifestDB, tmp_path: Path
) -> None:
    run_id = _seed_run(db, batch_id="bid01", layer="silver")
    db.mark_run_failed(
        run_id=run_id,
        finished_at="2026-04-25T11:30:00+00:00",
        duration_ms=10,
        error_type="OriginalError",
        error_message="initial failure",
    )
    escalate(
        exc=RuntimeError("boom"),
        kind=ErrorKind.REGEX_BREAK,
        layer=Layer.SILVER,
        batch_id="bid01",
        log_path=tmp_path / "agent.jsonl",
        manifest=db,
        now=_FIXED_NOW,
    )
    row = db.get_run(run_id)
    assert row is not None
    # Original error_type preserved — escalator did not overwrite.
    assert row.error_type == "OriginalError"


# ---------------------------------------------------------------------------
# make_escalator (curried adapter).
# ---------------------------------------------------------------------------


def test_make_escalator_returns_executor_compatible_callable(
    db: ManifestDB, tmp_path: Path
) -> None:
    """The Executor expects a 4-arg callable; ``make_escalator``
    must return one. The exception/kind/layer/batch_id positional
    order matches `pipeline.agent.executor.Escalator`."""
    log_path = tmp_path / "agent.jsonl"
    fn = make_escalator(log_path=log_path, now=lambda: _FIXED_NOW)
    fn(RuntimeError("boom"), ErrorKind.REGEX_BREAK, Layer.SILVER, "bid01")
    line = log_path.read_text(encoding="utf-8").strip()
    payload = json.loads(line)
    assert payload["batch_id"] == "bid01"
    assert payload["layer"] == "silver"
    assert payload["error_class"] == "regex_break"


# ---------------------------------------------------------------------------
# Defaults.
# ---------------------------------------------------------------------------


def test_default_log_path_matches_design() -> None:
    """``logs/agent.jsonl`` per design §9 — pin so a quiet rename
    surfaces in CI."""
    assert Path("logs/agent.jsonl") == DEFAULT_LOG_PATH
