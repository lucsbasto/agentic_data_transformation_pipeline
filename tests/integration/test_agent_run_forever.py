"""Integration coverage for the F4 ``run_forever`` loop (F4.15)."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.agent._logging import AgentEventLogger, fixed_clock
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import DEFAULT_LOOP_INTERVAL_S, run_forever
from pipeline.agent.planner import LayerRunner
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
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


_FIXED = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


def _logger(tmp_path: Path) -> AgentEventLogger:
    return AgentEventLogger(
        log_path=tmp_path / "agent.jsonl", clock=fixed_clock(_FIXED)
    )


def _noop_classifier(exc: BaseException, layer: Layer, batch_id: str) -> ErrorKind:
    return ErrorKind.UNKNOWN


def _no_fix(
    exc: BaseException, kind: ErrorKind, layer: Layer, batch_id: str
) -> Fix | None:
    return None


def _no_escalate(
    exc: BaseException, kind: ErrorKind, layer: Layer, batch_id: str
) -> None:
    return None


def _no_runners(_: str) -> Mapping[Layer, LayerRunner]:
    return {}


# ---------------------------------------------------------------------------
# max_iters caps the loop.
# ---------------------------------------------------------------------------


def test_run_forever_stops_after_max_iters(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    results = run_forever(
        manifest=db,
        source_root=raw,
        runners_for=_no_runners,
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_no_escalate,
        interval=0.0,  # don't sleep between iterations in tests
        max_iters=3,
        lock=AgentLock(tmp_path / "agent.lock"),
        event_logger=_logger(tmp_path),
    )
    assert len(results) == 3
    assert all(r.status is RunStatus.COMPLETED for r in results)


def test_run_forever_returns_empty_when_max_iters_zero(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    results = run_forever(
        manifest=db,
        source_root=raw,
        runners_for=_no_runners,
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_no_escalate,
        interval=0.0,
        max_iters=0,
        lock=AgentLock(tmp_path / "agent.lock"),
        event_logger=_logger(tmp_path),
    )
    assert results == []


# ---------------------------------------------------------------------------
# stop_event cancels mid-loop without waiting out the interval.
# ---------------------------------------------------------------------------


def test_run_forever_stops_when_event_set_between_iterations(
    db: ManifestDB, tmp_path: Path
) -> None:
    """Set the cancel event AFTER the first iteration completes; the
    loop must stop instead of sleeping for the full interval."""
    raw = tmp_path / "raw"
    raw.mkdir()
    cancel = threading.Event()
    iterations: list[int] = []
    real_no_runners = _no_runners

    def runners_for(batch_id: str) -> Mapping[Layer, LayerRunner]:
        iterations.append(1)
        return real_no_runners(batch_id)

    # Schedule cancellation 0.1s after kickoff so the first iteration
    # finishes (no batches -> instant) and the .wait(60) is short-circuited.
    def _cancel() -> None:
        time.sleep(0.05)
        cancel.set()

    cancel_thread = threading.Thread(target=_cancel, daemon=True)
    cancel_thread.start()

    start = time.time()
    results = run_forever(
        manifest=db,
        source_root=raw,
        runners_for=runners_for,
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_no_escalate,
        interval=60.0,  # would block for a minute without cancel
        stop_event=cancel,
        lock=AgentLock(tmp_path / "agent.lock"),
        event_logger=_logger(tmp_path),
    )
    elapsed = time.time() - start
    cancel_thread.join(timeout=1.0)

    assert len(results) >= 1
    assert elapsed < 5.0, f"loop did not honor stop_event quickly enough: {elapsed:.2f}s"


def test_run_forever_does_not_sleep_after_last_max_iter(
    db: ManifestDB, tmp_path: Path
) -> None:
    """``max_iters=1`` means one iteration and zero inter-iteration
    sleeps — even with a long ``interval`` the call should return
    immediately."""
    raw = tmp_path / "raw"
    raw.mkdir()
    start = time.time()
    results = run_forever(
        manifest=db,
        source_root=raw,
        runners_for=_no_runners,
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_no_escalate,
        interval=10.0,  # would dominate elapsed time if it slept
        max_iters=1,
        lock=AgentLock(tmp_path / "agent.lock"),
        event_logger=_logger(tmp_path),
    )
    elapsed = time.time() - start
    assert len(results) == 1
    assert elapsed < 1.0, f"loop slept after final max_iter: {elapsed:.2f}s"


# ---------------------------------------------------------------------------
# Defaults.
# ---------------------------------------------------------------------------


def test_default_loop_interval_matches_design() -> None:
    """Spec §7 D5 fixes the default at 60s — pin so a quiet bump
    surfaces in CI."""
    assert DEFAULT_LOOP_INTERVAL_S == 60.0
