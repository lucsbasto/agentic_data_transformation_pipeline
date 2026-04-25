"""Integration coverage for the F4 ``run_once`` orchestrator
(F4.14).

These tests exercise the full wiring (lock + manifest + observer +
planner + executor + escalator + event logger) but stub out the
real layer entrypoints with simple lambdas — the goal is to prove
the orchestration is correct, not to re-test each component."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.agent._logging import (
    EVENT_BATCH_STARTED,
    EVENT_LAYER_COMPLETED,
    EVENT_LAYER_STARTED,
    EVENT_LOOP_STARTED,
    EVENT_LOOP_STOPPED,
    AgentEventLogger,
    fixed_clock,
)
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.planner import LayerRunner
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS
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


def _write_source_parquet(path: Path, *, marker: str) -> str:
    rows = {col: [marker] for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"]
    rows["direction"] = ["inbound"]
    rows["message_type"] = ["text"]
    rows["status"] = ["delivered"]
    df = pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return compute_batch_identity(path).batch_id


def _noop_classifier(exc: BaseException, layer: Layer, batch_id: str) -> ErrorKind:
    return ErrorKind.UNKNOWN


def _no_fix(
    exc: BaseException, kind: ErrorKind, layer: Layer, batch_id: str
) -> Fix | None:
    return None


def _record_escalations(
    log: list[tuple[ErrorKind, Layer, str]],
) -> Callable[[BaseException, ErrorKind, Layer, str], None]:
    def _inner(exc: BaseException, kind: ErrorKind, layer: Layer, batch_id: str) -> None:
        log.append((kind, layer, batch_id))

    return _inner


def _make_runner_factory(
    *, results_per_batch: dict[str, dict[Layer, Callable[[], None]]]
) -> Callable[[str], Mapping[Layer, LayerRunner]]:
    def _factory(batch_id: str) -> Mapping[Layer, LayerRunner]:
        return results_per_batch.get(batch_id, {})

    return _factory


_FIXED = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


def _logger(tmp_path: Path) -> AgentEventLogger:
    return AgentEventLogger(
        log_path=tmp_path / "logs" / "agent.jsonl",
        clock=fixed_clock(_FIXED),
    )


def _read_events(tmp_path: Path) -> list[dict]:
    log_path = tmp_path / "logs" / "agent.jsonl"
    if not log_path.exists():
        return []
    return [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]


# ---------------------------------------------------------------------------
# No-op pass on a clean filesystem.
# ---------------------------------------------------------------------------


def test_run_once_returns_completed_when_no_pending_batches(
    db: ManifestDB, tmp_path: Path
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    lock = AgentLock(tmp_path / "agent.lock")
    result = run_once(
        manifest=db,
        source_root=raw,
        runners_for=lambda _: {},
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_record_escalations([]),
        lock=lock,
        event_logger=_logger(tmp_path),
    )
    assert result.status is RunStatus.COMPLETED
    assert result.batches_processed == 0
    assert result.failures_recovered == 0
    assert result.escalations == 0


def test_run_once_releases_lock_on_clean_run(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    lock_path = tmp_path / "agent.lock"
    run_once(
        manifest=db,
        source_root=raw,
        runners_for=lambda _: {},
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_record_escalations([]),
        lock=AgentLock(lock_path),
        event_logger=_logger(tmp_path),
    )
    assert not lock_path.exists()


# ---------------------------------------------------------------------------
# Happy path with one batch + three layers all succeeding.
# ---------------------------------------------------------------------------


def test_run_once_drives_all_layers_for_one_batch(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    batch_id = _write_source_parquet(raw / "a.parquet", marker="a")
    invoked: list[Layer] = []

    def make_runner(layer: Layer) -> LayerRunner:
        return lambda: invoked.append(layer)

    runners = {
        Layer.BRONZE: make_runner(Layer.BRONZE),
        Layer.SILVER: make_runner(Layer.SILVER),
        Layer.GOLD: make_runner(Layer.GOLD),
    }
    result = run_once(
        manifest=db,
        source_root=raw,
        runners_for=lambda bid: runners if bid == batch_id else {},
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_record_escalations([]),
        lock=AgentLock(tmp_path / "agent.lock"),
        event_logger=_logger(tmp_path),
    )
    assert result.status is RunStatus.COMPLETED
    assert result.batches_processed == 1
    assert invoked == [Layer.BRONZE, Layer.SILVER, Layer.GOLD]


def test_run_once_emits_canonical_event_sequence(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    batch_id = _write_source_parquet(raw / "a.parquet", marker="a")
    runners = {Layer.BRONZE: lambda: None}
    run_once(
        manifest=db,
        source_root=raw,
        runners_for=lambda bid: runners if bid == batch_id else {},
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_record_escalations([]),
        lock=AgentLock(tmp_path / "agent.lock"),
        event_logger=_logger(tmp_path),
    )
    event_names = [event["event"] for event in _read_events(tmp_path)]
    # The orchestrator emits at least these milestones in order.
    expected_subsequence = [
        EVENT_LOOP_STARTED,
        EVENT_BATCH_STARTED,
        EVENT_LAYER_STARTED,
        EVENT_LAYER_COMPLETED,
        EVENT_LOOP_STOPPED,
    ]
    # Sub-sequence check (other events may be interleaved).
    indices = [event_names.index(name) for name in expected_subsequence]
    assert indices == sorted(indices)


def test_run_once_writes_agent_run_row_with_completed_status(
    db: ManifestDB, tmp_path: Path
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    result = run_once(
        manifest=db,
        source_root=raw,
        runners_for=lambda _: {},
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_record_escalations([]),
        lock=AgentLock(tmp_path / "agent.lock"),
        event_logger=_logger(tmp_path),
    )
    conn = db._require_conn()
    row = conn.execute(
        "SELECT status, batches_processed, failures_recovered, escalations "
        "FROM agent_runs WHERE agent_run_id = ?;",
        (result.agent_run_id,),
    ).fetchone()
    assert row["status"] == "COMPLETED"
    assert row["batches_processed"] == 0
    assert row["failures_recovered"] == 0
    assert row["escalations"] == 0


# ---------------------------------------------------------------------------
# Failure isolation per F4-RF-08 — one bad batch must NOT block siblings.
# ---------------------------------------------------------------------------


def test_run_once_isolates_failure_per_batch(db: ManifestDB, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    bid_a = _write_source_parquet(raw / "a.parquet", marker="a")
    bid_b = _write_source_parquet(raw / "b.parquet", marker="b")
    invoked: list[tuple[str, Layer]] = []

    def make_failing(batch_id: str, layer: Layer) -> LayerRunner:
        def _runner() -> None:
            invoked.append((batch_id, layer))
            raise RuntimeError(f"{batch_id} {layer.value} broke")
        return _runner

    def make_passing(batch_id: str, layer: Layer) -> LayerRunner:
        def _runner() -> None:
            invoked.append((batch_id, layer))
        return _runner

    def runners_for(batch_id: str) -> Mapping[Layer, LayerRunner]:
        if batch_id == bid_a:
            # Bronze fails -> escalation -> Silver/Gold skipped for A.
            return {Layer.BRONZE: make_failing(batch_id, Layer.BRONZE)}
        return {
            Layer.BRONZE: make_passing(batch_id, Layer.BRONZE),
            Layer.SILVER: make_passing(batch_id, Layer.SILVER),
            Layer.GOLD: make_passing(batch_id, Layer.GOLD),
        }

    escalations: list[tuple[ErrorKind, Layer, str]] = []
    result = run_once(
        manifest=db,
        source_root=raw,
        runners_for=runners_for,
        classify=_noop_classifier,
        build_fix=_no_fix,
        escalate=_record_escalations(escalations),
        lock=AgentLock(tmp_path / "agent.lock"),
        event_logger=_logger(tmp_path),
    )
    # Both batches were processed; A escalated; B completed normally.
    assert result.batches_processed == 2
    assert result.escalations == 1
    assert escalations == [(ErrorKind.UNKNOWN, Layer.BRONZE, bid_a)]
    # B's three layers all ran.
    b_invocations = [layer for bid, layer in invoked if bid == bid_b]
    assert b_invocations == [Layer.BRONZE, Layer.SILVER, Layer.GOLD]


# ---------------------------------------------------------------------------
# Lock contention.
# ---------------------------------------------------------------------------


def test_run_once_releases_lock_even_when_runner_raises_baseexception(
    db: ManifestDB, tmp_path: Path
) -> None:
    """A non-KeyboardInterrupt BaseException must still leave the
    lock cleaned up. The agent_run row is sealed FAILED."""
    raw = tmp_path / "raw"
    bid_a = _write_source_parquet(raw / "a.parquet", marker="a")
    lock_path = tmp_path / "agent.lock"

    def explode() -> None:
        raise SystemExit(1)

    runners = {Layer.BRONZE: explode}
    with pytest.raises(SystemExit):
        run_once(
            manifest=db,
            source_root=raw,
            runners_for=lambda bid: runners if bid == bid_a else {},
            classify=_noop_classifier,
            build_fix=_no_fix,
            escalate=_record_escalations([]),
            lock=AgentLock(lock_path),
            event_logger=_logger(tmp_path),
        )
    assert not lock_path.exists()
    conn = db._require_conn()
    statuses = [row["status"] for row in conn.execute("SELECT status FROM agent_runs;").fetchall()]
    assert statuses == ["FAILED"]


def test_run_once_seals_agent_run_as_interrupted_on_keyboard_interrupt(
    db: ManifestDB, tmp_path: Path
) -> None:
    """Spec F4-RF-09: Ctrl-C / SIGINT mid-iteration must produce
    `RunStatus.INTERRUPTED` on the agent_run row, not FAILED — the
    spec separates 'operator stopped me' from 'I crashed' so the
    observer's next sweep can decide whether to retry."""
    raw = tmp_path / "raw"
    bid_a = _write_source_parquet(raw / "a.parquet", marker="a")
    lock_path = tmp_path / "agent.lock"

    def explode() -> None:
        raise KeyboardInterrupt

    runners = {Layer.BRONZE: explode}
    with pytest.raises(KeyboardInterrupt):
        run_once(
            manifest=db,
            source_root=raw,
            runners_for=lambda bid: runners if bid == bid_a else {},
            classify=_noop_classifier,
            build_fix=_no_fix,
            escalate=_record_escalations([]),
            lock=AgentLock(lock_path),
            event_logger=_logger(tmp_path),
        )
    assert not lock_path.exists()
    conn = db._require_conn()
    statuses = [row["status"] for row in conn.execute("SELECT status FROM agent_runs;").fetchall()]
    assert statuses == ["INTERRUPTED"]
