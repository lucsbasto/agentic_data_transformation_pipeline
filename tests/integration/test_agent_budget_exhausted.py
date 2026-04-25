"""End-to-end coverage for the F4 retry-budget contract (F4.18).

Pins the spec §3 F4-RF-04..F4-RF-08 invariants from the loop's
perspective rather than the executor's: a single batch that always
fails must produce ``retry_budget`` failure rows + 1 escalation,
and a sibling batch must keep running even when its peer escalates.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.agent._logging import AgentEventLogger, fixed_clock
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.planner import LayerRunner
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus
from pipeline.errors import SilverRegexMissError
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


_FIXED = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


def _logger(tmp_path: Path) -> AgentEventLogger:
    return AgentEventLogger(
        log_path=tmp_path / "agent.jsonl", clock=fixed_clock(_FIXED)
    )


def _write_source(path: Path, *, marker: str) -> str:
    rows = {col: [marker] for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"]
    rows["direction"] = ["inbound"]
    rows["message_type"] = ["text"]
    rows["status"] = ["delivered"]
    df = pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return compute_batch_identity(path).batch_id


def _classify_regex_break(
    _exc: BaseException, _layer: Layer, _batch_id: str
) -> ErrorKind:
    return ErrorKind.REGEX_BREAK


def _broken_fix_apply() -> None:
    """A fix that itself raises — the executor logs and retries the
    layer anyway. With the runner ALSO failing, the loop cycles
    through the full retry budget without recording any
    ``failures_recovered``."""
    raise RuntimeError("fix itself broke")


def _build_broken_fix(
    _exc: BaseException, _kind: ErrorKind, _layer: Layer, _batch_id: str
) -> Fix | None:
    return Fix(kind="regenerate_regex", description="x", apply=_broken_fix_apply)


def _agent_failure_count(db: ManifestDB, *, batch_id: str) -> int:
    conn = db._require_conn()
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM agent_failures WHERE batch_id = ?;",
            (batch_id,),
        ).fetchone()[0]
    )


def _agent_escalated_count(db: ManifestDB, *, batch_id: str) -> int:
    conn = db._require_conn()
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM agent_failures "
            "WHERE batch_id = ? AND escalated = 1;",
            (batch_id,),
        ).fetchone()[0]
    )


# ---------------------------------------------------------------------------
# Single-batch budget exhaustion.
# ---------------------------------------------------------------------------


def test_run_once_writes_three_failure_rows_then_escalates_once(
    db: ManifestDB, tmp_path: Path
) -> None:
    """`retry_budget=3`, runner always fails, fix is a no-op
    (doesn't actually heal anything) -> three `agent_failures` rows
    AND exactly one `escalated=1`."""
    raw = tmp_path / "raw"
    bid = _write_source(raw / "a.parquet", marker="a")

    def always_fails() -> None:
        raise SilverRegexMissError("permanent regex miss")

    runners = {Layer.SILVER: always_fails}
    escalations: list[tuple[ErrorKind, Layer, str]] = []
    result = run_once(
        manifest=db,
        source_root=raw,
        runners_for=lambda b: runners if b == bid else {},
        classify=_classify_regex_break,
        build_fix=_build_broken_fix,
        escalate=lambda exc, kind, layer, batch_id: escalations.append((kind, layer, batch_id)),
        lock=AgentLock(tmp_path / "agent.lock"),
        retry_budget=3,
        event_logger=_logger(tmp_path),
    )
    assert result.status is RunStatus.COMPLETED  # the loop itself completed
    assert result.escalations == 1
    assert result.failures_recovered == 0
    assert _agent_failure_count(db, batch_id=bid) == 3
    assert _agent_escalated_count(db, batch_id=bid) == 1
    # Escalator was invoked exactly once with the right tuple.
    assert escalations == [(ErrorKind.REGEX_BREAK, Layer.SILVER, bid)]


def test_run_once_respects_custom_retry_budget(db: ManifestDB, tmp_path: Path) -> None:
    """Bump budget to 5 -> 5 failure rows + 1 escalation."""
    raw = tmp_path / "raw"
    bid = _write_source(raw / "a.parquet", marker="b")

    def always_fails() -> None:
        raise SilverRegexMissError("permanent")

    runners = {Layer.SILVER: always_fails}
    run_once(
        manifest=db,
        source_root=raw,
        runners_for=lambda b: runners if b == bid else {},
        classify=_classify_regex_break,
        build_fix=_build_broken_fix,
        escalate=lambda *args: None,
        lock=AgentLock(tmp_path / "agent.lock"),
        retry_budget=5,
        event_logger=_logger(tmp_path),
    )
    assert _agent_failure_count(db, batch_id=bid) == 5
    assert _agent_escalated_count(db, batch_id=bid) == 1


# ---------------------------------------------------------------------------
# Per-batch isolation (F4-RF-08).
# ---------------------------------------------------------------------------


def test_failed_batch_does_not_block_sibling_batch(db: ManifestDB, tmp_path: Path) -> None:
    """Batch A burns its budget in Silver. Batch B's three layers
    must still run end-to-end."""
    raw = tmp_path / "raw"
    bid_a = _write_source(raw / "a.parquet", marker="a")
    bid_b = _write_source(raw / "b.parquet", marker="b")
    invoked_b: list[Layer] = []

    def make_failing() -> LayerRunner:
        def runner() -> None:
            raise SilverRegexMissError("permanent")
        return runner

    def make_passing(layer: Layer) -> LayerRunner:
        def runner() -> None:
            invoked_b.append(layer)
        return runner

    def runners_for(batch_id: str) -> Mapping[Layer, LayerRunner]:
        if batch_id == bid_a:
            return {Layer.SILVER: make_failing()}
        return {layer: make_passing(layer) for layer in (Layer.BRONZE, Layer.SILVER, Layer.GOLD)}

    result = run_once(
        manifest=db,
        source_root=raw,
        runners_for=runners_for,
        classify=_classify_regex_break,
        build_fix=_build_broken_fix,
        escalate=lambda *args: None,
        lock=AgentLock(tmp_path / "agent.lock"),
        retry_budget=3,
        event_logger=_logger(tmp_path),
    )
    assert result.batches_processed == 2
    assert result.escalations == 1
    # B still ran every layer in canonical order.
    assert invoked_b == [Layer.BRONZE, Layer.SILVER, Layer.GOLD]
    # B has zero failure rows.
    assert _agent_failure_count(db, batch_id=bid_b) == 0
    # A has the full budget worth of failure rows.
    assert _agent_failure_count(db, batch_id=bid_a) == 3
