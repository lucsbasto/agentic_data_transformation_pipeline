"""Coverage for the agent-loop manifest methods (F4.2).

These tests exercise the six new ``ManifestDB`` methods that back the
F4 retry / escalation engine: ``start_agent_run``, ``end_agent_run``,
``record_agent_failure``, ``record_agent_fix``,
``mark_agent_failure_escalated``, ``count_agent_attempts``. They run
against an in-memory SQLite so each test gets a fresh schema.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from pipeline.errors import ManifestError
from pipeline.state.manifest import ManifestDB

# ---------------------------------------------------------------------------
# Fixtures + helpers.
# ---------------------------------------------------------------------------


@pytest.fixture
def db() -> Iterator[ManifestDB]:
    manifest = ManifestDB(":memory:").open()
    try:
        yield manifest
    finally:
        manifest.close()


def _row_count(db: ManifestDB, table: str) -> int:
    conn = db._require_conn()
    return int(conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0])


# ---------------------------------------------------------------------------
# start_agent_run / end_agent_run.
# ---------------------------------------------------------------------------


def test_start_agent_run_inserts_in_progress_row(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run(now_iso="2026-04-25T10:00:00Z")
    conn = db._require_conn()
    row = conn.execute(
        "SELECT agent_run_id, started_at, ended_at, status, batches_processed, "
        "failures_recovered, escalations FROM agent_runs WHERE agent_run_id = ?;",
        (agent_run_id,),
    ).fetchone()
    assert row is not None
    assert row["agent_run_id"] == agent_run_id
    assert row["started_at"] == "2026-04-25T10:00:00Z"
    assert row["ended_at"] is None
    assert row["status"] == "IN_PROGRESS"
    assert row["batches_processed"] == 0
    assert row["failures_recovered"] == 0
    assert row["escalations"] == 0


def test_start_agent_run_returns_unique_ids(db: ManifestDB) -> None:
    ids = {db.start_agent_run() for _ in range(50)}
    assert len(ids) == 50


def test_end_agent_run_stamps_terminal_status_and_counters(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run(now_iso="2026-04-25T10:00:00Z")
    db.end_agent_run(
        agent_run_id,
        status="COMPLETED",
        batches_processed=3,
        failures_recovered=1,
        escalations=0,
        now_iso="2026-04-25T10:05:00Z",
    )
    conn = db._require_conn()
    row = conn.execute(
        "SELECT status, ended_at, batches_processed, failures_recovered, escalations "
        "FROM agent_runs WHERE agent_run_id = ?;",
        (agent_run_id,),
    ).fetchone()
    assert row["status"] == "COMPLETED"
    assert row["ended_at"] == "2026-04-25T10:05:00Z"
    assert row["batches_processed"] == 3
    assert row["failures_recovered"] == 1
    assert row["escalations"] == 0


@pytest.mark.parametrize("status", ["INTERRUPTED", "FAILED"])
def test_end_agent_run_accepts_other_terminal_statuses(
    db: ManifestDB, status: str
) -> None:
    agent_run_id = db.start_agent_run()
    db.end_agent_run(agent_run_id, status=status)
    conn = db._require_conn()
    assert (
        conn.execute(
            "SELECT status FROM agent_runs WHERE agent_run_id = ?;", (agent_run_id,)
        ).fetchone()["status"]
        == status
    )


def test_end_agent_run_rejects_in_progress_status(db: ManifestDB) -> None:
    """``IN_PROGRESS`` is the start-of-life state — refusing it here
    keeps callers from accidentally "ending" a run while it's still
    live."""
    agent_run_id = db.start_agent_run()
    with pytest.raises(ManifestError, match="terminal status"):
        db.end_agent_run(agent_run_id, status="IN_PROGRESS")


def test_end_agent_run_rejects_invalid_status(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    with pytest.raises(ManifestError, match="invalid agent_run status"):
        db.end_agent_run(agent_run_id, status="MOSTLY_DONE")


def test_end_agent_run_raises_when_id_unknown(db: ManifestDB) -> None:
    with pytest.raises(ManifestError, match="no agent_run row to end"):
        db.end_agent_run("missing", status="COMPLETED")


# ---------------------------------------------------------------------------
# record_agent_failure.
# ---------------------------------------------------------------------------


def test_record_agent_failure_inserts_attempt_row(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    failure_id = db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=1,
        last_error_msg="no match for /R\\$\\d+/",
        now_iso="2026-04-25T10:01:00Z",
    )
    conn = db._require_conn()
    row = conn.execute(
        "SELECT failure_id, agent_run_id, batch_id, layer, error_class, attempts, "
        "last_fix_kind, escalated, last_error_msg, ts "
        "FROM agent_failures WHERE failure_id = ?;",
        (failure_id,),
    ).fetchone()
    assert row["agent_run_id"] == agent_run_id
    assert row["batch_id"] == "bid01"
    assert row["layer"] == "silver"
    assert row["error_class"] == "regex_break"
    assert row["attempts"] == 1
    assert row["last_fix_kind"] is None
    assert row["escalated"] == 0
    assert row["last_error_msg"] == "no match for /R\\$\\d+/"
    assert row["ts"] == "2026-04-25T10:01:00Z"


def test_record_agent_failure_truncates_long_error_msg(db: ManifestDB) -> None:
    """``last_error_msg`` is capped at 512 chars (spec §4.2) so a
    runaway traceback can never blow up a manifest row."""
    agent_run_id = db.start_agent_run()
    long_msg = "x" * 5000
    failure_id = db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="bronze",
        error_class="schema_drift",
        attempts=1,
        last_error_msg=long_msg,
    )
    conn = db._require_conn()
    row = conn.execute(
        "SELECT last_error_msg FROM agent_failures WHERE failure_id = ?;",
        (failure_id,),
    ).fetchone()
    assert len(row["last_error_msg"]) == 512


def test_record_agent_failure_returns_unique_failure_ids(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    ids = {
        db.record_agent_failure(
            agent_run_id=agent_run_id,
            batch_id="bid01",
            layer="silver",
            error_class="regex_break",
            attempts=i,
            last_error_msg=f"attempt {i}",
        )
        for i in range(1, 6)
    }
    assert len(ids) == 5


def test_record_agent_failure_rejects_invalid_layer(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    with pytest.raises(ManifestError, match="invalid run layer"):
        db.record_agent_failure(
            agent_run_id=agent_run_id,
            batch_id="bid01",
            layer="platinum",
            error_class="regex_break",
            attempts=1,
            last_error_msg="boom",
        )


def test_record_agent_failure_rejects_invalid_error_class(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    with pytest.raises(ManifestError, match="invalid error_class"):
        db.record_agent_failure(
            agent_run_id=agent_run_id,
            batch_id="bid01",
            layer="silver",
            error_class="cosmic_ray",
            attempts=1,
            last_error_msg="boom",
        )


def test_record_agent_failure_rejects_zero_attempts(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    with pytest.raises(ManifestError, match="attempts must be >= 1"):
        db.record_agent_failure(
            agent_run_id=agent_run_id,
            batch_id="bid01",
            layer="silver",
            error_class="regex_break",
            attempts=0,
            last_error_msg="boom",
        )


def test_record_agent_failure_rejects_unknown_agent_run_id(db: ManifestDB) -> None:
    """FK constraint: an orphan failure (agent_run_id not in
    agent_runs) must be refused to keep the cascade contract sane."""
    with pytest.raises(ManifestError, match="integrity error"):
        db.record_agent_failure(
            agent_run_id="missing",
            batch_id="bid01",
            layer="silver",
            error_class="regex_break",
            attempts=1,
            last_error_msg="boom",
        )


# ---------------------------------------------------------------------------
# record_agent_fix / mark_agent_failure_escalated.
# ---------------------------------------------------------------------------


def test_record_agent_fix_stamps_kind_without_escalating(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    failure_id = db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=1,
        last_error_msg="boom",
    )
    db.record_agent_fix(failure_id, fix_kind="regenerate_regex")
    conn = db._require_conn()
    row = conn.execute(
        "SELECT last_fix_kind, escalated FROM agent_failures WHERE failure_id = ?;",
        (failure_id,),
    ).fetchone()
    assert row["last_fix_kind"] == "regenerate_regex"
    assert row["escalated"] == 0


def test_record_agent_fix_raises_when_id_unknown(db: ManifestDB) -> None:
    with pytest.raises(ManifestError, match="no agent_failure row to update"):
        db.record_agent_fix("missing", fix_kind="noop")


def test_mark_agent_failure_escalated_sets_flag(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    failure_id = db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=3,
        last_error_msg="exhausted",
    )
    db.mark_agent_failure_escalated(failure_id)
    conn = db._require_conn()
    assert (
        conn.execute(
            "SELECT escalated FROM agent_failures WHERE failure_id = ?;", (failure_id,)
        ).fetchone()["escalated"]
        == 1
    )


def test_mark_agent_failure_escalated_is_idempotent(db: ManifestDB) -> None:
    """Re-escalating a row that's already escalated must not raise —
    the executor may retry the escalation path under cleanup."""
    agent_run_id = db.start_agent_run()
    failure_id = db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=3,
        last_error_msg="exhausted",
    )
    db.mark_agent_failure_escalated(failure_id)
    db.mark_agent_failure_escalated(failure_id)


def test_mark_agent_failure_escalated_raises_when_id_unknown(db: ManifestDB) -> None:
    with pytest.raises(ManifestError, match="no agent_failure row to escalate"):
        db.mark_agent_failure_escalated("missing")


# ---------------------------------------------------------------------------
# count_agent_attempts.
# ---------------------------------------------------------------------------


def test_count_agent_attempts_zero_for_fresh_triple(db: ManifestDB) -> None:
    assert (
        db.count_agent_attempts(
            batch_id="bid01", layer="silver", error_class="regex_break"
        )
        == 0
    )


def test_count_agent_attempts_grows_with_recorded_failures(db: ManifestDB) -> None:
    agent_run_id = db.start_agent_run()
    for i in range(1, 4):
        db.record_agent_failure(
            agent_run_id=agent_run_id,
            batch_id="bid01",
            layer="silver",
            error_class="regex_break",
            attempts=i,
            last_error_msg=f"try {i}",
        )
    assert (
        db.count_agent_attempts(
            batch_id="bid01", layer="silver", error_class="regex_break"
        )
        == 3
    )


def test_count_agent_attempts_isolates_by_layer_and_error_class(db: ManifestDB) -> None:
    """The counter is scoped by the full triple — a failure in Gold
    must not bleed into Silver's retry budget."""
    agent_run_id = db.start_agent_run()
    db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=1,
        last_error_msg="boom",
    )
    db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="gold",
        error_class="regex_break",
        attempts=1,
        last_error_msg="boom",
    )
    db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="silver",
        error_class="schema_drift",
        attempts=1,
        last_error_msg="boom",
    )
    assert (
        db.count_agent_attempts(
            batch_id="bid01", layer="silver", error_class="regex_break"
        )
        == 1
    )
    assert (
        db.count_agent_attempts(
            batch_id="bid01", layer="gold", error_class="regex_break"
        )
        == 1
    )
    assert (
        db.count_agent_attempts(
            batch_id="bid01", layer="silver", error_class="schema_drift"
        )
        == 1
    )


def test_count_agent_attempts_aggregates_across_agent_runs(db: ManifestDB) -> None:
    """Retry budget is *not* reset by starting a new agent_run — a
    flaky regex that exhausted its budget yesterday stays exhausted
    today (operator must clear ``agent_failures`` to retry)."""
    run_a = db.start_agent_run()
    db.record_agent_failure(
        agent_run_id=run_a,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=1,
        last_error_msg="boom",
    )
    db.end_agent_run(run_a, status="COMPLETED")
    run_b = db.start_agent_run()
    db.record_agent_failure(
        agent_run_id=run_b,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=2,
        last_error_msg="boom",
    )
    assert (
        db.count_agent_attempts(
            batch_id="bid01", layer="silver", error_class="regex_break"
        )
        == 2
    )


def test_count_agent_attempts_rejects_invalid_layer(db: ManifestDB) -> None:
    with pytest.raises(ManifestError, match="invalid run layer"):
        db.count_agent_attempts(
            batch_id="bid01", layer="platinum", error_class="regex_break"
        )


def test_count_agent_attempts_rejects_invalid_error_class(db: ManifestDB) -> None:
    with pytest.raises(ManifestError, match="invalid error_class"):
        db.count_agent_attempts(
            batch_id="bid01", layer="silver", error_class="cosmic_ray"
        )


# ---------------------------------------------------------------------------
# Schema integrity — DDL idempotence + cascade.
# ---------------------------------------------------------------------------


def test_ensure_schema_is_idempotent(db: ManifestDB) -> None:
    """Re-running ``ensure_schema`` after writes must not wipe data."""
    agent_run_id = db.start_agent_run()
    db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=1,
        last_error_msg="boom",
    )
    db.ensure_schema()
    assert _row_count(db, "agent_runs") == 1
    assert _row_count(db, "agent_failures") == 1


def test_agent_failures_cascades_on_agent_run_delete(db: ManifestDB) -> None:
    """``ON DELETE CASCADE`` keeps the failure trail tidy when a test
    or dev reset wipes an agent_run row."""
    agent_run_id = db.start_agent_run()
    db.record_agent_failure(
        agent_run_id=agent_run_id,
        batch_id="bid01",
        layer="silver",
        error_class="regex_break",
        attempts=1,
        last_error_msg="boom",
    )
    conn = db._require_conn()
    conn.execute("DELETE FROM agent_runs WHERE agent_run_id = ?;", (agent_run_id,))
    assert _row_count(db, "agent_failures") == 0
