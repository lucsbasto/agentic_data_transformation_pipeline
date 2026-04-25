"""Coverage for the F4 agent executor (F4.11)."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest

from pipeline.agent.executor import (
    DEFAULT_RETRY_BUDGET,
    Executor,
    Outcome,
)
from pipeline.agent.types import ErrorKind, Fix, Layer
from pipeline.errors import SilverRegexMissError
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


def _make_executor(
    db: ManifestDB,
    *,
    classify: Callable[..., ErrorKind] | None = None,
    build_fix: Callable[..., Fix | None] | None = None,
    escalate: Callable[..., None] | None = None,
    retry_budget: int = DEFAULT_RETRY_BUDGET,
) -> tuple[Executor, str, list[tuple[ErrorKind, Layer, str]]]:
    """Build an Executor wired with simple defaults — caller can
    override any injection point. Returns the executor, the
    ``agent_run_id``, and a shared list that escalation calls
    append to (for assertions)."""
    agent_run_id = db.start_agent_run()
    escalations: list[tuple[ErrorKind, Layer, str]] = []

    def _default_escalate(exc: BaseException, kind: ErrorKind, layer: Layer, batch_id: str) -> None:
        escalations.append((kind, layer, batch_id))

    executor = Executor(
        manifest=db,
        agent_run_id=agent_run_id,
        classify=classify or (lambda exc, layer, batch_id: ErrorKind.REGEX_BREAK),
        build_fix=build_fix or (lambda exc, kind, layer, batch_id: None),
        escalate=escalate or _default_escalate,
        retry_budget=retry_budget,
    )
    return executor, agent_run_id, escalations


def _failure_count(db: ManifestDB, *, batch_id: str, layer: Layer, kind: ErrorKind) -> int:
    return db.count_agent_attempts(
        batch_id=batch_id, layer=layer.value, error_class=kind.value
    )


def _escalated_count(db: ManifestDB, *, batch_id: str) -> int:
    conn = db._require_conn()
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM agent_failures WHERE batch_id = ? AND escalated = 1;",
            (batch_id,),
        ).fetchone()[0]
    )


# ---------------------------------------------------------------------------
# Happy path.
# ---------------------------------------------------------------------------


def test_run_with_recovery_returns_completed_when_fn_succeeds(db: ManifestDB) -> None:
    executor, _, escalations = _make_executor(db)
    calls = 0

    def runner() -> None:
        nonlocal calls
        calls += 1

    result = executor.run_with_recovery(layer=Layer.SILVER, batch_id="bid01", fn=runner)
    assert result.outcome is Outcome.COMPLETED
    assert result.attempts == 0
    assert result.failures_recovered == 0
    assert calls == 1
    assert escalations == []


def test_run_with_recovery_does_not_record_failures_on_clean_run(db: ManifestDB) -> None:
    executor, _, _ = _make_executor(db)
    executor.run_with_recovery(
        layer=Layer.SILVER, batch_id="bid01", fn=lambda: None
    )
    assert (
        _failure_count(db, batch_id="bid01", layer=Layer.SILVER, kind=ErrorKind.REGEX_BREAK)
        == 0
    )


# ---------------------------------------------------------------------------
# Fix succeeds, layer recovers.
# ---------------------------------------------------------------------------


def test_run_with_recovery_retries_after_successful_fix(db: ManifestDB) -> None:
    """First call raises; the fix's `apply` flips a flag the runner
    consults so the second call succeeds. Outcome must be COMPLETED
    with one recovered failure."""
    state = {"healed": False}

    def runner() -> None:
        if not state["healed"]:
            raise SilverRegexMissError("boom")

    def fix_apply() -> None:
        state["healed"] = True

    fix = Fix(kind="regenerate_regex", description="x", apply=fix_apply)

    executor, _, escalations = _make_executor(
        db,
        classify=lambda exc, layer, batch_id: ErrorKind.REGEX_BREAK,
        build_fix=lambda exc, kind, layer, batch_id: fix,
    )
    result = executor.run_with_recovery(
        layer=Layer.SILVER, batch_id="bid01", fn=runner
    )
    assert result.outcome is Outcome.COMPLETED
    assert result.attempts == 1
    assert result.failures_recovered == 1
    assert escalations == []
    # Fix kind was stamped on the failure row.
    conn = db._require_conn()
    row = conn.execute(
        "SELECT last_fix_kind FROM agent_failures WHERE batch_id = ?;", ("bid01",)
    ).fetchone()
    assert row["last_fix_kind"] == "regenerate_regex"


# ---------------------------------------------------------------------------
# Fix fails, runner keeps failing, budget exhausts.
# ---------------------------------------------------------------------------


def test_run_with_recovery_escalates_when_budget_exhausted(db: ManifestDB) -> None:
    """Runner always fails, fix always fails -> 3 attempts -> escalate."""
    def runner() -> None:
        raise SilverRegexMissError("permanent")

    def fix_apply() -> None:
        raise RuntimeError("fix itself broke")

    fix = Fix(kind="regenerate_regex", description="x", apply=fix_apply)
    executor, _, escalations = _make_executor(
        db,
        classify=lambda exc, layer, batch_id: ErrorKind.REGEX_BREAK,
        build_fix=lambda exc, kind, layer, batch_id: fix,
        retry_budget=3,
    )
    result = executor.run_with_recovery(
        layer=Layer.SILVER, batch_id="bid01", fn=runner
    )
    assert result.outcome is Outcome.ESCALATED
    assert result.attempts == 3
    assert result.failures_recovered == 0
    assert escalations == [(ErrorKind.REGEX_BREAK, Layer.SILVER, "bid01")]
    assert _failure_count(db, batch_id="bid01", layer=Layer.SILVER, kind=ErrorKind.REGEX_BREAK) == 3
    assert _escalated_count(db, batch_id="bid01") == 1


def test_run_with_recovery_marks_only_latest_failure_escalated(db: ManifestDB) -> None:
    """Three failure rows must exist; only the LAST one carries
    ``escalated=1``."""
    def runner() -> None:
        raise SilverRegexMissError("p")

    fix = Fix(kind="r", description="r", apply=lambda: None)  # fix always succeeds
    # Even with successful fix, the runner keeps failing -> budget exhaustion.
    executor, _, _ = _make_executor(
        db,
        classify=lambda exc, layer, batch_id: ErrorKind.REGEX_BREAK,
        build_fix=lambda exc, kind, layer, batch_id: fix,
        retry_budget=3,
    )
    executor.run_with_recovery(layer=Layer.SILVER, batch_id="bid01", fn=runner)
    conn = db._require_conn()
    rows = conn.execute(
        "SELECT attempts, escalated FROM agent_failures "
        "WHERE batch_id = ? ORDER BY attempts ASC;",
        ("bid01",),
    ).fetchall()
    assert [r["attempts"] for r in rows] == [1, 2, 3]
    assert [r["escalated"] for r in rows] == [0, 0, 1]


# ---------------------------------------------------------------------------
# UNKNOWN -> immediate escalation.
# ---------------------------------------------------------------------------


def test_run_with_recovery_escalates_unknown_immediately(db: ManifestDB) -> None:
    """``UNKNOWN`` short-circuits the retry loop on the FIRST attempt
    — no fix is built, no retry is attempted."""
    fix_calls = 0

    def fake_build_fix(exc, kind, layer, batch_id):
        nonlocal fix_calls
        fix_calls += 1
        return Fix(kind="x", description="x", apply=lambda: None)

    executor, _, escalations = _make_executor(
        db,
        classify=lambda exc, layer, batch_id: ErrorKind.UNKNOWN,
        build_fix=fake_build_fix,
        retry_budget=3,
    )
    result = executor.run_with_recovery(
        layer=Layer.GOLD, batch_id="bid01", fn=lambda: (_ for _ in ()).throw(RuntimeError("?"))
    )
    assert result.outcome is Outcome.ESCALATED
    assert result.attempts == 1
    assert escalations == [(ErrorKind.UNKNOWN, Layer.GOLD, "bid01")]
    assert fix_calls == 0  # build_fix never invoked


# ---------------------------------------------------------------------------
# Fix builder returns None -> escalate.
# ---------------------------------------------------------------------------


def test_run_with_recovery_escalates_when_no_fix_registered(db: ManifestDB) -> None:
    """If the diagnoser returns a known kind but `build_fix` has no
    handler registered, escalate immediately rather than burn the
    whole budget retrying a hopeless layer."""
    executor, _, escalations = _make_executor(
        db,
        classify=lambda exc, layer, batch_id: ErrorKind.SCHEMA_DRIFT,
        build_fix=lambda exc, kind, layer, batch_id: None,
        retry_budget=3,
    )
    result = executor.run_with_recovery(
        layer=Layer.BRONZE,
        batch_id="bid01",
        fn=lambda: (_ for _ in ()).throw(RuntimeError("?")),
    )
    assert result.outcome is Outcome.ESCALATED
    assert result.attempts == 1
    assert escalations == [(ErrorKind.SCHEMA_DRIFT, Layer.BRONZE, "bid01")]


# ---------------------------------------------------------------------------
# Default budget pinned.
# ---------------------------------------------------------------------------


def test_default_retry_budget_matches_spec() -> None:
    """Spec §7 D1 fixes the default retry budget at 3 — pin so a
    quiet bump surfaces in CI."""
    assert DEFAULT_RETRY_BUDGET == 3
