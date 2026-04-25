"""Shared campaign harness for FIC fault-injection tests (FIC.1).

Provides:
  - ``pipeline_tree``  — tmp dir with bronze/silver/gold/state/logs layout + ManifestDB.
  - ``seeded_batch``   — writes a clean bronze-compatible source parquet (5 rows).
  - ``fake_llm_client``— scripted LLM stub; records calls for assertion.
  - ``run_agent_once`` — thin wrapper around ``pipeline.agent.loop.run_once``.
  - ``assert_run_status``          — helper for agent_runs status check.
  - ``assert_agent_failure_count`` — helper for agent_failures count check.
  - ``tail_jsonl``     — reads JSONL event log.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from pipeline.agent._logging import AgentEventLogger, fixed_clock
from pipeline.agent.lock import AgentLock
from pipeline.agent.loop import run_once
from pipeline.agent.types import AgentResult, ErrorKind, Fix, Layer, RunStatus
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS
from pipeline.state.manifest import ManifestDB

# ---------------------------------------------------------------------------
# Fixed timestamp used across the campaign for deterministic log replay.
# ---------------------------------------------------------------------------

_FIXED_TS = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)

# ---------------------------------------------------------------------------
# pipeline_tree fixture.
# ---------------------------------------------------------------------------


@dataclass
class PipelineTree:
    """Paths for a fully-initialized pipeline workspace."""

    root: Path
    bronze: Path
    silver: Path
    gold: Path
    state: Path
    logs: Path
    manifest: ManifestDB


@pytest.fixture
def pipeline_tree(tmp_path: Path) -> Iterator[PipelineTree]:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    state = tmp_path / "state"
    logs = tmp_path / "logs"
    for d in (bronze, silver, gold, state, logs):
        d.mkdir(parents=True)

    manifest = ManifestDB(str(state / "manifest.db")).open()
    try:
        yield PipelineTree(
            root=tmp_path,
            bronze=bronze,
            silver=silver,
            gold=gold,
            state=state,
            logs=logs,
            manifest=manifest,
        )
    finally:
        manifest.close()


# ---------------------------------------------------------------------------
# seeded_batch fixture.
# ---------------------------------------------------------------------------


def _make_source_df(marker: str, n_rows: int = 5) -> pl.DataFrame:
    """Build a small source-compatible DataFrame with ``n_rows`` rows."""
    rows: dict[str, list[str]] = {col: [marker] * n_rows for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"] * n_rows
    rows["direction"] = ["inbound"] * n_rows
    rows["message_type"] = ["text"] * n_rows
    rows["status"] = ["delivered"] * n_rows
    rows["message_body"] = [f"mensagem_{i}" for i in range(n_rows)]
    if "message_id" in rows:
        rows["message_id"] = [f"{marker}_msg_{i}" for i in range(n_rows)]
    return pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))


@pytest.fixture
def seeded_batch(pipeline_tree: PipelineTree) -> tuple[Path, str]:
    """Write a clean 5-row source parquet; return ``(path, batch_id)``."""
    src = pipeline_tree.root / "raw" / "conversations.parquet"
    src.parent.mkdir(parents=True, exist_ok=True)
    _make_source_df("seed").write_parquet(src)
    batch_id = compute_batch_identity(src).batch_id
    return src, batch_id


# ---------------------------------------------------------------------------
# Fake LLM client.
# ---------------------------------------------------------------------------


@dataclass
class FakeLLMClient:
    """Scripted LLM stub that returns canned replies from a queue.

    Mirrors the ``cached_call`` protocol used by ``pipeline.agent.diagnoser``
    and the ``messages.create`` protocol used by direct anthropic calls.
    The ``.calls`` list lets tests assert what was sent.
    """

    _replies: list[str] = field(default_factory=list)
    calls: list[dict[str, Any]] = field(default_factory=list)

    def queue_reply(self, text: str) -> None:
        """Enqueue one scripted reply (FIFO)."""
        self._replies.append(text)

    # diagnoser protocol
    def cached_call(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if not self._replies:
            raise AssertionError("FakeLLMClient ran out of scripted replies")

        @dataclass
        class _Resp:
            text: str

        return _Resp(text=self._replies.pop(0))

    # anthropic messages.create protocol (for tests that need it)
    class _Messages:
        def __init__(self, client: FakeLLMClient) -> None:
            self._client = client

        def create(self, **kwargs: Any) -> Any:
            return self._client.cached_call(**kwargs)

    @property
    def messages(self) -> _Messages:
        return self._Messages(self)


@pytest.fixture
def fake_llm_client() -> FakeLLMClient:
    return FakeLLMClient()


# ---------------------------------------------------------------------------
# run_agent_once helper.
# ---------------------------------------------------------------------------


def run_agent_once(
    *,
    tree: PipelineTree,
    runners_for: Any | None = None,
    classify: Any | None = None,
    build_fix: Any | None = None,
    escalate: Any | None = None,
    retry_budget: int = 3,
) -> AgentResult:
    """Thin wrapper around ``pipeline.agent.loop.run_once`` with
    sensible defaults wired to a ``PipelineTree`` fixture."""

    def _noop_classify(exc: BaseException, layer: Layer, batch_id: str) -> ErrorKind:
        return ErrorKind.UNKNOWN

    def _no_fix(
        exc: BaseException, kind: ErrorKind, layer: Layer, batch_id: str
    ) -> Fix | None:
        return None

    def _no_escalate(
        exc: BaseException, kind: ErrorKind, layer: Layer, batch_id: str
    ) -> None:
        pass

    return run_once(
        manifest=tree.manifest,
        source_root=tree.root / "raw",
        runners_for=runners_for or (lambda _: {}),
        classify=classify or _noop_classify,
        build_fix=build_fix or _no_fix,
        escalate=escalate or _no_escalate,
        lock=AgentLock(tree.state / "agent.lock"),
        retry_budget=retry_budget,
        event_logger=AgentEventLogger(
            log_path=tree.logs / "agent.jsonl",
            clock=fixed_clock(_FIXED_TS),
        ),
    )


# ---------------------------------------------------------------------------
# Assertion helpers.
# ---------------------------------------------------------------------------


def assert_run_status(
    manifest: ManifestDB,
    agent_run_id: str,
    expected: RunStatus,
) -> None:
    """Assert the terminal status of an agent_run row."""
    conn = manifest._require_conn()
    row = conn.execute(
        "SELECT status FROM agent_runs WHERE agent_run_id = ?;",
        (agent_run_id,),
    ).fetchone()
    assert row is not None, f"agent_run {agent_run_id!r} not found"
    assert row["status"] == expected.value, (
        f"expected status {expected.value!r}, got {row['status']!r}"
    )


def assert_agent_failure_count(
    manifest: ManifestDB,
    batch_id: str,
    layer: Layer,
    error_class: ErrorKind,
    expected: int,
) -> None:
    """Assert the number of agent_failures rows for a specific triple."""
    conn = manifest._require_conn()
    count = conn.execute(
        "SELECT COUNT(*) FROM agent_failures "
        "WHERE batch_id = ? AND layer = ? AND error_class = ?;",
        (batch_id, layer.value, error_class.value),
    ).fetchone()[0]
    assert count == expected, (
        f"expected {expected} failure row(s) for ({batch_id!r}, {layer.value!r}, "
        f"{error_class.value!r}), got {count}"
    )


# ---------------------------------------------------------------------------
# JSONL helper.
# ---------------------------------------------------------------------------


def tail_jsonl(path: Path, n: int | None = None) -> list[dict[str, Any]]:
    """Read all events from a JSONL file; optionally return only the last ``n``."""
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    events = [json.loads(line) for line in lines if line.strip()]
    if n is not None:
        events = events[-n:]
    return events
