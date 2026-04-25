"""FIC.17 — stale lock takeover.

Scenario: write a lock file by hand with a dead PID (guaranteed not running)
and an mtime older than ``stale_after_s`` (default 3600s).  Then call
``run_once`` and assert:

- Run completes successfully (lock taken over).
- Lock file PID was overwritten with the current process's PID.
- No orphaned ``.tmp`` lock files remain after the run.
"""

from __future__ import annotations

import os
import time

import pytest

from pipeline.agent.lock import AgentLock
from pipeline.agent.types import ErrorKind, Fix, Layer, RunStatus

from .conftest import (
    PipelineTree,
    run_agent_once,
)


@pytest.mark.fault_campaign
def test_stale_lock_takeover_succeeds(pipeline_tree: PipelineTree) -> None:
    """run_once takes over a stale lock (dead PID + old mtime) and completes."""

    lock_path = pipeline_tree.state / "agent.lock"

    # Write a lock file with a PID guaranteed to be dead.
    # PID 0 is never valid for kill(); _pid_alive returns False for pid <= 0.
    dead_pid = 0
    lock_path.write_text(f"{dead_pid}\n", encoding="utf-8")

    # Back-date the mtime to 2h ago — well past the 1h stale_after_s default.
    old_mtime = time.time() - 7200.0
    os.utime(lock_path, (old_mtime, old_mtime))

    # Confirm setup: file exists with old mtime and dead PID.
    assert lock_path.exists()
    assert lock_path.read_text(encoding="utf-8").strip() == str(dead_pid)

    # ------------------------------------------------------------------ run
    result = run_agent_once(tree=pipeline_tree)

    # ------------------------------------------------------------------ assertions

    # Run completed successfully — lock takeover did not raise.
    assert result.status is RunStatus.COMPLETED

    # Lock file was released after run_once finished.
    assert not lock_path.exists(), "lock file should be released after run_once"

    # No orphaned .tmp files.
    tmp_files = list(pipeline_tree.state.glob("*.tmp"))
    assert tmp_files == [], f"orphaned .tmp files: {tmp_files}"


@pytest.mark.fault_campaign
def test_stale_lock_pid_overwritten(pipeline_tree: PipelineTree) -> None:
    """During takeover the lock file PID is updated to the current process."""

    lock_path = pipeline_tree.state / "agent.lock"

    # Intercept _write_self_pid to capture the PID written during acquire.
    written_pids: list[int] = []
    original_write = AgentLock._write_self_pid

    def capturing_write(self: AgentLock) -> None:  # type: ignore[override]
        original_write(self)
        written_pids.append(lock_path.read_text(encoding="utf-8").strip())

    AgentLock._write_self_pid = capturing_write  # type: ignore[method-assign]
    try:
        # Stale lock: dead PID, old mtime.
        lock_path.write_text("0\n", encoding="utf-8")
        os.utime(lock_path, (time.time() - 7200.0,) * 2)

        result = run_agent_once(tree=pipeline_tree)
    finally:
        AgentLock._write_self_pid = original_write  # type: ignore[method-assign]

    assert result.status is RunStatus.COMPLETED

    # The PID written to the lock during takeover must match the current process.
    assert written_pids, "lock was never acquired"
    assert int(written_pids[0]) == os.getpid()
