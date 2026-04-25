"""Coverage for the agent lockfile (F4.3).

Each test runs against ``tmp_path`` so the real ``state/agent.lock``
is never touched. PID liveness is simulated either by reusing the
test runner's own PPID (guaranteed alive) or by monkey-patching
``AgentLock._pid_alive`` to report a chosen PID as dead — this avoids
``os.fork`` (deprecated under multi-threaded test runners).
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from pipeline.agent.lock import AgentLock
from pipeline.errors import AgentBusyError, PipelineError

# ``os.fork`` is referenced indirectly by the dead-PID helper below
# only on POSIX; the test file therefore implicitly assumes POSIX.
# Skip the whole file on Windows when CI eventually runs there.
pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="agent lockfile semantics are POSIX-only"
)


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _force_dead_pid(monkeypatch: pytest.MonkeyPatch, dead_value: int = 999_999) -> int:
    """Stub :meth:`AgentLock._pid_alive` so any call with ``dead_value``
    returns False. Avoids ``os.fork`` (deprecated in multi-threaded
    test runners) while still simulating a dead PID."""
    real = AgentLock._pid_alive

    def _fake(pid: int) -> bool:
        if pid == dead_value:
            return False
        return real(pid)

    monkeypatch.setattr(AgentLock, "_pid_alive", staticmethod(_fake))
    return dead_value


# ---------------------------------------------------------------------------
# acquire on a clean filesystem.
# ---------------------------------------------------------------------------


def test_acquire_writes_pid_when_path_missing(tmp_path: Path) -> None:
    lock_path = tmp_path / "agent.lock"
    lock = AgentLock(lock_path)
    lock.acquire()
    assert lock_path.read_text(encoding="utf-8").strip() == str(os.getpid())
    assert lock.held is True


def test_acquire_creates_parent_directory(tmp_path: Path) -> None:
    """Mirrors the real default ``state/agent.lock`` — parent dir may
    not exist yet on a fresh checkout."""
    lock_path = tmp_path / "deep" / "nested" / "agent.lock"
    AgentLock(lock_path).acquire()
    assert lock_path.exists()


def test_acquire_is_reentrant_for_same_pid(tmp_path: Path) -> None:
    """Re-acquiring within the same process must succeed (not raise
    ``AgentBusyError``) — the agent loop calls ``acquire`` defensively at
    the top of each iteration."""
    lock = AgentLock(tmp_path / "agent.lock")
    lock.acquire()
    lock.acquire()
    assert lock.held is True


# ---------------------------------------------------------------------------
# acquire when another live process holds the lock.
# ---------------------------------------------------------------------------


def test_acquire_raises_agent_busy_when_other_pid_alive(tmp_path: Path) -> None:
    """Simulate a healthy peer by writing the parent process's PID
    (the test runner itself — always alive). PPID is convenient and
    guaranteed != current PID."""
    lock_path = tmp_path / "agent.lock"
    lock_path.write_text(f"{os.getppid()}\n", encoding="utf-8")
    with pytest.raises(AgentBusyError, match="held by pid"):
        AgentLock(lock_path).acquire()


def test_agent_busy_inherits_from_pipeline_error(tmp_path: Path) -> None:
    """Callers at the entrypoint catch ``PipelineError`` once for
    everything we throw; ``AgentBusyError`` must fit that umbrella."""
    lock_path = tmp_path / "agent.lock"
    lock_path.write_text(f"{os.getppid()}\n", encoding="utf-8")
    with pytest.raises(PipelineError):
        AgentLock(lock_path).acquire()


# ---------------------------------------------------------------------------
# Stale takeover.
# ---------------------------------------------------------------------------


def test_acquire_overwrites_stale_lock_when_pid_dead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stub the liveness check so the recorded PID looks dead — the
    lock should be overwritten with our PID."""
    dead = _force_dead_pid(monkeypatch)
    lock_path = tmp_path / "agent.lock"
    lock_path.write_text(f"{dead}\n", encoding="utf-8")
    AgentLock(lock_path).acquire()
    assert lock_path.read_text(encoding="utf-8").strip() == str(os.getpid())


def test_acquire_overwrites_lock_with_corrupt_contents(tmp_path: Path) -> None:
    """Empty file, garbage text, or truncated PID should not lock us
    out forever — treat as stale."""
    lock_path = tmp_path / "agent.lock"
    lock_path.write_text("not-a-pid\n", encoding="utf-8")
    AgentLock(lock_path).acquire()
    assert lock_path.read_text(encoding="utf-8").strip() == str(os.getpid())


def test_acquire_overwrites_empty_lock_file(tmp_path: Path) -> None:
    lock_path = tmp_path / "agent.lock"
    lock_path.write_text("", encoding="utf-8")
    AgentLock(lock_path).acquire()
    assert lock_path.read_text(encoding="utf-8").strip() == str(os.getpid())


def test_acquire_overwrites_when_lock_is_too_old_even_if_pid_alive(
    tmp_path: Path,
) -> None:
    """PID rotation guard (design §10): if the lockfile is older than
    ``stale_after_s`` we take over even when the recorded PID happens
    to be alive — that PID likely belongs to an unrelated process now.
    """
    lock_path = tmp_path / "agent.lock"
    lock_path.write_text(f"{os.getppid()}\n", encoding="utf-8")
    # Push mtime two days into the past.
    old = time.time() - 2 * 24 * 3600
    os.utime(lock_path, (old, old))
    AgentLock(lock_path, stale_after_s=3600).acquire()
    assert lock_path.read_text(encoding="utf-8").strip() == str(os.getpid())


# ---------------------------------------------------------------------------
# release semantics.
# ---------------------------------------------------------------------------


def test_release_removes_lockfile_when_pid_matches(tmp_path: Path) -> None:
    lock = AgentLock(tmp_path / "agent.lock")
    lock.acquire()
    lock.release()
    assert not (tmp_path / "agent.lock").exists()
    assert lock.held is False


def test_release_is_a_noop_when_lockfile_already_gone(tmp_path: Path) -> None:
    """Signal handlers may double-release. Releasing an already-gone
    lockfile must not raise."""
    lock = AgentLock(tmp_path / "agent.lock")
    lock.acquire()
    (tmp_path / "agent.lock").unlink()
    lock.release()


def test_release_does_not_remove_lock_owned_by_another_pid(tmp_path: Path) -> None:
    """Hardening: if a stale takeover overwrote a healthy peer's
    lock, the original instance attempting ``release`` later must NOT
    delete the new owner's file."""
    lock_path = tmp_path / "agent.lock"
    lock_path.write_text(f"{os.getppid()}\n", encoding="utf-8")
    lock = AgentLock(lock_path)
    lock._held = True
    lock.release()
    assert lock_path.exists()
    assert lock_path.read_text(encoding="utf-8").strip() == str(os.getppid())


# ---------------------------------------------------------------------------
# Context-manager protocol.
# ---------------------------------------------------------------------------


def test_context_manager_acquires_and_releases(tmp_path: Path) -> None:
    lock_path = tmp_path / "agent.lock"
    with AgentLock(lock_path) as lock:
        assert lock_path.exists()
        assert lock.held is True
    assert not lock_path.exists()


def test_context_manager_releases_on_exception(tmp_path: Path) -> None:
    lock_path = tmp_path / "agent.lock"
    with pytest.raises(RuntimeError, match="boom"), AgentLock(lock_path):
        raise RuntimeError("boom")
    assert not lock_path.exists()


# ---------------------------------------------------------------------------
# Defaults.
# ---------------------------------------------------------------------------


def test_default_lock_path_matches_design(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Default path is ``state/agent.lock`` (design §10). Pin it so a
    rename surfaces as a test failure."""
    monkeypatch.chdir(tmp_path)
    lock = AgentLock()
    assert lock.path == Path("state/agent.lock")


def test_pid_alive_returns_false_for_zero_or_negative(tmp_path: Path) -> None:
    """Defensive: PID values <= 0 are not real Unix processes — never
    treat them as alive."""
    assert AgentLock._pid_alive(0) is False
    assert AgentLock._pid_alive(-1) is False
