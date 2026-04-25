"""Filesystem lock for the F4 agent loop (design §10, ADR-008).

Single-instance guard for ``run_once`` / ``run_forever``. The lockfile
stores the agent process's PID; ``acquire`` refuses if the recorded
PID is alive AND the lockfile is younger than ``stale_after_s``,
otherwise it overwrites the stale entry. ``release`` removes the file
only if its PID matches the current process — so a second agent that
inherited a stale lock cannot delete a healthy lock by mistake.

Signal-handler integration (SIGINT/SIGTERM → ``release``) lives in
``pipeline.agent.loop`` (F4.14); this module exposes only the
``acquire`` / ``release`` / context-manager surface.
"""

from __future__ import annotations

import contextlib
import errno
import os
import time
from pathlib import Path
from types import TracebackType

from pipeline.errors import AgentBusyError

_DEFAULT_LOCK_PATH: str = "state/agent.lock"
_DEFAULT_STALE_AFTER_S: float = 3600.0  # 1h, matches design §10 default


class AgentLock:
    """PID-based filesystem lock.

    Use as a context manager (``with AgentLock() as lock:``) so
    ``release`` runs even on uncaught exceptions. Direct
    ``acquire``/``release`` is also supported for callers that need to
    hand off the lock across function boundaries.
    """

    def __init__(
        self,
        path: Path | str = _DEFAULT_LOCK_PATH,
        *,
        stale_after_s: float = _DEFAULT_STALE_AFTER_S,
    ) -> None:
        self._path: Path = Path(path)
        self._stale_after_s: float = stale_after_s
        self._held: bool = False

    @property
    def path(self) -> Path:
        return self._path

    def acquire(self) -> None:
        """Take the lock or raise :class:`AgentBusyError`.

        Decision tree:

        - missing file              -> write own PID
        - corrupt file              -> overwrite (treated as stale)
        - PID == own                -> reentrant; no-op
        - PID alive AND fresh       -> raise ``AgentBusyError``
        - PID dead OR file too old  -> overwrite (stale)

        ``too old`` (mtime older than ``stale_after_s``) catches the
        edge case where the OS reused the recorded PID for an
        unrelated process — without it a long-dead lock could keep us
        out forever just because PID 1234 happens to belong to bash
        now.
        """
        recorded = self._read_pid()
        if recorded is None:
            self._write_self_pid()
            return
        if recorded == os.getpid():
            self._held = True
            return
        if self._pid_alive(recorded) and self._is_fresh():
            raise AgentBusyError(
                f"agent lock at {self._path} is held by pid={recorded}"
            )
        self._write_self_pid()

    def release(self) -> None:
        """Remove the lockfile if it still records this process's PID.

        A no-op if the file is gone, owned by a different PID, or
        unparseable — releasing what is not ours would let a healthy
        agent be unlocked by a process that lost its claim during
        stale takeover.
        """
        recorded = self._read_pid()
        if recorded != os.getpid():
            self._held = False
            return
        with contextlib.suppress(FileNotFoundError):
            self._path.unlink()
        self._held = False

    @property
    def held(self) -> bool:
        """True if this instance currently owns the lockfile."""
        return self._held

    # ------------------------------------------------------------------ context manager

    def __enter__(self) -> AgentLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    # ------------------------------------------------------------------ internals

    def _read_pid(self) -> int | None:
        """Return the PID stored in the lockfile, or ``None`` when the
        file is missing / empty / unparseable."""
        try:
            text = self._path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return None
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None

    def _write_self_pid(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Plain write — single-operator tool, no need for O_EXCL atomicity.
        self._path.write_text(f"{os.getpid()}\n", encoding="utf-8")
        self._held = True

    def _is_fresh(self) -> bool:
        """``True`` if the lockfile mtime is within ``stale_after_s``
        of now. Older than that, treat as stale even if the recorded
        PID happens to be alive (PID rotation guard)."""
        try:
            mtime = self._path.stat().st_mtime
        except FileNotFoundError:
            return False
        return (time.time() - mtime) < self._stale_after_s

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        """``signal 0`` is the POSIX trick for ``does this PID exist
        and can I send it signals?`` — no signal is delivered, only
        the existence + permission check is performed.

        - returns ``True`` on success, on ``EPERM`` (process exists
          but is owned by another user), and on any unexpected OS
          error (be conservative — assume alive).
        - returns ``False`` only on ``ESRCH`` (no such PID).
        """
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError as exc:
            return exc.errno != errno.ESRCH
        return True
