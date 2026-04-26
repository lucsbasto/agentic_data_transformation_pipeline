"""PERF.8 — peak RSS + ManifestDB IO instrumentation.

Library module. Future scenarios opt in via instrumented_session(db).
Linux-first: ru_maxrss is in KB on Linux, bytes on macOS — conversion
is documented inline. Returns None on platforms without resource.
"""

from __future__ import annotations

import resource
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

# pipeline imports kept lazy/local to avoid circular cost during measurement.


_READ_PREFIXES: tuple[str, ...] = ("get_", "count_", "is_", "list_")
_WRITE_PREFIXES: tuple[str, ...] = ("start_", "end_", "record_", "mark_")


@dataclass(frozen=True)
class InstrumentSnapshot:
    """Combined RSS + IO counts from a measurement session."""

    peak_rss_mb: float | None
    io_reads: int
    io_writes: int


def _ru_maxrss_to_mb(ru_maxrss: int) -> float:
    """Convert ru_maxrss to MB. Linux=KB, macOS=bytes."""
    if sys.platform == "darwin":
        # macOS ru_maxrss is in bytes
        return ru_maxrss / (1024 * 1024)
    else:
        # Linux and most others ru_maxrss is in KB
        return ru_maxrss / 1024


@dataclass
class ManifestIOCounter:
    """Proxy that counts ManifestDB read/write operations by method prefix.

    Methods starting with prefixes in ``_READ_PREFIXES`` are counted as reads.
    Methods starting with prefixes in ``_WRITE_PREFIXES`` are counted as writes.
    All other methods pass through uncounted.
    """

    _target: Any  # ManifestDB instance
    _reads: int = 0
    _writes: int = 0

    def snapshot(self) -> dict[str, int]:
        """Return current counts as {reads, writes}."""
        return {"reads": self._reads, "writes": self._writes}

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._target, name)

        if name.startswith(_READ_PREFIXES):
            self._reads += 1
        elif name.startswith(_WRITE_PREFIXES):
            self._writes += 1

        # Only wrap callable attributes (methods, not properties)
        if callable(attr):

            def _wrapper(*args: Any, **kwargs: Any) -> Any:
                return attr(*args, **kwargs)

            return _wrapper

        return attr


@contextmanager
def measure_peak_rss() -> Iterator[Callable[[], float | None]]:
    """Context manager yielding a getter that returns peak RSS in MB.

    The getter returns None on platforms where resource calls are unavailable.
    Usage::

        with measure_peak_rss() as get_peak:
            # do memory-intensive work
            peak = get_peak()  # Returns MB or None
    """
    try:
        resource.getrusage(resource.RUSAGE_SELF)
    except OSError:
        yield lambda: None
        return

    def _get_peak() -> float | None:
        try:
            r1 = resource.getrusage(resource.RUSAGE_SELF)
            return _ru_maxrss_to_mb(r1.ru_maxrss)
        except OSError:
            return None

    yield _get_peak


@contextmanager
def instrumented_session(
    db: Any,
) -> Iterator[tuple[ManifestIOCounter, Callable[[], InstrumentSnapshot]]]:
    """Combine RSS + IO measurement in one context.

    Yields (wrapped_db, take_snapshot) where wrapped_db is an IO-counting
    proxy and take_snapshot() returns current InstrumentSnapshot.
    """
    with measure_peak_rss() as get_rss:
        counter = ManifestIOCounter(db)

        def _take_snapshot() -> InstrumentSnapshot:
            io_counts = counter.snapshot()
            return InstrumentSnapshot(
                peak_rss_mb=get_rss(),
                io_reads=io_counts["reads"],
                io_writes=io_counts["writes"],
            )

        yield counter, _take_snapshot
