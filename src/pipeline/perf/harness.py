"""Perf-bench harness: scenario protocol, registry, and JSONL writer.

Public surface used by ``scripts/bench_agent.py`` and by the future
PERF.2..8 scenario modules:

* :func:`now_utc` — ISO-8601 UTC timestamp with microsecond precision.
* :data:`Clock` — alias for ``Callable[[], str]``; tests inject a
  fixed-string clock for byte-stable JSONL output.
* :class:`RunRecord` — frozen dataclass; one JSONL row per run.
* :class:`ScenarioContext` — frozen dataclass; everything a scenario
  needs to execute one or more runs.
* :class:`Scenario` — runtime-checkable Protocol; scenario modules
  expose ``SCENARIO: Scenario``.
* :func:`discover_scenarios` — walks a package via
  ``pkgutil.iter_modules`` and returns the registry.
* :class:`JsonlWriter` — context manager; atomic per-line append with
  ``mkdir -p`` on the parent directory.
"""

from __future__ import annotations

import importlib
import json
import pkgutil
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, TracebackType
from typing import IO, Protocol, runtime_checkable


def now_utc() -> str:
    """Return an ISO-8601 UTC timestamp with microsecond precision.

    Suffix is ``+00:00`` (not ``Z``) so the string is directly
    parseable by :func:`datetime.fromisoformat`.
    """
    return datetime.now(UTC).isoformat(timespec="microseconds")


Clock = Callable[[], str]
"""Wall-clock provider for perf records.

Defaults to :func:`now_utc`. Tests inject a fixed-string clock so
JSONL output is byte-stable across runs.
"""


@dataclass(frozen=True)
class RunRecord:
    """One row of perf telemetry — one JSONL line per record.

    Future scenarios may carry layer/batch metadata (``layer``,
    ``batch_id``) or skip them; both are optional.
    """

    scenario: str
    layer: str | None
    run_idx: int
    wall_ms: float
    peak_rss_mb: float | None
    batch_id: str | None
    ts: str

    def to_jsonline(self) -> str:
        """Serialize to a single compact JSON object (no trailing newline)."""
        return json.dumps(asdict(self), separators=(",", ":"))


@dataclass(frozen=True)
class ScenarioContext:
    """Everything a :class:`Scenario` needs for one invocation.

    Invariants:

    * ``runs >= 1`` — enforced in ``__post_init__``.
    * ``out_path`` and ``work_root`` are absolute or repo-relative
      :class:`pathlib.Path` instances; the scenario decides whether
      it writes under ``work_root``.
    * ``extra`` carries CLI ``--opt KEY=VAL`` pairs verbatim.
    """

    scenario_name: str
    runs: int
    out_path: Path
    work_root: Path
    extra: Mapping[str, str] = field(default_factory=dict)
    clock: Clock = field(default=now_utc)

    def __post_init__(self) -> None:
        if self.runs < 1:
            raise ValueError("runs must be >= 1")


@runtime_checkable
class Scenario(Protocol):
    """A perf scenario.

    Implementations are typically module-level objects exposed as
    ``SCENARIO``. ``run`` is a generator of :class:`RunRecord`s so
    long-running scenarios stream their telemetry instead of
    buffering it in memory.
    """

    name: str

    def run(self, ctx: ScenarioContext) -> Iterable[RunRecord]: ...


def discover_scenarios(
    package: ModuleType | None = None,
) -> dict[str, Scenario]:
    """Walk ``package`` and return every scenario found.

    A module contributes a scenario iff it exposes a module-level
    ``SCENARIO`` attribute that satisfies the :class:`Scenario`
    protocol (checked via ``isinstance`` thanks to
    ``@runtime_checkable``). Modules without ``SCENARIO`` are
    skipped silently.

    Raises :class:`RuntimeError` if two scenarios claim the same
    ``name`` — duplicate names would silently shadow each other in a
    dict, which is exactly the kind of silent loss the registry
    should refuse.
    """
    if package is None:
        package = importlib.import_module("pipeline.perf.scenarios")

    registry: dict[str, Scenario] = {}
    for module_info in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
        module = importlib.import_module(module_info.name)
        scenario = getattr(module, "SCENARIO", None)
        if scenario is None:
            continue
        if not isinstance(scenario, Scenario):
            continue
        if scenario.name in registry:
            raise RuntimeError(
                f"duplicate perf scenario name {scenario.name!r} (module {module_info.name})"
            )
        registry[scenario.name] = scenario
    return registry


class JsonlWriter:
    """Append-only JSONL sink with mkdir-parent + flush per record.

    One ``write`` call per record on a flushed handle keeps the
    write atomic on POSIX for line lengths under ``PIPE_BUF`` —
    perf records are short, so a partially-written line is
    practically impossible.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._handle: IO[bytes] | None = None

    def __enter__(self) -> JsonlWriter:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self._path.open("ab")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def write(self, record: RunRecord) -> None:
        if self._handle is None:
            raise RuntimeError("JsonlWriter used outside of context manager")
        line = (record.to_jsonline() + "\n").encode("utf-8")
        self._handle.write(line)
        self._handle.flush()
