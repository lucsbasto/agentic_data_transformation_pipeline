"""Retry + recovery wrapper around a single layer entrypoint
(F4 design §6).

The executor is the loop's hot path: it invokes the runner, catches
any exception, asks the diagnoser to classify it, records the
failure on ``agent_failures``, asks an injected ``build_fix``
callable for a deterministic recovery, applies the fix, and retries
— all bounded by a per-``(batch_id, layer, error_class)`` budget.

Everything around the executor is injected (classifier, fix builder,
escalator) so unit tests can drive the recovery loop without
spinning up the real LLM client or the real filesystem fixes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

import structlog

from pipeline.agent.types import ErrorKind, Fix, Layer
from pipeline.state.manifest import ManifestDB

_LOG = structlog.get_logger(__name__)

DEFAULT_RETRY_BUDGET: Final[int] = 3
"""Per-``(batch_id, layer, error_class)`` retry cap (spec §7 D1)."""


class Outcome(StrEnum):
    """Terminal state of one ``run_with_recovery`` call."""

    COMPLETED = "completed"
    ESCALATED = "escalated"


Classifier = Callable[[BaseException, Layer, str], ErrorKind]
"""``classify(exc, layer, batch_id) -> ErrorKind`` — typically
:func:`pipeline.agent.diagnoser.classify` curried with the LLM
client + diagnose budget."""

FixBuilder = Callable[[BaseException, ErrorKind, Layer, str], Fix | None]
"""``build_fix(exc, kind, layer, batch_id) -> Fix | None`` — None
when no deterministic fix module is registered for ``kind``."""

Escalator = Callable[[BaseException, ErrorKind, Layer, str], None]
"""``escalate(exc, kind, layer, batch_id) -> None`` — invoked when
the retry budget is exhausted or the kind is ``UNKNOWN``."""


@dataclass(frozen=True)
class RecoveryResult:
    """Outcome of one ``run_with_recovery`` call, returned to the
    loop for tally bookkeeping."""

    outcome: Outcome
    attempts: int
    failures_recovered: int
    last_kind: ErrorKind | None


class Executor:
    """Retry-budget-aware runner for one layer entrypoint."""

    def __init__(
        self,
        *,
        manifest: ManifestDB,
        agent_run_id: str,
        classify: Classifier,
        build_fix: FixBuilder,
        escalate: Escalator,
        retry_budget: int = DEFAULT_RETRY_BUDGET,
    ) -> None:
        """Wire the executor with its strategy collaborators.

        All callables are injected rather than imported directly so
        unit tests can drive the full recovery loop with stubs, without
        spinning up the real LLM client or filesystem fix modules."""
        self._manifest = manifest
        self._agent_run_id = agent_run_id
        self._classify = classify
        self._build_fix = build_fix
        self._escalate = escalate
        self._retry_budget = retry_budget

    def run_with_recovery(
        self,
        *,
        layer: Layer,
        batch_id: str,
        fn: Callable[[], None],
    ) -> RecoveryResult:
        """Drive one layer to completion or escalation.

        Loop body, mirroring design §6:

        1. invoke ``fn()`` — return COMPLETED on success.
        2. classify the exception; record one row in
           ``agent_failures`` with the running attempt count.
        3. ``UNKNOWN`` -> escalate immediately, return ESCALATED.
        4. ``build_fix`` returns ``None`` -> no recovery known,
           escalate, return ESCALATED.
        5. apply the fix:
           - success -> ``record_agent_fix`` stamps the kind, then
             we loop and retry ``fn()``.
           - failure -> log, do NOT mark the failure row with a
             fix, loop and retry anyway. The next iteration may
             succeed if the fix had partial effect; otherwise it
             will exhaust the budget.

        When the loop exits without success the caller-supplied
        ``escalate`` is invoked once with the last exception.
        """
        attempts = 0
        failures_recovered = 0
        last_exc: BaseException | None = None
        last_kind: ErrorKind | None = None
        while attempts < self._retry_budget:
            try:
                fn()
            except Exception as exc:
                attempts += 1
                last_exc = exc
                kind = self._classify(exc, layer, batch_id)
                last_kind = kind
                failure_id = self._manifest.record_agent_failure(
                    agent_run_id=self._agent_run_id,
                    batch_id=batch_id,
                    layer=layer.value,
                    error_class=kind.value,
                    attempts=attempts,
                    last_error_msg=str(exc),
                )
                _LOG.info(
                    "agent.executor.failure",
                    batch_id=batch_id,
                    layer=layer.value,
                    error_class=kind.value,
                    attempt=attempts,
                    exc_type=type(exc).__name__,
                )
                if kind is ErrorKind.UNKNOWN:
                    self._escalate_and_mark(failure_id, exc, kind, layer, batch_id)
                    return RecoveryResult(
                        outcome=Outcome.ESCALATED,
                        attempts=attempts,
                        failures_recovered=failures_recovered,
                        last_kind=kind,
                    )
                fix = self._build_fix(exc, kind, layer, batch_id)
                if fix is None:
                    self._escalate_and_mark(failure_id, exc, kind, layer, batch_id)
                    return RecoveryResult(
                        outcome=Outcome.ESCALATED,
                        attempts=attempts,
                        failures_recovered=failures_recovered,
                        last_kind=kind,
                    )
                try:
                    fix.apply()
                except Exception as fix_exc:
                    _LOG.warning(
                        "agent.executor.fix_failed",
                        batch_id=batch_id,
                        layer=layer.value,
                        fix_kind=fix.kind,
                        fix_error=str(fix_exc),
                    )
                else:
                    self._manifest.record_agent_fix(failure_id, fix_kind=fix.kind)
                    failures_recovered += 1
            else:
                _LOG.info(
                    "agent.executor.completed",
                    batch_id=batch_id,
                    layer=layer.value,
                    attempts=attempts,
                )
                return RecoveryResult(
                    outcome=Outcome.COMPLETED,
                    attempts=attempts,
                    failures_recovered=failures_recovered,
                    last_kind=last_kind,
                )
        # budget exhausted
        assert last_exc is not None  # loop only exits via budget after >=1 fail
        assert last_kind is not None
        # Pull the most recent failure_id for this triple so the
        # escalator can flip ``escalated=1`` on the right row.
        last_failure_id = self._latest_failure_id(
            batch_id=batch_id, layer=layer, kind=last_kind
        )
        self._escalate_and_mark(
            last_failure_id, last_exc, last_kind, layer, batch_id
        )
        return RecoveryResult(
            outcome=Outcome.ESCALATED,
            attempts=attempts,
            failures_recovered=failures_recovered,
            last_kind=last_kind,
        )

    # ------------------------------------------------------------------ helpers

    def _escalate_and_mark(
        self,
        failure_id: str | None,
        exc: BaseException,
        kind: ErrorKind,
        layer: Layer,
        batch_id: str,
    ) -> None:
        """Flip ``escalated=1`` on the failure row then invoke the
        escalator. Marking happens first so a crash inside the escalator
        does not leave the row un-flagged in the manifest."""
        if failure_id is not None:
            self._manifest.mark_agent_failure_escalated(failure_id)
        self._escalate(exc, kind, layer, batch_id)

    def _latest_failure_id(
        self, *, batch_id: str, layer: Layer, kind: ErrorKind
    ) -> str | None:
        """Look up the most recent ``agent_failures`` row for the
        triple — used at budget exhaustion to mark exactly the row
        that triggered the escalation. Delegates to the manifest's
        public API so the executor stays decoupled from SQLite
        details."""
        return self._manifest.latest_agent_failure_id(
            batch_id=batch_id, layer=layer.value, error_class=kind.value
        )
