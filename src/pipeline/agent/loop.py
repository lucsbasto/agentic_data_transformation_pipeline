"""Single-iteration orchestrator for the F4 agent loop (design §2).

``run_once`` wires every other agent module together for one
end-to-end pass:

    acquire lock
    open agent_run row
    for each pending batch (observer):
        for each pending layer (planner):
            execute with retry/recovery (executor)
    close agent_run row with the tally
    release lock

Everything around ``run_once`` is injected (classifier, fix builder,
escalator, runners_for, lock, event_logger) so the same function is
used by the production CLI (F4.16), the demo script (F4.17), and
the integration tests in this commit.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from pipeline.agent._logging import (
    EVENT_BATCH_STARTED,
    EVENT_LAYER_COMPLETED,
    EVENT_LAYER_STARTED,
    EVENT_LOOP_ITERATION,
    EVENT_LOOP_STARTED,
    EVENT_LOOP_STOPPED,
    AgentEventLogger,
)
from pipeline.agent.executor import (
    DEFAULT_RETRY_BUDGET,
    Classifier,
    Escalator,
    Executor,
    FixBuilder,
    Outcome,
)
from pipeline.agent.lock import AgentLock
from pipeline.agent.observer import scan
from pipeline.agent.planner import LayerRunner, plan
from pipeline.agent.types import AgentResult, Layer, RunStatus
from pipeline.state.manifest import ManifestDB

RunnersFor = Callable[[str], Mapping[Layer, LayerRunner]]
"""``runners_for(batch_id) -> {Layer: zero-arg runner}`` — the loop
asks the caller to materialize per-batch runners on demand so
configuration (paths, settings) stays in the caller's closure."""


def run_once(
    *,
    manifest: ManifestDB,
    source_root: Path,
    runners_for: RunnersFor,
    classify: Classifier,
    build_fix: FixBuilder,
    escalate: Escalator,
    lock: AgentLock | None = None,
    retry_budget: int = DEFAULT_RETRY_BUDGET,
    event_logger: AgentEventLogger | None = None,
) -> AgentResult:
    """Drive one full agent iteration end-to-end.

    Returns a populated :class:`AgentResult` with the terminal
    :class:`RunStatus` and the loop's tallies (batches processed,
    failures recovered, escalations).
    """
    logger = event_logger or AgentEventLogger()
    held_lock = lock or AgentLock()
    held_lock.acquire()
    agent_run_id = manifest.start_agent_run()
    logger.event(EVENT_LOOP_STARTED, agent_run_id=agent_run_id)

    batches_processed = 0
    failures_recovered = 0
    escalations = 0
    status = RunStatus.COMPLETED

    try:
        pending = scan(manifest, source_root)
        logger.event(EVENT_LOOP_ITERATION, pending_count=len(pending))

        executor = Executor(
            manifest=manifest,
            agent_run_id=agent_run_id,
            classify=classify,
            build_fix=build_fix,
            escalate=escalate,
            retry_budget=retry_budget,
        )

        for batch_id in pending:
            logger.event(EVENT_BATCH_STARTED, batch_id=batch_id)
            steps = plan(batch_id, manifest=manifest, runners=runners_for(batch_id))
            batch_had_failure = False
            for layer, runner in steps:
                logger.event(
                    EVENT_LAYER_STARTED, batch_id=batch_id, layer=layer.value
                )
                result = executor.run_with_recovery(
                    layer=layer, batch_id=batch_id, fn=runner
                )
                failures_recovered += result.failures_recovered
                if result.outcome is Outcome.ESCALATED:
                    escalations += 1
                    batch_had_failure = True
                    # Stop processing this batch — the failed layer
                    # blocks downstream layers from even attempting.
                    break
                logger.event(
                    EVENT_LAYER_COMPLETED,
                    batch_id=batch_id,
                    layer=layer.value,
                    attempts=result.attempts,
                )
            batches_processed += 1
            if batch_had_failure:
                # Continue to the next batch — independence per
                # F4-RF-08: one failed batch must NOT stop sibling
                # batches.
                continue
    except BaseException:
        status = RunStatus.FAILED
        raise
    finally:
        manifest.end_agent_run(
            agent_run_id,
            status=status,
            batches_processed=batches_processed,
            failures_recovered=failures_recovered,
            escalations=escalations,
        )
        logger.event(
            EVENT_LOOP_STOPPED,
            agent_run_id=agent_run_id,
            status=status.value,
            batches_processed=batches_processed,
            failures_recovered=failures_recovered,
            escalations=escalations,
        )
        held_lock.release()

    return AgentResult(
        agent_run_id=agent_run_id,
        batches_processed=batches_processed,
        failures_recovered=failures_recovered,
        escalations=escalations,
        status=status,
    )
