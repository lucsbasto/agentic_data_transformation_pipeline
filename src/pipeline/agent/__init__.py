"""Agent core for the self-healing pipeline (F4).

This package owns the agentic loop that drives Bronze -> Silver -> Gold
end-to-end: observer detects pending batches, planner sequences the
layers, executor invokes them with a retry budget, diagnoser classifies
failures, and escalator emits structured alerts when the budget runs
out. Public surface is re-exported from this module so downstream code
(F5 CLI, demo scripts) imports from ``pipeline.agent`` directly.
"""

from __future__ import annotations

from pipeline.agent.loop import run_once
from pipeline.agent.types import (
    AgentResult,
    ErrorKind,
    FailureRecord,
    Fix,
    Layer,
    RunStatus,
)

__all__ = [
    "AgentResult",
    "ErrorKind",
    "FailureRecord",
    "Fix",
    "Layer",
    "RunStatus",
    "run_once",
]
