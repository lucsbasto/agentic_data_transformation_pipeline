"""Coverage for the F4 agent event logger (F4.13)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from pipeline.agent._logging import (
    CANONICAL_EVENTS,
    DEFAULT_LOG_PATH,
    EVENT_BATCH_STARTED,
    EVENT_ESCALATION,
    EVENT_FAILURE_DETECTED,
    EVENT_FIX_APPLIED,
    EVENT_LAYER_COMPLETED,
    EVENT_LAYER_STARTED,
    EVENT_LOOP_ITERATION,
    EVENT_LOOP_STARTED,
    EVENT_LOOP_STOPPED,
    AgentEventLogger,
    default_clock,
    fixed_clock,
    redact_secrets,
    redact_secrets_processor,
)

_FIXED = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Canonical event names — design §13 table.
# ---------------------------------------------------------------------------


def test_canonical_events_match_design_table() -> None:
    """A future rename / removal of any event surfaces here loudly."""
    assert set(CANONICAL_EVENTS) == {
        EVENT_LOOP_STARTED,
        EVENT_LOOP_ITERATION,
        EVENT_BATCH_STARTED,
        EVENT_LAYER_STARTED,
        EVENT_LAYER_COMPLETED,
        EVENT_FAILURE_DETECTED,
        EVENT_FIX_APPLIED,
        EVENT_ESCALATION,
        EVENT_LOOP_STOPPED,
    }


def test_canonical_events_count_pinned() -> None:
    assert len(CANONICAL_EVENTS) == 9


# ---------------------------------------------------------------------------
# AgentEventLogger.event.
# ---------------------------------------------------------------------------


def test_event_appends_one_jsonl_line(tmp_path: Path) -> None:
    log_path = tmp_path / "agent.jsonl"
    logger = AgentEventLogger(log_path=log_path, clock=fixed_clock(_FIXED))
    logger.event(EVENT_LOOP_STARTED, interval=60)
    line = log_path.read_text(encoding="utf-8").strip()
    assert json.loads(line) == {
        "event": "loop_started",
        "ts": "2026-04-25T12:00:00+00:00",
        "interval": 60,
    }


def test_event_appends_multiple_lines_in_order(tmp_path: Path) -> None:
    log_path = tmp_path / "agent.jsonl"
    logger = AgentEventLogger(log_path=log_path, clock=fixed_clock(_FIXED))
    logger.event(EVENT_LOOP_STARTED, interval=60)
    logger.event(EVENT_LOOP_ITERATION, iter=1, pending_count=3)
    logger.event(EVENT_LOOP_STOPPED, reason="completed")
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["event"] for line in lines] == [
        "loop_started",
        "loop_iteration",
        "loop_stopped",
    ]


def test_event_returns_written_payload(tmp_path: Path) -> None:
    """The return value lets callers (escalator, executor) inspect
    exactly what was committed to disk without re-parsing the file."""
    logger = AgentEventLogger(log_path=tmp_path / "agent.jsonl", clock=fixed_clock(_FIXED))
    payload = logger.event(EVENT_FIX_APPLIED, batch_id="bid01", fix_kind="x")
    assert payload == {
        "event": "fix_applied",
        "ts": "2026-04-25T12:00:00+00:00",
        "batch_id": "bid01",
        "fix_kind": "x",
    }


def test_event_creates_parent_directories_lazily(tmp_path: Path) -> None:
    """Mirrors the real `state/` / `logs/` paths — parent may not
    exist on a fresh checkout."""
    log_path = tmp_path / "deep" / "nested" / "agent.jsonl"
    logger = AgentEventLogger(log_path=log_path, clock=fixed_clock(_FIXED))
    logger.event(EVENT_LOOP_STARTED)
    assert log_path.exists()


def test_event_uses_injected_clock_for_byte_stable_replay(tmp_path: Path) -> None:
    """NFR-06: same fixture + same fixed clock = byte-identical
    log output on replay."""
    log_a = tmp_path / "a.jsonl"
    log_b = tmp_path / "b.jsonl"
    for path in (log_a, log_b):
        logger = AgentEventLogger(log_path=path, clock=fixed_clock(_FIXED))
        logger.event(EVENT_LAYER_STARTED, batch_id="bid01", layer="silver")
        logger.event(EVENT_LAYER_COMPLETED, batch_id="bid01", layer="silver", duration_ms=120)
        logger.event(EVENT_FAILURE_DETECTED, batch_id="bid01", layer="silver", attempt=1)
    assert log_a.read_bytes() == log_b.read_bytes()


def test_event_emits_to_stdout_logger(tmp_path: Path) -> None:
    """The structlog stdout side stays in sync with the JSONL side
    so operators tailing the terminal see what landed on disk."""
    captured: list[tuple[str, dict]] = []

    class _StubLogger:
        def info(self, name: str, **fields) -> None:
            captured.append((name, fields))

    logger = AgentEventLogger(
        log_path=tmp_path / "agent.jsonl",
        clock=fixed_clock(_FIXED),
        stdout_logger=_StubLogger(),
    )
    logger.event(EVENT_BATCH_STARTED, batch_id="bid01")
    assert captured == [("batch_started", {"batch_id": "bid01"})]


# ---------------------------------------------------------------------------
# Clock helpers.
# ---------------------------------------------------------------------------


def test_default_clock_returns_aware_utc_datetime() -> None:
    now = default_clock()
    assert now.tzinfo is not None
    assert now.utcoffset().total_seconds() == 0


def test_fixed_clock_returns_pinned_value() -> None:
    clock = fixed_clock(_FIXED)
    assert clock() == _FIXED
    assert clock() == _FIXED  # repeatable


# ---------------------------------------------------------------------------
# Defaults.
# ---------------------------------------------------------------------------


def test_default_log_path_matches_design() -> None:
    """``logs/agent.jsonl`` per design §9 + §13 — pin so a quiet
    rename surfaces in CI."""
    assert Path("logs/agent.jsonl") == DEFAULT_LOG_PATH


# ---------------------------------------------------------------------------
# F7.3 — secret redaction processor.
# ---------------------------------------------------------------------------


def test_redact_secrets_masks_nested_dict_secret_keys() -> None:
    """A nested dict whose key matches *_KEY/_TOKEN/_SECRET/_PASSWORD
    must be replaced with ``"<redacted>"`` no matter the depth."""
    payload = {
        "outer": {
            "DASHSCOPE_API_KEY": "sk-secret-value-1234567890abcdef",
            "inner": {"slack_token": "xoxb-abc-123"},
        }
    }
    out = redact_secrets(payload)
    assert out["outer"]["DASHSCOPE_API_KEY"] == "<redacted>"
    assert out["outer"]["inner"]["slack_token"] == "<redacted>"


def test_redact_secrets_walks_lists_of_dicts() -> None:
    payload = {
        "secrets": [
            {"DB_PASSWORD": "hunter2"},
            {"unrelated": "ok"},
        ]
    }
    out = redact_secrets(payload)
    assert out["secrets"][0]["DB_PASSWORD"] == "<redacted>"
    assert out["secrets"][1]["unrelated"] == "ok"


def test_redact_secrets_matches_sk_value_pattern() -> None:
    """A key whose name looks innocuous but holds an ``sk-...`` API
    key must still be redacted on the value-pattern path."""
    payload = {"raw_response": "got back token sk-1234567890ABCDEFGHIJ"}
    out = redact_secrets(payload)
    assert out["raw_response"] == "<redacted>"


def test_redact_secrets_case_insensitive_key_match() -> None:
    """Key matching is case-insensitive — ``api_key``, ``API_KEY``,
    and ``Api_Key`` all redact identically."""
    payload = {
        "api_key": "v1",
        "API_KEY": "v2",
        "Api_Key": "v3",
    }
    out = redact_secrets(payload)
    assert out == {
        "api_key": "<redacted>",
        "API_KEY": "<redacted>",
        "Api_Key": "<redacted>",
    }


def test_redact_secrets_leaves_plaintext_untouched() -> None:
    """Innocuous strings, ints, bools, None, and primitive-only
    structures pass through verbatim — no false positives."""
    payload = {
        "agent_run_id": "abc123",
        "iter": 5,
        "ok": True,
        "missing": None,
        "items": ["a", "b", 42],
    }
    out = redact_secrets(payload)
    assert out == payload


def test_redact_secrets_processor_signature_matches_structlog() -> None:
    """The 3-arg wrapper plugs into ``structlog.configure(processors=[...])``
    without any adapter — same shape as built-in processors."""
    event_dict = {"event": "x", "DB_PASSWORD": "leaky"}
    out = redact_secrets_processor(None, "info", event_dict)
    assert out == {"event": "x", "DB_PASSWORD": "<redacted>"}


def test_event_redacts_secret_fields_before_writing(tmp_path: Path) -> None:
    """End-to-end: AgentEventLogger.event writes a redacted JSONL line
    when called with a key that matches the secret pattern."""
    log_path = tmp_path / "agent.jsonl"
    logger = AgentEventLogger(log_path=log_path, clock=fixed_clock(_FIXED))
    logger.event(EVENT_LOOP_STARTED, DASHSCOPE_API_KEY="sk-leaked-1234567890abcdef")
    line = log_path.read_text(encoding="utf-8").strip()
    parsed = json.loads(line)
    assert parsed["DASHSCOPE_API_KEY"] == "<redacted>"
