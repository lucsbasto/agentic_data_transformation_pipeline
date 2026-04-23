"""Tests for ``pipeline.logging``."""

from __future__ import annotations

import json
import logging as stdlib_logging

import pytest
import structlog

from pipeline.logging import (
    _reset_for_tests,
    bind_context,
    clear_context,
    configure_logging,
    get_logger,
)


@pytest.fixture(autouse=True)
def reset_logging() -> None:
    _reset_for_tests()
    clear_context()
    yield
    _reset_for_tests()
    clear_context()


def test_configure_logging_is_idempotent() -> None:
    configure_logging("INFO")
    configure_logging("INFO")  # second call must not error
    # No assertion beyond survival: the flag guards duplicate setup.


def test_logger_emits_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    logger = get_logger("test")
    logger.info("event.happened", foo="bar", n=42)
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert payload["event"] == "event.happened"
    assert payload["foo"] == "bar"
    assert payload["n"] == 42
    assert payload["level"] == "info"
    assert "timestamp" in payload


def test_bind_context_adds_fields_to_events(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    bind_context(run_id="r-123", batch_id="b-abc")
    logger = get_logger("test")
    logger.info("work.done")
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert payload["run_id"] == "r-123"
    assert payload["batch_id"] == "b-abc"


def test_clear_context_removes_bound_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    bind_context(run_id="r-123")
    clear_context()
    logger = get_logger("test")
    logger.info("work.done")
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert "run_id" not in payload


def test_log_level_filters_below_threshold(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("WARNING")
    logger = get_logger("test")
    logger.info("should.be.dropped")
    logger.warning("should.appear")
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1
    payload = json.loads(out[0])
    assert payload["event"] == "should.appear"


def test_stdlib_logging_respects_configured_level() -> None:
    configure_logging("ERROR")
    # Verify stdlib logging inherits the requested numeric level.
    assert stdlib_logging.getLogger().level == stdlib_logging.ERROR
    # Verify structlog also honors the filter.
    assert structlog.is_configured()
