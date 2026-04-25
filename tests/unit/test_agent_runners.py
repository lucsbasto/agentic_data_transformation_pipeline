"""Coverage for the WIRING-1 + WIRING-2 factories
(`pipeline.agent.runners`).

The factories assemble the production wiring for `run_once`. These
unit tests use monkeypatch to swap the inner `_run_<layer>`
helpers for spies so we can assert the runner closures call them
with the right arguments without actually invoking the heavy
F1/F2/F3 transforms.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from pipeline.agent import runners as runners_mod
from pipeline.agent.runners import (
    RunnerWiringError,
    _resolve_source,
    make_fix_builder,
    make_runners_for,
)
from pipeline.agent.types import ErrorKind, Layer
from pipeline.ingest.batch import compute_batch_identity
from pipeline.schemas.bronze import SOURCE_COLUMNS
from pipeline.settings import Settings

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


def _write_source(path: Path, *, marker: str) -> str:
    rows = {col: [marker] for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"]
    rows["direction"] = ["inbound"]
    rows["message_type"] = ["text"]
    rows["status"] = ["delivered"]
    df = pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return compute_batch_identity(path).batch_id


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[Settings]:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv(
        "PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef"
    )
    yield Settings.load()


# ---------------------------------------------------------------------------
# _resolve_source.
# ---------------------------------------------------------------------------


def test_resolve_source_returns_path_for_known_batch(tmp_path: Path) -> None:
    src = tmp_path / "raw" / "a.parquet"
    bid = _write_source(src, marker="a")
    assert _resolve_source(tmp_path / "raw", bid) == src


def test_resolve_source_raises_when_batch_unknown(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    _write_source(raw / "a.parquet", marker="a")
    with pytest.raises(RunnerWiringError, match="no source parquet"):
        _resolve_source(raw, "notarealbatch")


def test_resolve_source_raises_when_directory_missing(tmp_path: Path) -> None:
    with pytest.raises(RunnerWiringError, match="no source parquet"):
        _resolve_source(tmp_path / "missing", "anyid")


# ---------------------------------------------------------------------------
# make_runners_for.
# ---------------------------------------------------------------------------


def test_make_runners_for_returns_all_three_layers(
    tmp_path: Path, settings: Settings
) -> None:
    raw = tmp_path / "raw"
    bid = _write_source(raw / "a.parquet", marker="a")
    factory = make_runners_for(
        source_root=raw,
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        settings=settings,
    )
    runners = factory(bid)
    assert set(runners.keys()) == {Layer.BRONZE, Layer.SILVER, Layer.GOLD}
    for runner in runners.values():
        assert callable(runner)


def test_make_runners_for_bronze_runner_calls_run_ingest(
    tmp_path: Path, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    src = raw / "a.parquet"
    bid = _write_source(src, marker="a")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        runners_mod, "_run_ingest", lambda **kw: calls.append(kw)
    )
    factory = make_runners_for(
        source_root=raw,
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        settings=settings,
    )
    factory(bid)[Layer.BRONZE]()
    assert len(calls) == 1
    assert calls[0]["source"] == src
    assert calls[0]["bronze_root"] == tmp_path / "bronze"
    assert calls[0]["settings"] is settings


def test_make_runners_for_silver_runner_calls_run_silver(
    tmp_path: Path, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    bid = _write_source(raw / "a.parquet", marker="a")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        runners_mod, "_run_silver", lambda **kw: calls.append(kw)
    )
    factory = make_runners_for(
        source_root=raw,
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        settings=settings,
    )
    factory(bid)[Layer.SILVER]()
    assert len(calls) == 1
    assert calls[0]["batch_id"] == bid
    assert calls[0]["bronze_root"] == tmp_path / "bronze"
    assert calls[0]["silver_root"] == tmp_path / "silver"
    # Each call should mint a fresh run_id.
    assert isinstance(calls[0]["run_id"], str) and len(calls[0]["run_id"]) > 0


def test_make_runners_for_gold_runner_calls_run_gold(
    tmp_path: Path, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    bid = _write_source(raw / "a.parquet", marker="a")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        runners_mod, "_run_gold", lambda **kw: calls.append(kw)
    )
    factory = make_runners_for(
        source_root=raw,
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        settings=settings,
    )
    factory(bid)[Layer.GOLD]()
    assert len(calls) == 1
    assert calls[0]["batch_id"] == bid
    assert calls[0]["silver_root"] == tmp_path / "silver"
    assert calls[0]["gold_root"] == tmp_path / "gold"


# ---------------------------------------------------------------------------
# make_fix_builder.
# ---------------------------------------------------------------------------


def test_make_fix_builder_dispatches_schema_drift(tmp_path: Path) -> None:
    builder = make_fix_builder(
        source_root=tmp_path / "raw",
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
    )
    fix = builder(
        RuntimeError("x"), ErrorKind.SCHEMA_DRIFT, Layer.BRONZE, "bid01"
    )
    assert fix is not None
    assert fix.kind == "schema_drift_repair"


def test_make_fix_builder_dispatches_partition_missing(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    bid = _write_source(raw / "a.parquet", marker="a")
    builder = make_fix_builder(
        source_root=raw,
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
    )
    fix = builder(
        FileNotFoundError("x"), ErrorKind.PARTITION_MISSING, Layer.BRONZE, bid
    )
    assert fix is not None
    assert fix.kind == "recreate_partition"


def test_make_fix_builder_returns_none_when_partition_source_unknown(
    tmp_path: Path,
) -> None:
    """If `_resolve_source` raises (no source for the batch), the
    dispatcher returns None so the executor escalates rather than
    crashes inside `build_fix`."""
    builder = make_fix_builder(
        source_root=tmp_path / "raw",  # empty
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
    )
    assert (
        builder(
            FileNotFoundError("x"),
            ErrorKind.PARTITION_MISSING,
            Layer.BRONZE,
            "ghost",
        )
        is None
    )


def test_make_fix_builder_dispatches_out_of_range(tmp_path: Path) -> None:
    builder = make_fix_builder(
        source_root=tmp_path / "raw",
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
    )
    fix = builder(
        RuntimeError("x"), ErrorKind.OUT_OF_RANGE, Layer.SILVER, "bid01"
    )
    assert fix is not None
    assert fix.kind == "acknowledge_quarantine"


def test_make_fix_builder_returns_none_for_regex_break(tmp_path: Path) -> None:
    """Regex_break needs sample messages + baseline that the
    exception context does not carry — dispatcher punts and the
    executor escalates."""
    builder = make_fix_builder(
        source_root=tmp_path / "raw",
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
    )
    assert (
        builder(
            RuntimeError("x"), ErrorKind.REGEX_BREAK, Layer.SILVER, "bid01"
        )
        is None
    )


def test_make_fix_builder_returns_none_for_unknown(tmp_path: Path) -> None:
    builder = make_fix_builder(
        source_root=tmp_path / "raw",
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
    )
    assert (
        builder(RuntimeError("x"), ErrorKind.UNKNOWN, Layer.GOLD, "bid01")
        is None
    )
