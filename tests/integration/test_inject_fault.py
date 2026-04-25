"""Coverage for the F4 fault injection helper script (F4.17)."""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest
from click.testing import CliRunner

# The script lives outside ``src/`` so we add ``scripts/`` to sys.path
# before importing it. This mirrors how a developer would run it.
_SCRIPTS_DIR = Path(__file__).parents[2] / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

import inject_fault  # noqa: E402 — added to sys.path above

from pipeline.agent.diagnoser import _DiagnoseBudget, classify  # noqa: E402
from pipeline.agent.fixes.partition_missing import recreate_partition  # noqa: E402
from pipeline.agent.fixes.schema_drift import detect_delta  # noqa: E402
from pipeline.agent.types import ErrorKind, Layer  # noqa: E402
from pipeline.ingest.batch import compute_batch_identity  # noqa: E402
from pipeline.schemas.bronze import BRONZE_SCHEMA, SOURCE_COLUMNS  # noqa: E402

# ---------------------------------------------------------------------------
# Fixture helpers.
# ---------------------------------------------------------------------------


def _write_source_parquet(path: Path, *, marker: str = "x") -> None:
    rows = {col: [marker] for col in SOURCE_COLUMNS}
    rows["timestamp"] = ["2026-04-25 12:00:00"]
    rows["direction"] = ["inbound"]
    rows["message_type"] = ["text"]
    rows["status"] = ["delivered"]
    rows["message_body"] = ["original mensagem"]
    df = pl.DataFrame(rows, schema=dict.fromkeys(SOURCE_COLUMNS, pl.String))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _seed_bronze_partition(tmp_path: Path) -> Path:
    """Run the ingest pipeline to produce a Bronze parquet under
    ``tmp_path/bronze/batch_id=<id>/part-0.parquet`` — the same
    layout the recreate_partition fix consumes."""
    src = tmp_path / "raw" / "conversations.parquet"
    _write_source_parquet(src)
    bronze_root = tmp_path / "bronze"
    identity = compute_batch_identity(src)
    recreate_partition(source=src, bronze_root=bronze_root, batch_id=identity.batch_id)
    return bronze_root / f"batch_id={identity.batch_id}" / "part-0.parquet"


# ---------------------------------------------------------------------------
# inject_schema_drift.
# ---------------------------------------------------------------------------


def test_inject_schema_drift_adds_unexpected_column(tmp_path: Path) -> None:
    parquet_path = _seed_bronze_partition(tmp_path)
    inject_fault.inject_schema_drift(parquet_path)
    df = pl.read_parquet(parquet_path)
    assert "injected_col" in df.columns
    delta = detect_delta(df.schema, BRONZE_SCHEMA)
    assert "injected_col" in delta.extra_cols


def test_inject_schema_drift_raises_on_missing_target(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        inject_fault.inject_schema_drift(tmp_path / "missing.parquet")


# ---------------------------------------------------------------------------
# inject_regex_break.
# ---------------------------------------------------------------------------


def test_inject_regex_break_replaces_first_message_body(tmp_path: Path) -> None:
    src = tmp_path / "raw.parquet"
    _write_source_parquet(src)
    inject_fault.inject_regex_break(src)
    df = pl.read_parquet(src)
    assert df["message_body"][0] == "❌ R$ 1.500,00"


def test_inject_regex_break_raises_on_empty_parquet(tmp_path: Path) -> None:
    src = tmp_path / "raw.parquet"
    pl.DataFrame(schema=dict.fromkeys(SOURCE_COLUMNS, pl.String)).write_parquet(src)
    with pytest.raises(ValueError, match="empty"):
        inject_fault.inject_regex_break(src)


# ---------------------------------------------------------------------------
# inject_partition_missing.
# ---------------------------------------------------------------------------


def test_inject_partition_missing_removes_directory(tmp_path: Path) -> None:
    parquet_path = _seed_bronze_partition(tmp_path)
    partition_dir = parquet_path.parent
    inject_fault.inject_partition_missing(partition_dir)
    assert not partition_dir.exists()


def test_inject_partition_missing_removes_single_file(tmp_path: Path) -> None:
    parquet_path = _seed_bronze_partition(tmp_path)
    inject_fault.inject_partition_missing(parquet_path)
    assert not parquet_path.exists()


def test_inject_partition_missing_raises_when_target_absent(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        inject_fault.inject_partition_missing(tmp_path / "ghost")


# ---------------------------------------------------------------------------
# inject_out_of_range.
# ---------------------------------------------------------------------------


def test_inject_out_of_range_appends_negative_value_row(tmp_path: Path) -> None:
    src = tmp_path / "raw.parquet"
    _write_source_parquet(src)
    inject_fault.inject_out_of_range(src)
    df = pl.read_parquet(src)
    assert df.height == 2
    assert df["message_body"][1] == "valor_pago_atual_brl=-999"


# ---------------------------------------------------------------------------
# inject (dispatch) + CLI.
# ---------------------------------------------------------------------------


def test_inject_dispatch_rejects_unknown_kind(tmp_path: Path) -> None:
    src = tmp_path / "raw.parquet"
    _write_source_parquet(src)
    with pytest.raises(Exception, match="unknown --kind"):
        inject_fault.inject("cosmic_ray", src)


def test_cli_main_dispatches_by_kind(tmp_path: Path) -> None:
    src = tmp_path / "raw.parquet"
    _write_source_parquet(src)
    runner = CliRunner()
    result = runner.invoke(
        inject_fault.main,
        ["--kind", "regex_break", "--target", str(src)],
    )
    assert result.exit_code == 0, result.output
    assert "injected regex_break" in result.output
    df = pl.read_parquet(src)
    assert df["message_body"][0] == "❌ R$ 1.500,00"


def test_cli_main_reports_missing_target(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        inject_fault.main,
        ["--kind", "schema_drift", "--target", str(tmp_path / "missing.parquet")],
    )
    assert result.exit_code != 0
    assert "target does not exist" in result.output


def test_cli_main_lists_every_canonical_kind() -> None:
    runner = CliRunner()
    result = runner.invoke(inject_fault.main, ["--help"])
    assert result.exit_code == 0
    for kind in inject_fault.KIND_CHOICES:
        assert kind in result.output


# ---------------------------------------------------------------------------
# inject_unknown.
# ---------------------------------------------------------------------------


def test_inject_unknown_truncates_parquet(tmp_path: Path) -> None:
    src = tmp_path / "raw.parquet"
    _write_source_parquet(src)
    original_size = src.stat().st_size
    inject_fault.inject_unknown(src)
    assert src.stat().st_size < original_size
    # polars must fail to read the corrupted file
    with pytest.raises(BaseException):  # noqa: B017 — any read error is valid
        pl.read_parquet(src)


def test_inject_unknown_raises_on_missing_target(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        inject_fault.inject_unknown(tmp_path / "missing.parquet")


def test_inject_unknown_raises_on_tiny_file(tmp_path: Path) -> None:
    src = tmp_path / "tiny.parquet"
    src.write_bytes(b"PAR1")  # only 4 bytes — too small to truncate meaningfully
    with pytest.raises(ValueError, match="too small"):
        inject_fault.inject_unknown(src)


def test_inject_unknown_forces_unknown_via_classifier(tmp_path: Path) -> None:
    """Corrupted bytes -> polars raises a non-deterministic exception ->
    classify with budget=0 returns UNKNOWN (no LLM call needed)."""
    src = tmp_path / "raw.parquet"
    _write_source_parquet(src)
    inject_fault.inject_unknown(src)

    try:
        pl.read_parquet(src)
    except BaseException as exc:
        kind = classify(
            exc,
            layer=Layer.BRONZE,
            batch_id="bid_unknown",
            budget=_DiagnoseBudget(cap=0),
        )
        assert kind is ErrorKind.UNKNOWN
    else:
        pytest.fail("Expected polars to raise on the corrupted parquet")
