"""Integration tests for ``python -m pipeline silver``."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from click.testing import CliRunner

from pipeline.cli.ingest import ingest
from pipeline.cli.silver import silver
from pipeline.schemas.silver import SILVER_SCHEMA
from pipeline.state.manifest import ManifestDB

pytestmark = pytest.mark.integration


def _configure_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Isolate env vars and return the state DB path."""
    for key in [
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_BASE_URL",
        "LLM_MODEL_PRIMARY",
        "LLM_MODEL_FALLBACK",
        "PIPELINE_RETRY_BUDGET",
        "PIPELINE_LOOP_SLEEP_SECONDS",
        "PIPELINE_STATE_DB",
        "PIPELINE_LOG_LEVEL",
        "PIPELINE_LEAD_SECRET",
    ]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv(
        "PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef"
    )
    state_db = tmp_path / "manifest.db"
    monkeypatch.setenv("PIPELINE_STATE_DB", str(state_db))
    monkeypatch.chdir(tmp_path)
    return state_db


def _run_ingest_and_return_batch_id(
    tmp_path: Path, source: Path
) -> tuple[str, Path]:
    """Run ingest against ``source`` and return ``(batch_id, bronze_root)``."""
    bronze_root = tmp_path / "bronze"
    runner = CliRunner()
    result = runner.invoke(
        ingest,
        ["--source", str(source), "--bronze-root", str(bronze_root)],
    )
    assert result.exit_code == 0, result.output
    partitions = list(bronze_root.glob("batch_id=*/part-*.parquet"))
    assert len(partitions) == 1
    batch_id = partitions[0].parent.name.split("=", 1)[1]
    return batch_id, bronze_root


def test_cli_silver_happy_path(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_db = _configure_env(tmp_path, monkeypatch)
    batch_id, bronze_root = _run_ingest_and_return_batch_id(
        tmp_path, tiny_source_parquet
    )
    silver_root = tmp_path / "silver"

    runner = CliRunner()
    result = runner.invoke(
        silver,
        [
            "--batch-id",
            batch_id,
            "--bronze-root",
            str(bronze_root),
            "--silver-root",
            str(silver_root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "silver wrote" in result.output

    # Silver partition exists with the exact declared schema.
    silver_files = list(silver_root.glob("batch_id=*/part-*.parquet"))
    assert len(silver_files) == 1
    out = pl.scan_parquet(silver_files[0]).collect()
    assert out.schema == SILVER_SCHEMA
    assert out.height > 0
    # Every row carries a lead_id (no null phones in the fixture).
    assert out["lead_id"].null_count() == 0

    # Manifest run row is COMPLETED with rows_in/rows_out/output_path.
    with ManifestDB(state_db) as manifest:
        run = manifest.get_latest_run(batch_id=batch_id, layer="silver")
    assert run is not None
    assert run.status == "COMPLETED"
    assert run.rows_in is not None and run.rows_in > 0
    assert run.rows_out == out.height
    assert run.rows_deduped == 0  # fixture has unique (conv, msg) pairs
    assert run.output_path is not None
    assert run.output_path.endswith("part-0.parquet")


def test_cli_silver_is_idempotent(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_env(tmp_path, monkeypatch)
    batch_id, bronze_root = _run_ingest_and_return_batch_id(
        tmp_path, tiny_source_parquet
    )
    silver_root = tmp_path / "silver"
    runner = CliRunner()

    first = runner.invoke(
        silver,
        [
            "--batch-id",
            batch_id,
            "--bronze-root",
            str(bronze_root),
            "--silver-root",
            str(silver_root),
        ],
    )
    second = runner.invoke(
        silver,
        [
            "--batch-id",
            batch_id,
            "--bronze-root",
            str(bronze_root),
            "--silver-root",
            str(silver_root),
        ],
    )
    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert "already computed" in second.output


def test_cli_silver_rejects_unknown_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_env(tmp_path, monkeypatch)
    # Bronze root is empty — any batch_id fails the partition-exists check.
    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        silver,
        [
            "--batch-id",
            "b-does-not-exist",
            "--bronze-root",
            str(bronze_root),
            "--silver-root",
            str(tmp_path / "silver"),
        ],
    )
    assert result.exit_code != 0
    assert "Bronze partition not found" in result.output


def test_cli_silver_rejects_path_shaped_batch_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_env(tmp_path, monkeypatch)
    runner = CliRunner()
    result = runner.invoke(
        silver,
        [
            "--batch-id",
            "../escape",
            "--bronze-root",
            str(tmp_path / "bronze"),
            "--silver-root",
            str(tmp_path / "silver"),
        ],
    )
    assert result.exit_code != 0
    assert "invalid --batch-id" in result.output


def test_cli_silver_rejects_path_traversal_in_silver_root(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_env(tmp_path, monkeypatch)
    batch_id, bronze_root = _run_ingest_and_return_batch_id(
        tmp_path, tiny_source_parquet
    )
    runner = CliRunner()
    result = runner.invoke(
        silver,
        [
            "--batch-id",
            batch_id,
            "--bronze-root",
            str(bronze_root),
            "--silver-root",
            "../../etc/silver",
        ],
    )
    assert result.exit_code != 0
    assert "'..' segments are not allowed in --silver-root" in result.output
