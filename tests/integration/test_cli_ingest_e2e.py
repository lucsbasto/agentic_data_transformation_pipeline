"""Integration tests for ``python -m pipeline ingest``."""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest
from click.testing import CliRunner

from pipeline.cli.ingest import ingest
from pipeline.state.manifest import ManifestDB

pytestmark = pytest.mark.integration


def _run_ingest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source: Path,
) -> tuple[int, str, Path, Path]:
    """Run the ingest command against an isolated working directory.

    Returns (exit_code, stdout, bronze_root, state_db).
    """
    for key in [
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_BASE_URL",
        "LLM_MODEL_PRIMARY",
        "LLM_MODEL_FALLBACK",
        "PIPELINE_RETRY_BUDGET",
        "PIPELINE_LOOP_SLEEP_SECONDS",
        "PIPELINE_STATE_DB",
        "PIPELINE_LOG_LEVEL",
    ]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    # Absolute path so it is not re-rooted against the real repo's project_root.
    state_db = tmp_path / "manifest.db"
    monkeypatch.setenv("PIPELINE_STATE_DB", str(state_db))
    monkeypatch.chdir(tmp_path)

    bronze_root = tmp_path / "bronze"
    runner = CliRunner()
    result = runner.invoke(
        ingest,
        ["--source", str(source), "--bronze-root", str(bronze_root)],
    )
    return result.exit_code, result.output, bronze_root, state_db


def test_cli_ingest_happy_path(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, stdout, bronze_root, state_db = _run_ingest(
        tmp_path, monkeypatch, tiny_source_parquet
    )
    assert exit_code == 0, stdout
    assert "ingested" in stdout
    # Bronze partition exists and can be re-scanned.
    partitions = list(bronze_root.glob("batch_id=*/part-*.parquet"))
    assert len(partitions) == 1
    reloaded = pl.scan_parquet(partitions[0]).collect()
    assert reloaded.height > 0
    # Manifest row is COMPLETED.
    with ManifestDB(state_db) as manifest:
        # Match the partition directory name back to a batch_id.
        batch_id = partitions[0].parent.name.split("=", 1)[1]
        row = manifest.get_batch(batch_id)
    assert row is not None
    assert row.is_completed


def test_cli_ingest_is_idempotent(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_code, first_out, _, _ = _run_ingest(
        tmp_path, monkeypatch, tiny_source_parquet
    )
    second_code, second_out, _, _ = _run_ingest(
        tmp_path, monkeypatch, tiny_source_parquet
    )
    assert first_code == 0, first_out
    assert second_code == 0, second_out
    assert "already ingested" in second_out


def test_cli_ingest_missing_source_is_click_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        ingest,
        ["--source", str(tmp_path / "missing.parquet")],
    )
    assert result.exit_code != 0


def test_cli_ingest_rejects_path_traversal_in_bronze_root(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_STATE_DB", str(tmp_path / "manifest.db"))
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        ingest,
        [
            "--source",
            str(tiny_source_parquet),
            "--bronze-root",
            "../../etc/bronze",
        ],
    )
    assert result.exit_code != 0
    assert "'..' segments are not allowed in --bronze-root" in result.output


def test_cli_ingest_marks_failed_on_unexpected_exception(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-PipelineError inside the ingest step must still flip the row to FAILED."""

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated polars crash")

    # Patch the symbol the CLI module actually binds — not the definition site.
    # ``pipeline.cli.__init__`` re-exports ``ingest`` (the Command), which
    # shadows the submodule when accessed as ``pipeline.cli.ingest``. Reach
    # the real module object via ``sys.modules`` so the monkeypatch lands on
    # the ``transform_to_bronze`` reference used inside ``_run_ingest``.
    cli_module = sys.modules["pipeline.cli.ingest"]
    monkeypatch.setattr(cli_module, "transform_to_bronze", _boom)

    exit_code, stdout, _bronze_root, state_db = _run_ingest(
        tmp_path, monkeypatch, tiny_source_parquet
    )

    assert exit_code != 0
    assert "simulated polars crash" in stdout

    with ManifestDB(state_db) as manifest:
        # There must be exactly one batch row, and it must be FAILED with the
        # original exception class recorded — not left orphaned in IN_PROGRESS.
        assert manifest._conn is not None
        rows = manifest._conn.execute(
            "SELECT batch_id, status, error_type, error_message FROM batches;"
        ).fetchall()
    assert len(rows) == 1
    (_bid, status, error_type, error_message) = tuple(rows[0])
    assert status == "FAILED"
    assert error_type == "RuntimeError"
    assert error_message == "simulated polars crash"
