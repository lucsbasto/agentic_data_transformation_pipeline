"""Integration tests for ``python -m pipeline silver``."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from click.testing import CliRunner

from pipeline.cli.ingest import ingest
from pipeline.cli.silver import silver
from pipeline.llm.client import LLMResponse
from pipeline.schemas.bronze import BRONZE_SCHEMA
from pipeline.schemas.silver import SILVER_SCHEMA
from pipeline.silver.quarantine import REJECTED_SCHEMA
from pipeline.state.manifest import ManifestDB

pytestmark = pytest.mark.integration


_NULL_LLM_PAYLOAD: str = json.dumps(
    {
        "veiculo_marca": None,
        "veiculo_modelo": None,
        "veiculo_ano": None,
        "concorrente_mencionado": None,
        "valor_pago_atual_brl": None,
        "sinistro_historico": None,
    }
)


class _StubLLMClient:
    """Stand-in for :class:`pipeline.llm.client.LLMClient`.

    Never touches the network. By default it returns an all-null JSON
    payload — the six LLM-extracted columns end up null, which leaves
    every unrelated assertion in the existing silver CLI tests
    intact. A per-test :func:`monkeypatch.setattr` can replace this
    with a stub that returns populated JSON to exercise the enrichment
    path end-to-end.
    """

    payload: str = _NULL_LLM_PAYLOAD

    def __init__(
        self, settings: Any, cache: Any, **_kwargs: Any
    ) -> None:
        self.settings = settings
        self.cache = cache

    def cached_call(
        self, *, system: str, user: str, **_kwargs: Any
    ) -> LLMResponse:
        return LLMResponse(
            text=self.payload,
            model="stub",
            input_tokens=0,
            output_tokens=0,
            cache_hit=False,
        )


@pytest.fixture(autouse=True)
def _stub_llm_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the CLI's ``LLMClient`` so silver never hits DashScope.

    Individual tests that want the populated-columns path can still
    call ``monkeypatch.setattr("pipeline.cli.silver.LLMClient", ...)``
    again — the last patch wins.
    """
    monkeypatch.setattr("pipeline.cli.silver.LLMClient", _StubLLMClient)


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


def test_cli_silver_quarantines_rows_with_null_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bronze rows missing message_id or conversation_id must land in a
    sibling ``rejected/`` partition and the manifest run must count
    them — never silently dropped.
    """
    state_db = _configure_env(tmp_path, monkeypatch)

    # Build a Bronze partition by hand: three valid rows + one bad row
    # with a null message_id. We bypass the ingest CLI here because the
    # fixture has only good rows; the batch row is inserted directly.
    bronze_root = tmp_path / "bronze"
    batch_id = "b-quarantine-test"
    partition_dir = bronze_root / f"batch_id={batch_id}"
    partition_dir.mkdir(parents=True)
    rows = [
        {
            "message_id": mid,
            "conversation_id": "c1",
            "timestamp": datetime(2026, 4, 23, 12, 0, 0),
            "direction": "inbound",
            "sender_phone": "+5511987654321",
            "sender_name": "Ana",
            "message_type": "text",
            "message_body": "hi",
            "status": "sent",
            "channel": "whatsapp",
            "campaign_id": "c",
            "agent_id": "a",
            "conversation_outcome": None,
            "metadata": "{}",
            "batch_id": batch_id,
            "ingested_at": datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC),
            "source_file_hash": "deadbeef",
        }
        for mid in ["m1", None, "m3", "m4"]
    ]
    pl.DataFrame(rows, schema=BRONZE_SCHEMA).write_parquet(
        partition_dir / "part-0.parquet"
    )

    # Register the batch in the manifest so silver can find it.
    with ManifestDB(state_db) as manifest:
        manifest.insert_batch(
            batch_id=batch_id,
            source_path=str(partition_dir / "part-0.parquet"),
            source_hash="deadbeef",
            source_mtime=0,
            started_at="2026-04-23T12:00:00Z",
        )
        manifest.mark_completed(
            batch_id=batch_id,
            rows_read=4,
            rows_written=4,
            bronze_path=str(partition_dir / "part-0.parquet"),
            finished_at="2026-04-23T12:00:01Z",
            duration_ms=1,
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
    assert "rejected 1" in result.output

    # Rejected partition exists with the declared schema.
    rejected_files = list(silver_root.glob("batch_id=*/rejected/part-*.parquet"))
    assert len(rejected_files) == 1
    rejected_df = pl.scan_parquet(rejected_files[0]).collect()
    assert rejected_df.schema == REJECTED_SCHEMA
    assert rejected_df.height == 1
    assert rejected_df["reject_reason"][0] == "null_message_id"

    # Valid Silver partition has the 3 surviving rows.
    silver_files = list(silver_root.glob("batch_id=*/part-*.parquet"))
    assert len(silver_files) == 1
    silver_df = pl.scan_parquet(silver_files[0]).collect()
    assert silver_df.height == 3

    # Manifest run row carries the quarantine counter.
    with ManifestDB(state_db) as manifest:
        run = manifest.get_latest_run(batch_id=batch_id, layer="silver")
    assert run is not None
    assert run.rows_in == 4
    assert run.rows_out == 3
    assert run.rows_rejected == 1
    assert run.rows_deduped == 0


def test_cli_silver_populates_llm_extracted_columns(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A populated LLM stub fills the six §6.2 entity columns without
    regressing ``audio_confidence`` or ``has_content``.
    """
    _configure_env(tmp_path, monkeypatch)

    populated_payload = json.dumps(
        {
            "veiculo_marca": "Toyota",
            "veiculo_modelo": "Corolla",
            "veiculo_ano": 2020,
            "concorrente_mencionado": "Porto Seguro",
            "valor_pago_atual_brl": 1850.5,
            "sinistro_historico": False,
        }
    )

    class _PopulatedClient(_StubLLMClient):
        payload = populated_payload

    monkeypatch.setattr("pipeline.cli.silver.LLMClient", _PopulatedClient)

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

    silver_files = list(silver_root.glob("batch_id=*/part-*.parquet"))
    assert len(silver_files) == 1
    out = pl.scan_parquet(silver_files[0]).collect()

    # Schema carries every LLM-extracted column with its declared dtype.
    assert out.schema == SILVER_SCHEMA
    for col in (
        "veiculo_marca",
        "veiculo_modelo",
        "veiculo_ano",
        "concorrente_mencionado",
        "valor_pago_atual_brl",
        "sinistro_historico",
    ):
        assert col in out.columns

    # Populated LLM stub -> every row carries the fixed payload values.
    assert out.height > 0
    assert set(out["veiculo_marca"].to_list()) == {"Toyota"}
    assert set(out["veiculo_modelo"].to_list()) == {"Corolla"}
    assert set(out["veiculo_ano"].to_list()) == {2020}
    assert set(out["concorrente_mencionado"].to_list()) == {"Porto Seguro"}
    assert set(out["valor_pago_atual_brl"].to_list()) == {1850.5}
    assert set(out["sinistro_historico"].to_list()) == {False}

    # Regression guard: the audio / content lanes from earlier F2
    # slices must keep working alongside the LLM lane.
    assert "audio_confidence" in out.columns
    assert "has_content" in out.columns
    assert out["has_content"].null_count() == 0


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
