"""Integration tests for ``python -m pipeline gold``.

Mirrors ``tests/integration/test_cli_silver_e2e.py``: spins the full
CLI through ``CliRunner``, isolates env vars to ``tmp_path``, and
stubs the LLM lane so the test never hits DashScope.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from click.testing import CliRunner

from pipeline.__main__ import cli as pipeline_cli
from pipeline.cli.gold import gold
from pipeline.cli.ingest import ingest
from pipeline.cli.silver import silver
from pipeline.gold.persona import PersonaResult
from pipeline.llm.client import LLMResponse
from pipeline.schemas.gold import (
    GOLD_AGENT_PERFORMANCE_SCHEMA,
    GOLD_COMPETITOR_INTEL_SCHEMA,
    GOLD_CONVERSATION_SCORES_SCHEMA,
    GOLD_LEAD_PROFILE_SCHEMA,
)
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

    Returns the all-null Silver payload by default. Identical to the
    Silver test helper so both CLIs see the same shape of stub.
    """

    payload: str = _NULL_LLM_PAYLOAD

    def __init__(self, settings: Any, cache: Any, **_kwargs: Any) -> None:
        self.settings = settings
        self.cache = cache

    def cached_call(self, *, system: str, user: str, **_kwargs: Any) -> LLMResponse:
        return LLMResponse(
            text=self.payload,
            model="stub",
            input_tokens=0,
            output_tokens=0,
            cache_hit=False,
        )


def _null_persona_classifier(
    aggregates: list[Any], _batch_latest: datetime
) -> dict[str, PersonaResult]:
    """Default persona classifier stub: every lead → ``persona=None``.

    Keeps ``persona``/``persona_confidence`` null on the lead profile
    so the CLI can run end-to-end without exercising the LLM persona
    lane. The populated-persona test overrides this with its own
    classifier.
    """
    return {
        agg.lead_id: PersonaResult(persona=None, persona_confidence=None, persona_source="skipped")
        for agg in aggregates
    }


@pytest.fixture(autouse=True)
def _stub_llm_and_persona(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace LLM-touching call sites with deterministic stubs.

    Silver's CLI is invoked in setup (to create the Silver partition
    Gold reads), so we patch ``pipeline.cli.silver.LLMClient`` too.
    Gold's persona lane is patched at the orchestrator entry point
    via the ``persona_classifier`` keyword in
    :func:`pipeline.gold.transform.transform_gold` — we stub the CLI's
    default factory instead so individual tests can still override.
    """
    monkeypatch.setattr("pipeline.cli.silver.LLMClient", _StubLLMClient)
    monkeypatch.setattr(
        "pipeline.cli.gold._default_persona_classifier",
        lambda _settings: _null_persona_classifier,
    )


def _configure_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
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
    monkeypatch.setenv("PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef")
    state_db = tmp_path / "manifest.db"
    monkeypatch.setenv("PIPELINE_STATE_DB", str(state_db))
    monkeypatch.chdir(tmp_path)
    return state_db


def _run_ingest_and_silver(tmp_path: Path, source: Path) -> tuple[str, Path, Path]:
    """Run ingest + silver against ``source``; return ``(batch_id, silver_root, bronze_root)``."""
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    runner = CliRunner()
    result = runner.invoke(
        ingest,
        ["--source", str(source), "--bronze-root", str(bronze_root)],
    )
    assert result.exit_code == 0, result.output
    partitions = list(bronze_root.glob("batch_id=*/part-*.parquet"))
    assert len(partitions) == 1
    batch_id = partitions[0].parent.name.split("=", 1)[1]

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
    return batch_id, silver_root, bronze_root


# --------------------------------------------------------------- happy path


def test_cli_gold_happy_path(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_db = _configure_env(tmp_path, monkeypatch)
    batch_id, silver_root, _ = _run_ingest_and_silver(tmp_path, tiny_source_parquet)
    gold_root = tmp_path / "gold"

    runner = CliRunner()
    result = runner.invoke(
        gold,
        [
            "--batch-id",
            batch_id,
            "--silver-root",
            str(silver_root),
            "--gold-root",
            str(gold_root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "gold wrote" in result.output

    # All four parquet tables land at the documented path layout.
    cs_files = list(gold_root.glob("conversation_scores/batch_id=*/part-*.parquet"))
    lp_files = list(gold_root.glob("lead_profile/batch_id=*/part-*.parquet"))
    ap_files = list(gold_root.glob("agent_performance/batch_id=*/part-*.parquet"))
    ci_files = list(gold_root.glob("competitor_intel/batch_id=*/part-*.parquet"))
    insights_files = list(gold_root.glob("insights/batch_id=*/summary.json"))
    assert len(cs_files) == 1
    assert len(lp_files) == 1
    assert len(ap_files) == 1
    assert len(ci_files) == 1
    assert len(insights_files) == 1

    # Schemas match the canonical Gold definitions.
    assert pl.scan_parquet(cs_files[0]).collect().schema == GOLD_CONVERSATION_SCORES_SCHEMA
    assert pl.scan_parquet(lp_files[0]).collect().schema == GOLD_LEAD_PROFILE_SCHEMA
    assert pl.scan_parquet(ap_files[0]).collect().schema == GOLD_AGENT_PERFORMANCE_SCHEMA
    assert pl.scan_parquet(ci_files[0]).collect().schema == GOLD_COMPETITOR_INTEL_SCHEMA

    # Insights JSON parses and carries the batch_id stamp.
    payload = json.loads(insights_files[0].read_text())
    assert payload["batch_id"] == batch_id

    # Manifest run row is COMPLETED with rows_in / rows_out / output_path.
    with ManifestDB(state_db) as manifest:
        run = manifest.get_latest_run(batch_id=batch_id, layer="gold")
    assert run is not None
    assert run.status == "COMPLETED"
    assert run.rows_in is not None and run.rows_in > 0
    assert run.rows_out is not None and run.rows_out > 0
    assert run.output_path is not None
    assert run.output_path == str(gold_root.resolve())


# --------------------------------------------------------------- idempotency


def test_cli_gold_is_idempotent(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_env(tmp_path, monkeypatch)
    batch_id, silver_root, _ = _run_ingest_and_silver(tmp_path, tiny_source_parquet)
    gold_root = tmp_path / "gold"
    runner = CliRunner()

    args = [
        "--batch-id",
        batch_id,
        "--silver-root",
        str(silver_root),
        "--gold-root",
        str(gold_root),
    ]
    first = runner.invoke(gold, args)
    second = runner.invoke(gold, args)
    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert "already computed" in second.output


# --------------------------------------------------------------- error paths


def test_cli_gold_rejects_missing_silver_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gold must refuse to run if the Silver run is not COMPLETED in the manifest."""
    _configure_env(tmp_path, monkeypatch)
    silver_root = tmp_path / "silver"
    silver_root.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        gold,
        [
            "--batch-id",
            "b-no-such-batch",
            "--silver-root",
            str(silver_root),
            "--gold-root",
            str(tmp_path / "gold"),
        ],
    )
    assert result.exit_code != 0
    assert "silver" in result.output.lower()


def test_cli_gold_rejects_path_shaped_batch_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_env(tmp_path, monkeypatch)
    runner = CliRunner()
    result = runner.invoke(
        gold,
        [
            "--batch-id",
            "../escape",
            "--silver-root",
            str(tmp_path / "silver"),
            "--gold-root",
            str(tmp_path / "gold"),
        ],
    )
    assert result.exit_code != 0
    assert "invalid --batch-id" in result.output


def test_cli_gold_rejects_path_traversal_in_gold_root(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_env(tmp_path, monkeypatch)
    batch_id, silver_root, _ = _run_ingest_and_silver(tmp_path, tiny_source_parquet)
    runner = CliRunner()
    result = runner.invoke(
        gold,
        [
            "--batch-id",
            batch_id,
            "--silver-root",
            str(silver_root),
            "--gold-root",
            "../../etc/gold",
        ],
    )
    assert result.exit_code != 0
    assert "'..' segments are not allowed in --gold-root" in result.output


def test_cli_gold_rejects_symlink_escape_in_gold_root(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_safe_resolve`` must reject any --gold-root whose resolution
    follows a symlink. Otherwise an attacker who plants a symlink could
    redirect Gold writes outside the declared root.
    """
    _configure_env(tmp_path, monkeypatch)
    batch_id, silver_root, _ = _run_ingest_and_silver(tmp_path, tiny_source_parquet)
    real_target = tmp_path / "real_target"
    real_target.mkdir()
    symlink_root = tmp_path / "gold_symlink"
    symlink_root.symlink_to(real_target, target_is_directory=True)
    runner = CliRunner()
    result = runner.invoke(
        gold,
        [
            "--batch-id",
            batch_id,
            "--silver-root",
            str(silver_root),
            "--gold-root",
            str(symlink_root),
        ],
    )
    assert result.exit_code != 0
    assert "symlink resolution detected in --gold-root" in result.output


# --------------------------------------------------------------- populated persona


def test_cli_gold_populated_persona(
    tiny_source_parquet: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Override the persona stub so at least one lead carries a non-null persona."""
    _configure_env(tmp_path, monkeypatch)
    batch_id, silver_root, _ = _run_ingest_and_silver(tmp_path, tiny_source_parquet)
    gold_root = tmp_path / "gold"

    def _populated_classifier(
        aggregates: list[Any], _batch_latest: datetime
    ) -> dict[str, PersonaResult]:
        # Tag every lead as ``preco`` with high confidence — a single
        # value keeps the assertion simple while still proving the LLM
        # branch wired through.
        return {
            agg.lead_id: PersonaResult(
                persona="pesquisador_de_preco", persona_confidence=0.9, persona_source="llm"
            )
            for agg in aggregates
        }

    monkeypatch.setattr(
        "pipeline.cli.gold._default_persona_classifier",
        lambda _settings: _populated_classifier,
    )

    runner = CliRunner()
    result = runner.invoke(
        gold,
        [
            "--batch-id",
            batch_id,
            "--silver-root",
            str(silver_root),
            "--gold-root",
            str(gold_root),
        ],
    )
    assert result.exit_code == 0, result.output

    lp_files = list(gold_root.glob("lead_profile/batch_id=*/part-*.parquet"))
    assert len(lp_files) == 1
    lp = pl.scan_parquet(lp_files[0]).collect()
    assert lp.height > 0
    # Every lead got the populated persona.
    assert set(lp["persona"].drop_nulls().to_list()) == {"pesquisador_de_preco"}


# --------------------------------------------------------------- main wiring + help


def test_main_help_lists_gold() -> None:
    """``python -m pipeline --help`` lists the gold subcommand."""
    runner = CliRunner()
    result = runner.invoke(pipeline_cli, ["--help"])
    assert result.exit_code == 0, result.output
    assert "gold" in result.output


def test_gold_help_renders() -> None:
    """``python -m pipeline gold --help`` renders without traceback."""
    runner = CliRunner()
    result = runner.invoke(pipeline_cli, ["gold", "--help"])
    assert result.exit_code == 0, result.output
    assert "--batch-id" in result.output
    assert "--silver-root" in result.output
    assert "--gold-root" in result.output


_ = datetime(2026, 4, 24, tzinfo=UTC)  # marker to keep `datetime` import live
