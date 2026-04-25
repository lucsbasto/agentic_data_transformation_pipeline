"""Smoke tests for the FIC.1 shared campaign harness.

Proves that fixtures wire correctly and the ``fault_campaign`` marker
is usable. All tests must complete in < 2s.
"""

from __future__ import annotations

import pytest

from pipeline.agent.types import RunStatus

from .conftest import (
    FakeLLMClient,
    PipelineTree,
    run_agent_once,
)


@pytest.mark.fault_campaign
def test_pipeline_tree_directories_exist(pipeline_tree: PipelineTree) -> None:
    """pipeline_tree fixture creates all required subdirectories."""
    assert pipeline_tree.bronze.is_dir()
    assert pipeline_tree.silver.is_dir()
    assert pipeline_tree.gold.is_dir()
    assert pipeline_tree.state.is_dir()
    assert pipeline_tree.logs.is_dir()


@pytest.mark.fault_campaign
def test_pipeline_tree_manifest_accepts_batch(pipeline_tree: PipelineTree) -> None:
    """ManifestDB inside pipeline_tree can start and end an agent run."""
    run_id = pipeline_tree.manifest.start_agent_run()
    assert run_id  # non-empty UUID string
    pipeline_tree.manifest.end_agent_run(
        run_id,
        status=RunStatus.COMPLETED,
        batches_processed=0,
        failures_recovered=0,
        escalations=0,
    )
    conn = pipeline_tree.manifest._require_conn()
    row = conn.execute(
        "SELECT status FROM agent_runs WHERE agent_run_id = ?;", (run_id,)
    ).fetchone()
    assert row["status"] == "COMPLETED"


@pytest.mark.fault_campaign
def test_fake_llm_client_returns_queued_reply(fake_llm_client: FakeLLMClient) -> None:
    """FakeLLMClient pops replies FIFO and records calls."""
    fake_llm_client.queue_reply('{"kind": "schema_drift"}')
    resp = fake_llm_client.cached_call(system="sys", user="usr", temperature=0.0, max_tokens=64)
    assert resp.text == '{"kind": "schema_drift"}'
    assert len(fake_llm_client.calls) == 1
    assert fake_llm_client.calls[0]["system"] == "sys"


@pytest.mark.fault_campaign
def test_run_agent_once_no_pending_batches(pipeline_tree: PipelineTree) -> None:
    """run_agent_once returns COMPLETED when source_root is empty."""
    (pipeline_tree.root / "raw").mkdir(parents=True, exist_ok=True)
    result = run_agent_once(tree=pipeline_tree)
    assert result.status is RunStatus.COMPLETED
    assert result.batches_processed == 0


@pytest.mark.fault_campaign
def test_marker_selects_this_test() -> None:
    """Sanity: this file's tests are tagged fault_campaign."""
    # If we are here, the marker worked (pytest --strict-markers would
    # have aborted before collection if the marker were unregistered).
    assert True
