"""Coverage for the F4 agent planner (F4.10)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from pipeline.agent.planner import LAYER_ORDER, is_layer_completed, plan
from pipeline.agent.types import Layer
from pipeline.state.manifest import ManifestDB

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def db() -> Iterator[ManifestDB]:
    manifest = ManifestDB(":memory:").open()
    try:
        yield manifest
    finally:
        manifest.close()


def _seed_batch(db: ManifestDB, batch_id: str) -> None:
    """``runs`` rows have a FK on ``batches`` — every test that
    inserts a run needs a parent batch row first."""
    db.insert_batch(
        batch_id=batch_id,
        source_path="src",
        source_hash="h",
        source_mtime=0,
        started_at="2026-04-25T12:00:00+00:00",
    )


def _seed_run(
    db: ManifestDB,
    *,
    batch_id: str,
    layer: str,
    status: str,
    started_at: str = "2026-04-25T12:01:00+00:00",
) -> None:
    db.insert_run(
        run_id=f"{batch_id}-{layer}-{status}",
        batch_id=batch_id,
        layer=layer,
        started_at=started_at,
    )
    if status != "IN_PROGRESS":
        if status == "COMPLETED":
            db.mark_run_completed(
                run_id=f"{batch_id}-{layer}-{status}",
                rows_in=10,
                rows_out=10,
                output_path="/tmp/x",
                finished_at="2026-04-25T12:02:00+00:00",
                duration_ms=100,
            )
        else:  # FAILED
            db.mark_run_failed(
                run_id=f"{batch_id}-{layer}-{status}",
                finished_at="2026-04-25T12:02:00+00:00",
                duration_ms=100,
                error_type="X",
                error_message="boom",
            )


def _runner_factory() -> tuple[dict[Layer, callable], list[Layer]]:
    """Build a `{Layer: runner}` mapping where each runner records
    the layer it was called with — handy for asserting the executor
    would invoke the right callables in the right order."""
    invocations: list[Layer] = []

    def make(layer: Layer) -> callable:
        def runner() -> None:
            invocations.append(layer)
        return runner

    runners = {layer: make(layer) for layer in LAYER_ORDER}
    return runners, invocations


# ---------------------------------------------------------------------------
# is_layer_completed.
# ---------------------------------------------------------------------------


def test_is_layer_completed_false_when_no_runs(db: ManifestDB) -> None:
    _seed_batch(db, "bid01")
    assert is_layer_completed(db, batch_id="bid01", layer=Layer.SILVER) is False


def test_is_layer_completed_true_when_latest_run_completed(db: ManifestDB) -> None:
    _seed_batch(db, "bid01")
    _seed_run(db, batch_id="bid01", layer="silver", status="COMPLETED")
    assert is_layer_completed(db, batch_id="bid01", layer=Layer.SILVER) is True


def test_is_layer_completed_false_when_latest_run_failed(db: ManifestDB) -> None:
    _seed_batch(db, "bid01")
    _seed_run(db, batch_id="bid01", layer="silver", status="FAILED")
    assert is_layer_completed(db, batch_id="bid01", layer=Layer.SILVER) is False


def test_is_layer_completed_isolates_by_layer(db: ManifestDB) -> None:
    """A completed Bronze run does NOT make Silver look completed."""
    _seed_batch(db, "bid01")
    _seed_run(db, batch_id="bid01", layer="bronze", status="COMPLETED")
    assert is_layer_completed(db, batch_id="bid01", layer=Layer.BRONZE) is True
    assert is_layer_completed(db, batch_id="bid01", layer=Layer.SILVER) is False
    assert is_layer_completed(db, batch_id="bid01", layer=Layer.GOLD) is False


# ---------------------------------------------------------------------------
# plan.
# ---------------------------------------------------------------------------


def test_plan_returns_all_layers_for_fresh_batch(db: ManifestDB) -> None:
    _seed_batch(db, "bid01")
    runners, _ = _runner_factory()
    steps = plan("bid01", manifest=db, runners=runners)
    assert [layer for layer, _ in steps] == list(LAYER_ORDER)


def test_plan_returns_canonical_bronze_silver_gold_order(db: ManifestDB) -> None:
    _seed_batch(db, "bid01")
    runners, _ = _runner_factory()
    # Pass runners in non-canonical order to prove the planner sorts.
    shuffled = {
        Layer.GOLD: runners[Layer.GOLD],
        Layer.BRONZE: runners[Layer.BRONZE],
        Layer.SILVER: runners[Layer.SILVER],
    }
    steps = plan("bid01", manifest=db, runners=shuffled)
    assert [layer for layer, _ in steps] == [Layer.BRONZE, Layer.SILVER, Layer.GOLD]


def test_plan_skips_completed_layers(db: ManifestDB) -> None:
    _seed_batch(db, "bid01")
    _seed_run(db, batch_id="bid01", layer="bronze", status="COMPLETED")
    _seed_run(db, batch_id="bid01", layer="silver", status="COMPLETED")
    runners, _ = _runner_factory()
    steps = plan("bid01", manifest=db, runners=runners)
    assert [layer for layer, _ in steps] == [Layer.GOLD]


def test_plan_returns_empty_when_all_layers_completed(db: ManifestDB) -> None:
    _seed_batch(db, "bid01")
    for layer in ("bronze", "silver", "gold"):
        _seed_run(db, batch_id="bid01", layer=layer, status="COMPLETED")
    runners, _ = _runner_factory()
    assert plan("bid01", manifest=db, runners=runners) == []


def test_plan_re_includes_failed_layers(db: ManifestDB) -> None:
    """Bronze done, Silver failed -> retry Silver and resume Gold."""
    _seed_batch(db, "bid01")
    _seed_run(db, batch_id="bid01", layer="bronze", status="COMPLETED")
    _seed_run(db, batch_id="bid01", layer="silver", status="FAILED")
    runners, _ = _runner_factory()
    steps = plan("bid01", manifest=db, runners=runners)
    assert [layer for layer, _ in steps] == [Layer.SILVER, Layer.GOLD]


def test_plan_callables_are_the_supplied_runners(db: ManifestDB) -> None:
    """The planner must not wrap or substitute the runners — the
    executor relies on identity to attach retry budget per layer."""
    _seed_batch(db, "bid01")
    runners, invocations = _runner_factory()
    steps = plan("bid01", manifest=db, runners=runners)
    for layer, runner in steps:
        assert runner is runners[layer]
        runner()
    assert invocations == [Layer.BRONZE, Layer.SILVER, Layer.GOLD]


def test_plan_silently_skips_layers_without_a_runner(db: ManifestDB) -> None:
    """If the caller deliberately omits a layer (e.g. demo without
    Gold), the planner returns only what was supplied — never a
    KeyError."""
    _seed_batch(db, "bid01")
    runners, _ = _runner_factory()
    partial = {Layer.BRONZE: runners[Layer.BRONZE], Layer.SILVER: runners[Layer.SILVER]}
    steps = plan("bid01", manifest=db, runners=partial)
    assert [layer for layer, _ in steps] == [Layer.BRONZE, Layer.SILVER]


def test_layer_order_constant_pins_canonical_sequence() -> None:
    """Reordering the medallion sequence is a breaking design change
    — pin it here so a future refactor surfaces it loudly."""
    assert LAYER_ORDER == (Layer.BRONZE, Layer.SILVER, Layer.GOLD)
