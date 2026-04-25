"""Property-style invariants for the F4 manifest counters (F4.19).

The F4 design names :func:`pipeline.state.manifest.ManifestDB.count_agent_attempts`
as the executor's hot lookup. Two invariants must hold no matter
how many failures land or in which order:

1. **Monotonic non-decreasing** — for any single
   ``(batch_id, layer, error_class)`` triple, the count grows by
   exactly one each time ``record_agent_failure`` is called for
   that triple. It never drops.
2. **Triple isolation** — recording a failure under one triple
   never increments the counter of another triple. The composite
   index in ``schemas.manifest.AGENT_FAILURES_INDEXES`` exists
   precisely to make this true at scale, but the semantic must
   hold even when no index is present.

`hypothesis` would be the natural fit but spec NFR-05 forbids new
runtime/test dependencies. Instead we drive a deterministic
permutation across a small Cartesian product of triples and pin
the same invariants.
"""

from __future__ import annotations

import itertools
import random
from collections.abc import Iterator

import pytest

from pipeline.state.manifest import ManifestDB

_BATCH_IDS = ("bid_a", "bid_b", "bid_c")
_LAYERS = ("bronze", "silver", "gold")
_ERROR_CLASSES = ("schema_drift", "regex_break", "partition_missing", "out_of_range")

# All 36 (3 batches x 3 layers x 4 classes) triples.
_TRIPLES: list[tuple[str, str, str]] = list(
    itertools.product(_BATCH_IDS, _LAYERS, _ERROR_CLASSES)
)


@pytest.fixture
def db() -> Iterator[ManifestDB]:
    manifest = ManifestDB(":memory:").open()
    try:
        yield manifest
    finally:
        manifest.close()


def _attempt_count(
    db: ManifestDB, *, batch_id: str, layer: str, error_class: str
) -> int:
    return db.count_agent_attempts(
        batch_id=batch_id, layer=layer, error_class=error_class
    )


# ---------------------------------------------------------------------------
# Invariant 1 — monotonic non-decreasing.
# ---------------------------------------------------------------------------


def test_count_grows_by_one_per_recorded_failure(db: ManifestDB) -> None:
    """Hammer one triple with N failures and assert the counter
    matches the call number after every step."""
    agent_run_id = db.start_agent_run()
    triple = ("bid01", "silver", "regex_break")
    for attempt_no in range(1, 51):
        db.record_agent_failure(
            agent_run_id=agent_run_id,
            batch_id=triple[0],
            layer=triple[1],
            error_class=triple[2],
            attempts=attempt_no,
            last_error_msg=f"attempt {attempt_no}",
        )
        assert (
            _attempt_count(db, batch_id=triple[0], layer=triple[1], error_class=triple[2])
            == attempt_no
        )


def test_count_is_non_decreasing_under_random_permutation(db: ManifestDB) -> None:
    """Drive 200 record_agent_failure calls picked deterministically
    (seeded RNG) across all 36 triples and assert no triple's
    counter ever decreases between observations."""
    rng = random.Random(2026)
    agent_run_id = db.start_agent_run()
    counts: dict[tuple[str, str, str], int] = dict.fromkeys(_TRIPLES, 0)
    for _ in range(200):
        triple = rng.choice(_TRIPLES)
        counts[triple] += 1
        db.record_agent_failure(
            agent_run_id=agent_run_id,
            batch_id=triple[0],
            layer=triple[1],
            error_class=triple[2],
            attempts=counts[triple],
            last_error_msg="x",
        )
        # Re-read every triple — none may have shrunk.
        for snap_triple, expected in counts.items():
            actual = _attempt_count(
                db,
                batch_id=snap_triple[0],
                layer=snap_triple[1],
                error_class=snap_triple[2],
            )
            assert actual == expected, (
                f"counter regressed for {snap_triple}: {actual} != {expected}"
            )


# ---------------------------------------------------------------------------
# Invariant 2 — triple isolation.
# ---------------------------------------------------------------------------


def test_recording_one_triple_does_not_increment_other_triples(db: ManifestDB) -> None:
    """Loop over all 36 triples; record one failure for each; after
    each insertion verify only the newly-touched triple grew."""
    agent_run_id = db.start_agent_run()
    expected: dict[tuple[str, str, str], int] = dict.fromkeys(_TRIPLES, 0)
    for triple in _TRIPLES:
        expected[triple] += 1
        db.record_agent_failure(
            agent_run_id=agent_run_id,
            batch_id=triple[0],
            layer=triple[1],
            error_class=triple[2],
            attempts=expected[triple],
            last_error_msg="x",
        )
        for other, target in expected.items():
            actual = _attempt_count(
                db,
                batch_id=other[0],
                layer=other[1],
                error_class=other[2],
            )
            assert actual == target


def test_count_is_zero_for_unseen_triples(db: ManifestDB) -> None:
    """A triple that no failure ever touched must report zero — not
    raise, not None, not negative — so the executor's first-attempt
    branch in F4.11 never special-cases the empty case."""
    db.start_agent_run()
    db.record_agent_failure(
        agent_run_id=db.start_agent_run(),
        batch_id="other_batch",
        layer="silver",
        error_class="regex_break",
        attempts=1,
        last_error_msg="x",
    )
    assert (
        _attempt_count(db, batch_id="missing", layer="silver", error_class="regex_break")
        == 0
    )
