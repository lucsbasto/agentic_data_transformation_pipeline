"""Tests for ``pipeline.silver.dedup``."""

from __future__ import annotations

from datetime import datetime

import polars as pl

from pipeline.schemas.bronze import STATUS_VALUES
from pipeline.silver.dedup import dedup_events, status_priority_expr


def _events_frame(rows: list[dict[str, object]]) -> pl.LazyFrame:
    """Build a minimal LazyFrame with just the columns dedup touches.

    Keeping the fixture narrow makes the test intent obvious — nothing
    else can influence the dedup outcome. Using ``pl.Enum(STATUS_VALUES)``
    mirrors the real Bronze schema so any cast-time surprise surfaces
    here too.
    """
    return pl.DataFrame(
        rows,
        schema={
            "conversation_id": pl.String(),
            "message_id": pl.String(),
            "status": pl.Enum(list(STATUS_VALUES)),
            "timestamp": pl.Datetime("us"),
            "batch_id": pl.String(),
        },
    ).lazy()


# ---------------------------------------------------------------------------
# status_priority_expr
# ---------------------------------------------------------------------------


def test_status_priority_mapping() -> None:
    df = pl.DataFrame(
        {"status": ["read", "delivered", "sent"]},
        schema={"status": pl.Enum(list(STATUS_VALUES))},
    )
    out = df.select(status_priority_expr(pl.col("status")).alias("p"))
    assert out["p"].to_list() == [3, 2, 1]
    assert out.schema["p"] == pl.Int32


def test_status_priority_null_defaults_to_zero() -> None:
    df = pl.DataFrame(
        {"status": [None]},
        schema={"status": pl.Enum(list(STATUS_VALUES))},
    )
    out = df.select(status_priority_expr(pl.col("status")).alias("p"))
    assert out["p"].to_list() == [0]


# ---------------------------------------------------------------------------
# dedup_events
# ---------------------------------------------------------------------------


def test_dedup_keeps_highest_priority_status() -> None:
    """A (sent, delivered, read) trio for the same message collapses to
    the ``read`` row, regardless of insertion order or timestamp.
    """
    lf = _events_frame(
        [
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "sent",
                "timestamp": datetime(2026, 4, 23, 12, 0, 5),
                "batch_id": "b1",
            },
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "read",
                "timestamp": datetime(2026, 4, 23, 12, 0, 1),
                "batch_id": "b1",
            },
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "delivered",
                "timestamp": datetime(2026, 4, 23, 12, 0, 3),
                "batch_id": "b1",
            },
        ]
    )
    out = dedup_events(lf).collect()
    assert out.height == 1
    assert out["status"][0] == "read"


def test_dedup_timestamp_breaks_priority_tie() -> None:
    """Two rows of the same status collapse to the newer one."""
    lf = _events_frame(
        [
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "delivered",
                "timestamp": datetime(2026, 4, 23, 12, 0, 1),
                "batch_id": "b1",
            },
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "delivered",
                "timestamp": datetime(2026, 4, 23, 12, 0, 9),
                "batch_id": "b1",
            },
        ]
    )
    out = dedup_events(lf).collect()
    assert out.height == 1
    assert out["timestamp"][0] == datetime(2026, 4, 23, 12, 0, 9)


def test_dedup_batch_id_breaks_full_tie_deterministically() -> None:
    """Same priority and same timestamp: the lexicographically larger
    ``batch_id`` wins. This is the idempotence guarantee — a second run
    over the same Bronze snapshot picks the same row.
    """
    ts = datetime(2026, 4, 23, 12, 0, 0)
    lf = _events_frame(
        [
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "sent",
                "timestamp": ts,
                "batch_id": "b-aaa",
            },
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "sent",
                "timestamp": ts,
                "batch_id": "b-zzz",
            },
        ]
    )
    out = dedup_events(lf).collect()
    assert out.height == 1
    assert out["batch_id"][0] == "b-zzz"


def test_dedup_preserves_distinct_messages() -> None:
    """Different ``message_id`` or ``conversation_id`` must not collapse."""
    lf = _events_frame(
        [
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "sent",
                "timestamp": datetime(2026, 4, 23, 12, 0, 0),
                "batch_id": "b1",
            },
            {
                "conversation_id": "c1",
                "message_id": "m2",
                "status": "sent",
                "timestamp": datetime(2026, 4, 23, 12, 0, 0),
                "batch_id": "b1",
            },
            {
                "conversation_id": "c2",
                "message_id": "m1",
                "status": "sent",
                "timestamp": datetime(2026, 4, 23, 12, 0, 0),
                "batch_id": "b1",
            },
        ]
    )
    out = dedup_events(lf).collect()
    assert out.height == 3


def test_dedup_drops_scratch_column() -> None:
    """The private ``_status_priority`` helper column must not leak into
    the output schema.
    """
    lf = _events_frame(
        [
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "read",
                "timestamp": datetime(2026, 4, 23, 12, 0, 0),
                "batch_id": "b1",
            },
        ]
    )
    out = dedup_events(lf).collect()
    assert "_status_priority" not in out.columns
    assert set(out.columns) == {
        "conversation_id",
        "message_id",
        "status",
        "timestamp",
        "batch_id",
    }


def test_dedup_is_idempotent() -> None:
    """Running ``dedup_events`` twice is the same as running it once."""
    lf = _events_frame(
        [
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "sent",
                "timestamp": datetime(2026, 4, 23, 12, 0, 0),
                "batch_id": "b1",
            },
            {
                "conversation_id": "c1",
                "message_id": "m1",
                "status": "read",
                "timestamp": datetime(2026, 4, 23, 12, 0, 1),
                "batch_id": "b1",
            },
        ]
    )
    once = dedup_events(lf).collect()
    twice = dedup_events(dedup_events(lf)).collect()
    assert once.equals(twice)


def test_dedup_keeps_null_keyed_rows_for_quarantine() -> None:
    """Rows with null keys are a contract violation; dedup leaves them
    for a separate validator to route to quarantine, rather than
    silently dropping them.
    """
    lf = _events_frame(
        [
            {
                "conversation_id": None,
                "message_id": "m1",
                "status": "sent",
                "timestamp": datetime(2026, 4, 23, 12, 0, 0),
                "batch_id": "b1",
            },
            {
                "conversation_id": "c1",
                "message_id": None,
                "status": "sent",
                "timestamp": datetime(2026, 4, 23, 12, 0, 0),
                "batch_id": "b1",
            },
        ]
    )
    out = dedup_events(lf).collect()
    assert out.height == 2
