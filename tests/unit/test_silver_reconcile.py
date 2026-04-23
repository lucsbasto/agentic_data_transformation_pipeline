"""Tests for ``pipeline.silver.reconcile``."""

from __future__ import annotations

import polars as pl

from pipeline.silver.reconcile import reconcile_name_by_lead


def _frame(rows: list[dict[str, object]]) -> pl.LazyFrame:
    return pl.DataFrame(
        rows,
        schema={
            "lead_id": pl.String(),
            "sender_name_normalized": pl.String(),
        },
    ).lazy()


def test_most_frequent_name_wins() -> None:
    lf = _frame(
        [
            {"lead_id": "L1", "sender_name_normalized": "ana paula"},
            {"lead_id": "L1", "sender_name_normalized": "ana paula"},
            {"lead_id": "L1", "sender_name_normalized": "ana p"},
        ]
    )
    out = reconcile_name_by_lead(lf).collect()
    assert out["sender_name_normalized"].to_list() == [
        "ana paula",
        "ana paula",
        "ana paula",
    ]


def test_count_tie_longer_name_wins() -> None:
    """Equal counts → the longer form carries more information."""
    lf = _frame(
        [
            {"lead_id": "L1", "sender_name_normalized": "ana paula ribeiro"},
            {"lead_id": "L1", "sender_name_normalized": "ana paula"},
        ]
    )
    out = reconcile_name_by_lead(lf).collect()
    assert set(out["sender_name_normalized"].to_list()) == {"ana paula ribeiro"}


def test_full_tie_breaks_lexicographically() -> None:
    """Equal counts, equal length → lex smallest wins, for pure
    determinism. No analytical meaning — it just pins the outcome so
    two pipeline runs over the same Bronze always produce the same id.
    """
    lf = _frame(
        [
            {"lead_id": "L1", "sender_name_normalized": "ana"},
            {"lead_id": "L1", "sender_name_normalized": "bob"},
        ]
    )
    out = reconcile_name_by_lead(lf).collect()
    assert set(out["sender_name_normalized"].to_list()) == {"ana"}


def test_multiple_leads_independent() -> None:
    lf = _frame(
        [
            {"lead_id": "L1", "sender_name_normalized": "ana paula"},
            {"lead_id": "L1", "sender_name_normalized": "ana p"},
            {"lead_id": "L2", "sender_name_normalized": "bruno"},
            {"lead_id": "L2", "sender_name_normalized": "bruno"},
            {"lead_id": "L2", "sender_name_normalized": "bru"},
        ]
    )
    out = (
        reconcile_name_by_lead(lf)
        .sort("lead_id")
        .collect()
    )
    by_lead = dict(zip(out["lead_id"], out["sender_name_normalized"], strict=True))
    # L1 is a count tie between two names — length breaks it, so the
    # longer "ana paula" wins for every L1 row.
    assert by_lead["L1"] == "ana paula"
    assert by_lead["L2"] == "bruno"


def test_null_name_rows_are_preserved() -> None:
    """A row with a null name keeps null only if its lead has no
    non-null name. If the lead DOES have a name, the null row gets
    reconciled too — otherwise we'd keep orphan nulls for no reason.
    """
    lf = _frame(
        [
            {"lead_id": "L1", "sender_name_normalized": "ana paula"},
            {"lead_id": "L1", "sender_name_normalized": None},
        ]
    )
    out = reconcile_name_by_lead(lf).sort("sender_name_normalized").collect()
    names = out["sender_name_normalized"].to_list()
    # Both rows ended up as "ana paula" — the null inherits the
    # canonical name from the same lead.
    assert sorted(n for n in names if n is not None) == ["ana paula", "ana paula"]


def test_lead_with_only_null_names_keeps_null() -> None:
    lf = _frame(
        [
            {"lead_id": "L1", "sender_name_normalized": None},
            {"lead_id": "L1", "sender_name_normalized": None},
        ]
    )
    out = reconcile_name_by_lead(lf).collect()
    assert out["sender_name_normalized"].to_list() == [None, None]


def test_null_lead_rows_untouched() -> None:
    """A null ``lead_id`` means the row is already broken and headed
    for quarantine. Do not rewrite its name — the original value is the
    signal the operator needs.
    """
    lf = _frame(
        [
            {"lead_id": None, "sender_name_normalized": "mystery name"},
            {"lead_id": "L1", "sender_name_normalized": "ana"},
            {"lead_id": "L1", "sender_name_normalized": "ana"},
        ]
    )
    out = reconcile_name_by_lead(lf).sort("lead_id", nulls_last=True).collect()
    # Null-lead row survives with its original name.
    null_rows = out.filter(pl.col("lead_id").is_null())
    assert null_rows["sender_name_normalized"].to_list() == ["mystery name"]


def test_row_count_preserved() -> None:
    """Reconciliation must never add or drop rows — Silver guarantees
    one row per ``(conversation_id, message_id)`` and join-based
    reconciliation can silently violate that if misused.
    """
    lf = _frame(
        [
            {"lead_id": "L1", "sender_name_normalized": "a"},
            {"lead_id": "L1", "sender_name_normalized": "a"},
            {"lead_id": "L2", "sender_name_normalized": "b"},
            {"lead_id": None, "sender_name_normalized": "c"},
        ]
    )
    before = lf.collect().height
    after = reconcile_name_by_lead(lf).collect().height
    assert before == after == 4


def test_is_idempotent() -> None:
    lf = _frame(
        [
            {"lead_id": "L1", "sender_name_normalized": "ana paula"},
            {"lead_id": "L1", "sender_name_normalized": "ana paula ribeiro"},
            {"lead_id": "L1", "sender_name_normalized": "ana paula"},
        ]
    )
    once = reconcile_name_by_lead(lf).collect()
    twice = reconcile_name_by_lead(reconcile_name_by_lead(lf)).collect()
    assert once.sort("sender_name_normalized").equals(
        twice.sort("sender_name_normalized")
    )


def test_scratch_column_does_not_leak() -> None:
    lf = _frame(
        [{"lead_id": "L1", "sender_name_normalized": "ana"}]
    )
    out = reconcile_name_by_lead(lf).collect()
    assert "_canonical_name" not in out.columns
    assert out.columns == ["lead_id", "sender_name_normalized"]
