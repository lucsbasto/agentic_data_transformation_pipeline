"""Lead-level reconciliation helpers for the Silver layer.

Per-row normalization already collapses ``"Ana Paula Ribeiro"`` /
``"ANA PAULA RIBEIRO"`` / ``"Aná Paula"`` into the canonical form
``"ana paula ribeiro"`` (see :mod:`pipeline.silver.normalize`). That
leaves a different problem: the **same lead** may have introduced
themselves as ``"ana paula ribeiro"`` in one conversation and
``"ana paula"`` in another. Silver's Gold-facing contract is *one
canonical name per lead*, so downstream joins on ``lead_id`` can show a
stable display string.

Rule
----

For each ``lead_id``, among the non-null normalized names on that lead's
rows, pick:

1. the name with the highest occurrence count;
2. tie → the longest name (more information — ``"ana paula ribeiro"``
   beats ``"ana paula"``);
3. tie → the lexicographically smallest name (pure determinism; no
   analytical meaning).

Rows whose lead has no non-null name at all keep ``null``. Rows whose
``lead_id`` itself is null are passed through untouched — they are
already a contract violation headed for quarantine, so rewriting their
name would only hide the signal.

Output
------

The returned :class:`polars.LazyFrame` has the same columns as the input.
The ``name_col`` is overwritten in place with the reconciled value, so
downstream transforms see a single column and do not need to know
whether reconciliation happened.
"""

from __future__ import annotations

import polars as pl

__all__ = ["reconcile_name_by_lead"]


# ---------------------------------------------------------------------------
# Canonical-name pick.
# ---------------------------------------------------------------------------


def _canonical_name_per_lead(
    lf: pl.LazyFrame,
    *,
    lead_col: str,
    name_col: str,
) -> pl.LazyFrame:
    """Build ``(lead_id, canonical_name)`` pairs for every lead with at
    least one non-null name.

    LEARN: the three-level sort + group_by pattern is the canonical
    Polars idiom for "pick one row per group according to an ordered
    rule". We sort the entire frame by the pick rule first, then
    ``group_by(..., maintain_order=True)`` + ``first()`` takes the
    winner without an extra aggregation pass.
    """
    # Keep only rows that can contribute to the vote.
    candidates = lf.filter(
        pl.col(lead_col).is_not_null() & pl.col(name_col).is_not_null()
    )

    # Count occurrences per (lead, name). ``pl.len()`` returns the row
    # count of the group and is cheaper than ``pl.col(name_col).count()``.
    counted = candidates.group_by([lead_col, name_col]).agg(
        pl.len().alias("_count")
    )

    # Sort rule: count desc, length desc, name asc.
    # - count desc: most-frequent wins;
    # - length desc: "ana paula ribeiro" beats "ana paula" on ties;
    # - name asc: pure determinism on total ties.
    # ``str.len_chars`` measures Unicode code points; on ASCII casefolded
    # names that is identical to byte length, but still the right choice
    # in case a non-ASCII name survives normalization in a future tweak.
    ranked = counted.sort(
        by=[
            "_count",
            pl.col(name_col).str.len_chars(),
            name_col,
        ],
        descending=[True, True, False],
    )

    # One row per lead — the top of each group. ``maintain_order=True``
    # preserves the sort we just paid for, which is what makes
    # ``first()`` deterministic across Polars versions.
    winners = ranked.group_by(lead_col, maintain_order=True).agg(
        pl.col(name_col).first().alias("_canonical_name"),
    )

    return winners.select([lead_col, "_canonical_name"])


# ---------------------------------------------------------------------------
# Public transform.
# ---------------------------------------------------------------------------


def reconcile_name_by_lead(
    lf: pl.LazyFrame,
    *,
    lead_col: str = "lead_id",
    name_col: str = "sender_name_normalized",
) -> pl.LazyFrame:
    """Overwrite ``name_col`` with the canonical name per ``lead_col``.

    Rows whose ``lead_col`` is null are untouched — their ``name_col``
    keeps whatever per-row normalization produced. Rows whose lead has
    *no* non-null names at all keep ``null`` (there is nothing to pick).

    LEARN: ``LazyFrame.join(..., how="left")`` pulls the right-side
    column in next to the left without collapsing rows; combined with
    ``coalesce`` we get "use reconciled value where available, else
    keep the original". This preserves total row count — critical for
    the Silver contract which guarantees one row per
    ``(conversation_id, message_id)``.
    """
    canonical = _canonical_name_per_lead(
        lf, lead_col=lead_col, name_col=name_col
    )

    joined = lf.join(canonical, on=lead_col, how="left")

    # Where the reconciled value exists, use it; otherwise fall back to
    # whatever the row already had (preserves null-lead rows and leads
    # that had no non-null names).
    reconciled = joined.with_columns(
        pl.coalesce(pl.col("_canonical_name"), pl.col(name_col)).alias(name_col),
    )

    return reconciled.drop("_canonical_name")
