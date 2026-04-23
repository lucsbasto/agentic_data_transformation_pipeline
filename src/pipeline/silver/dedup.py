"""Dedup transform for the Silver layer.

Bronze preserves every WhatsApp event verbatim — the same logical
message shows up once per status transition (``sent`` → ``delivered`` →
``read``). Silver is the first layer that claims *one row per message*,
so we need a deterministic rule for collapsing duplicates.

Rule
----

For each ``(conversation_id, message_id)`` partition, keep the row with
the highest lifecycle priority (``read`` > ``delivered`` > ``sent`` — see
:data:`pipeline.schemas.silver.STATUS_PRIORITY`). Ties on priority are
broken by the largest ``timestamp``, then by the lexicographically
largest ``batch_id`` (so the same Bronze snapshot always produces the
same Silver row — idempotence, not "whichever row Polars happened to
scan first").

Why priority before timestamp
-----------------------------

The timestamps of ``sent`` / ``delivered`` / ``read`` for the *same*
message can be minutes apart or identical, depending on the source
system's clock resolution. Priority is an ordered contract measured
from the data (``read`` always happens after ``delivered`` in the
source), so it is the stronger signal. We keep ``timestamp`` and
``batch_id`` as tiebreakers so ties between two ``delivered`` rows
collapse deterministically.

Output
------

A :class:`polars.LazyFrame` with the same columns as the input and at
most one row per :data:`SILVER_DEDUP_KEY`. The helper is *pure*: no I/O,
no logging — the Silver orchestrator is responsible for reading Bronze,
applying :func:`dedup_events`, and recording ``rows_deduped`` on the
manifest run.
"""

from __future__ import annotations

import polars as pl

from pipeline.schemas.silver import SILVER_DEDUP_KEY, STATUS_PRIORITY

__all__ = ["dedup_events", "status_priority_expr"]


# ---------------------------------------------------------------------------
# Status -> priority (read=3, delivered=2, sent=1, unknown=0)
# ---------------------------------------------------------------------------


def status_priority_expr(expr: pl.Expr) -> pl.Expr:
    """Map a ``status`` expression to an integer priority.

    LEARN: :meth:`polars.Expr.replace_strict` takes a ``dict`` of
    ``value -> replacement`` and maps every element in one vectorized
    pass. ``default=0`` is what an unknown status (or null) collapses
    to, so the expression is total: no row is dropped by the mapping
    itself. Using ``replace_strict`` instead of a chain of
    ``when().then()`` keeps the plan readable and lets Polars push the
    mapping into its columnar engine.

    Why a plain ``Int32``? The priority is a tiny domain (0..3); the
    default ``Int64`` would waste four bytes per row across a 153k-row
    fixture for no gain. The dtype also documents intent: "this is a
    small rank, not a counter".
    """
    return expr.replace_strict(
        STATUS_PRIORITY,
        default=0,
        return_dtype=pl.Int32,
    )


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------


def dedup_events(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Collapse duplicate events to one row per ``(conversation_id, message_id)``.

    The tiebreak chain is priority desc → timestamp desc → batch_id
    desc. ``batch_id`` is the final deterministic tiebreak so two runs
    over the same Bronze snapshot always pick the same winning row
    (idempotence; see PRD §RF-01).

    The function does not drop rows with null keys — that is a contract
    violation that belongs in a separate validator, so it can route the
    bad rows to quarantine rather than silently lose them here.

    LEARN: ``LazyFrame`` is a *plan*, not a materialized table. Every
    method on it returns a new plan; Polars only executes when the
    caller hits ``.collect()`` (or ``.sink_parquet()``, etc.). That is
    why every helper in ``silver/`` takes and returns a ``LazyFrame``:
    composing plans stays cheap even when we chain six transforms.
    """
    # LEARN: ``with_columns`` adds or overwrites a named column on the
    # plan. We materialize the priority into a real column (instead of
    # passing it only as a sort key) because that keeps the plan
    # inspectable — if a dedup looks wrong, a reviewer can collect the
    # intermediate LazyFrame and see why a given row won.
    ranked = lf.with_columns(
        status_priority_expr(pl.col("status")).alias("_status_priority"),
    )

    # LEARN: ``sort`` on a LazyFrame applies a stable, multi-key sort.
    # ``descending=[True, True, True]`` says "largest first" for each
    # of the three columns. ``nulls_last=True`` pushes null priorities,
    # timestamps, or batch_ids to the bottom, so ``unique(keep="first")``
    # never prefers a null-filled row over a real one.
    sorted_lf = ranked.sort(
        by=["_status_priority", "timestamp", "batch_id"],
        descending=[True, True, True],
        nulls_last=True,
    )

    # LEARN: ``unique(subset=..., keep="first")`` drops later rows that
    # share the subset values. Because we sorted first, "first" is the
    # highest-priority row per key. ``maintain_order=True`` is not
    # needed for correctness (the dedup key determines identity), but
    # it keeps the result stable across Polars versions for tests that
    # compare row order.
    deduped = sorted_lf.unique(
        subset=list(SILVER_DEDUP_KEY),
        keep="first",
        maintain_order=True,
    )

    # Drop the scratch column so the output schema is a strict subset
    # of the input schema — callers composing this with other helpers
    # never see internal bookkeeping leak through.
    return deduped.drop("_status_priority")
