"""Normalization helpers for the Silver layer.

Three independent pure transforms are exposed here. Each one is
*composable* — it takes a :class:`polars.Expr` (or a plain string for
the pure-Python helpers) and returns a new expression, never touching
I/O. The Silver orchestrator (``silver/transform.py``) wires them into
the lazy plan.

What lives here
---------------

- :func:`parse_timestamp_utc` — tag a naive ``Datetime`` column as UTC.
  Does NOT convert between timezones; Bronze's ``timestamp`` has no
  timezone metadata in the source parquet, and the source dictionary
  documents the values as UTC.
- :func:`parse_metadata_expr` — decode a JSON-string column into the
  typed ``METADATA_STRUCT`` declared in ``schemas/silver.py``. Rows
  whose JSON does not parse land as ``null`` (counted in the log line
  ``silver.metadata.null_decode``).
- :func:`normalize_name` / :func:`normalize_name_expr` — trim, case-fold,
  NFKD-strip accents so ``"Ana Paula Ribeiro"`` / ``"ANA PAULA RIBEIRO"``
  / ``"Aná Paula"`` all collapse to the same canonical form. Per-phone
  reconciliation (picking the most frequent normalized name per lead)
  is a separate aggregation done in ``silver/transform.py``.
"""

from __future__ import annotations

# LEARN: ``json`` is Python's built-in JSON codec. We use
# ``json.loads`` (string -> Python dict) inside the row-level metadata
# helper; Polars' native ``json_decode`` raises on the first malformed
# row, which conflicts with the "null + logged count" contract from the
# Silver spec, so we go through a per-row Python callback instead.
import json

# LEARN: ``unicodedata`` exposes the Unicode database. We use two
# features here:
#   - ``normalize("NFKD", s)`` — decompose ``é`` into ``e`` + combining
#     acute accent. That separation lets us strip the accent while
#     keeping the base letter.
#   - ``combining(ch)`` — truthy for combining diacritical marks. A
#     ``"".join(...)`` of the non-combining characters gives us an
#     accent-free copy of the string.
import unicodedata
from typing import Any

import polars as pl

from pipeline.schemas.silver import METADATA_STRUCT

# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


def parse_timestamp_utc(expr: pl.Expr) -> pl.Expr:
    """Attach UTC as the declared timezone of ``expr``.

    Bronze emits ``Datetime('us')`` naive (no timezone). The source
    parquet has no timezone metadata and the data dictionary documents
    its values as UTC, so Silver tags them without converting.

    LEARN: there are two Polars methods that look identical but behave
    differently:
      - ``.dt.replace_time_zone("UTC")`` — TAGS the existing instant as
        UTC. No shift. Wall-clock time stays the same; interpretation
        changes from "unknown" to "UTC".
      - ``.dt.convert_time_zone("UTC")`` — CONVERTS from a previously
        declared zone to UTC. Shifts the wall clock by the offset.
    We use ``replace_time_zone`` here because the naive input is
    already UTC wall-clock by contract.
    """
    return expr.dt.replace_time_zone("UTC")


# ---------------------------------------------------------------------------
# Metadata JSON
# ---------------------------------------------------------------------------


def _parse_metadata_row(raw: str | None) -> dict[str, Any] | None:
    """Parse one metadata cell to a Python dict, or ``None`` on failure.

    Called per-row via :func:`polars.Expr.map_elements`. Returning
    ``None`` (not raising) keeps the Silver pipeline moving even if one
    row has a corrupted JSON blob — the orchestrator aggregates the
    null count and emits it as a log field.
    """
    if raw is None or not raw.strip():
        return None
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    # Extra type guard: ``json.loads("42")`` returns an int, not a dict.
    # We only accept objects here because METADATA_STRUCT expects named
    # fields, and mapping an int into a Struct would raise downstream.
    if not isinstance(parsed, dict):
        return None
    return parsed


def parse_metadata_expr(expr: pl.Expr) -> pl.Expr:
    """Decode a JSON-string column into :data:`METADATA_STRUCT`.

    Malformed JSON rows decode to null rather than failing the batch.
    """
    # LEARN: ``map_elements`` runs the Python callback ONCE per row.
    # Polars marshals the result into the declared ``return_dtype``, so
    # the callback can return a plain dict and Polars reshapes it into
    # the struct. ``skip_nulls=True`` skips rows that are already null
    # on the input, letting us not branch for that case inside the
    # callback.
    return expr.map_elements(
        _parse_metadata_row,
        return_dtype=METADATA_STRUCT,
        skip_nulls=True,
    )


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------


def normalize_name(raw: str | None) -> str | None:
    """Canonicalize a lead name so reconciliation by phone works.

    ``"Ana Paula Ribeiro"`` / ``"ANA PAULA RIBEIRO"`` / ``"Aná P."``
    all collapse onto a lowercase, accent-free form with outer
    whitespace stripped. Internal whitespace is preserved so short
    forms (``"ana p"``) remain distinguishable from full names
    (``"ana paula"``) — the transform layer decides which form wins for
    a given phone number.

    ``None`` or a string that is empty after trimming returns ``None``.
    """
    if raw is None:
        return None
    trimmed = raw.strip()
    if not trimmed:
        return None
    # NFKD decomposes "é" into ("e", combining acute). Stripping every
    # combining code point leaves the unaccented base letter.
    decomposed = unicodedata.normalize("NFKD", trimmed)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    # LEARN: ``.casefold()`` is a stronger lowercase. ``"ß".lower()`` is
    # ``"ß"`` but ``"ß".casefold()`` is ``"ss"`` — matters when we
    # compare names reconciled across systems that may spell the same
    # sound differently. Harmless on ASCII names.
    return stripped.casefold()


def normalize_name_expr(expr: pl.Expr) -> pl.Expr:
    """Polars wrapper around :func:`normalize_name`.

    LEARN: ``map_elements`` is the Python-per-row escape hatch. It is
    slower than native ``str`` methods but unavoidable here because
    Polars has no built-in "strip Unicode combining marks" expression.
    For the 153k-row fixture the cost is a few hundred milliseconds —
    well inside the F2-RNF-07 budget.
    """
    return expr.map_elements(
        normalize_name,
        return_dtype=pl.String,
        skip_nulls=True,
    )
