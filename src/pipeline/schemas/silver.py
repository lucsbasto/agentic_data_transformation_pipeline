"""Silver layer schema.

The Silver layer is the *first analytical* layer. It takes the typed but
raw-preserved Bronze parquet and turns it into something a human analyst
(or a Gold transform) can trust: deduplicated on ``(conversation_id,
message_id)``, timestamps tagged as UTC, PII positionally masked, and a
stable ``lead_id`` per phone number so multiple conversations of the same
person can be linked.

What changes between Bronze and Silver
--------------------------------------

- ``timestamp``  naive ``Datetime[µs]`` in Bronze  UTC-aware
  ``Datetime[µs, UTC]`` in Silver. The source file has no timezone
  information, so "UTC" here is a *documented assumption*, not a
  conversion from another zone.
- ``sender_phone``  replaced with ``sender_phone_masked`` (positional
  masking, keeps last 2 digits). The raw column does not survive to
  Silver.
- ``sender_name``  kept as ``sender_name_normalized`` (trimmed, case-
  folded, accent-stripped, reconciled per phone: same phone always maps
  to its most-frequent normalized name).
- ``message_body``  replaced by ``message_body_masked`` (email/CPF/
  phone/CEP/plate masked in place with positional rules).
- ``metadata`` JSON string  typed :class:`polars.Struct`.
- New columns: ``lead_id`` (HMAC-derived), ``has_content`` (boolean),
  ``silver_batch_id`` (1:1 with Bronze batch), ``transformed_at`` (UTC
  wall clock of the Silver run).

Drift guard
-----------

Closed-set columns (``direction``, ``status``, ``message_type``) stay as
:class:`polars.Enum` so a new value in the source will *fail loudly* at
Silver write time instead of leaking into analytics. The enum values
mirror Bronze exactly — this module imports them rather than redeclaring
to avoid drift between the two schemas.
"""

from __future__ import annotations

# LEARN: ``typing.Final`` is a "please don't reassign this" marker.
# Python doesn't enforce it at runtime; mypy and ruff do. It signals to
# future readers that the export is read-only and safe to cache.
from typing import Final

# LEARN: ``polars as pl`` is the community convention. All data-plane
# types (``pl.String``, ``pl.Enum``, ``pl.Struct``, ``pl.Schema``) live
# on this module.
import polars as pl

# LEARN: we import Bronze's closed-set tuples directly instead of
# redeclaring them here. If Bronze ever widens an enum, Silver picks it
# up automatically — one source of truth, no silent divergence.
from pipeline.schemas.bronze import (
    DIRECTION_VALUES,
    MESSAGE_TYPE_VALUES,
    STATUS_VALUES,
)

# ---------------------------------------------------------------------------
# Metadata struct — parsed shape of the JSON blob carried from Bronze.
# ---------------------------------------------------------------------------
# LEARN: a ``pl.Struct`` is Polars' equivalent of "a nested record". It
# lets one column hold an object with named, typed fields, so we get
# ``df["metadata"].struct.field("city")`` without a second join.
#
# Why declare the fields explicitly instead of letting Polars infer from
# ``json_decode``? Because inference is shape-dependent: if a single row
# in a parquet happens to have ``response_time_sec`` as a string, the
# inferred struct would be ``pl.String`` for the whole column and every
# downstream aggregation would break. A fixed schema lets Polars raise
# on the *offending row* instead of silently widening the column type.
METADATA_FIELDS: Final[dict[str, pl.DataType]] = {
    "device": pl.String(),
    "city": pl.String(),
    "state": pl.String(),
    # LEARN: ``pl.Int32`` (32-bit signed int) is plenty for a response-
    # time-in-seconds. ``Int64`` would double the bytes for no gain; a
    # single conversation message won't take >2 billion seconds.
    "response_time_sec": pl.Int32(),
    "is_business_hours": pl.Boolean(),
    "lead_source": pl.String(),
}

METADATA_STRUCT: Final[pl.Struct] = pl.Struct(METADATA_FIELDS)
"""Typed shape of the ``metadata`` column in the Silver parquet."""


# ---------------------------------------------------------------------------
# Silver schema (column order + dtypes).
# ---------------------------------------------------------------------------
# LEARN: we build this as a plain ``dict`` first so the ordering is
# explicit and reviewable, then wrap it in ``pl.Schema`` at the end. The
# order matters: Polars writes parquet columns in schema order, and a
# stable order keeps diffs legible when someone inspects a Silver file.
_SILVER_FIELDS: dict[str, pl.DataType] = {
    # --- dedup + grain keys ---------------------------------------------------
    "message_id": pl.String(),
    "conversation_id": pl.String(),
    # --- time ------------------------------------------------------------------
    # LEARN: Bronze stores ``timestamp`` as a *naive* ``Datetime("us")``.
    # Silver tags the same instant as UTC. The "µs" (microsecond)
    # precision matches Polars' native default and is good enough for
    # sub-second ordering of messages.
    "timestamp": pl.Datetime("us", time_zone="UTC"),
    # --- identity (masked or derived) -----------------------------------------
    "sender_phone_masked": pl.String(),
    "sender_name_normalized": pl.String(),
    # LEARN: ``lead_id`` is a truncated HMAC-SHA256 hex string (see
    # ``silver/lead.py``). HMAC (not plain SHA-256) is used so the
    # mapping phone -> lead_id is non-reversible without the secret —
    # even if someone brute-forces SHA-256 over 11-digit phone numbers.
    "lead_id": pl.String(),
    # --- content --------------------------------------------------------------
    "message_body_masked": pl.String(),
    "has_content": pl.Boolean(),
    # --- extracted analytical dimensions (F2, regex lane) ---------------------
    # LEARN: these columns are *non-identifying* on purpose — they are
    # the analytical shadow of the PII maskers. ``email_domain`` keeps
    # the provider bucket (``"gmail.com"``) but drops the local part;
    # ``cep_prefix`` keeps the region (first 5 digits) but drops the
    # street-level suffix; ``plate_format`` keeps the Mercosul-vs-old
    # distinction but no characters from the plate itself. The two
    # boolean flags (``has_cpf`` / ``has_phone_mention``) signal
    # presence without carrying any dimension — a CPF value has no
    # analytical shape worth keeping.
    "email_domain": pl.String(),
    "has_cpf": pl.Boolean(),
    "cep_prefix": pl.String(),
    "has_phone_mention": pl.Boolean(),
    "plate_format": pl.String(),
    # --- passthrough (Bronze -> Silver unchanged) -----------------------------
    "campaign_id": pl.String(),
    "agent_id": pl.String(),
    # LEARN: closed-set enums are re-used from Bronze. If Bronze's
    # ``ingest.transform`` could cast a value into ``direction``, Silver
    # can keep that Enum guarantee for free.
    "direction": pl.Enum(list(DIRECTION_VALUES)),
    "message_type": pl.Enum(list(MESSAGE_TYPE_VALUES)),
    "status": pl.Enum(list(STATUS_VALUES)),
    "channel": pl.String(),
    "conversation_outcome": pl.String(),
    # --- parsed nested record -------------------------------------------------
    "metadata": METADATA_STRUCT,
    # --- lineage --------------------------------------------------------------
    # LEARN: ``ingested_at`` flows through from Bronze unchanged; it
    # tells a reader WHEN Bronze landed this row. ``transformed_at`` is
    # NEW in Silver: it says WHEN Silver ran. Having both lets us spot
    # batches where Silver lags far behind Bronze.
    "ingested_at": pl.Datetime("us", time_zone="UTC"),
    "silver_batch_id": pl.String(),
    "source_file_hash": pl.String(),
    "transformed_at": pl.Datetime("us", time_zone="UTC"),
}

SILVER_SCHEMA: Final[pl.Schema] = pl.Schema(_SILVER_FIELDS)
"""Authoritative ``pl.Schema`` for Silver parquet files."""


# ---------------------------------------------------------------------------
# Column groups — convenience tuples used by transforms and tests.
# ---------------------------------------------------------------------------
SILVER_COLUMNS: Final[tuple[str, ...]] = tuple(_SILVER_FIELDS.keys())
"""All Silver columns, in write order. Useful for round-trip asserts."""

SILVER_DEDUP_KEY: Final[tuple[str, ...]] = ("conversation_id", "message_id")
"""The ``(conversation_id, message_id)`` pair that defines a unique row."""


# ---------------------------------------------------------------------------
# Status priority for dedup tie-breaks.
# ---------------------------------------------------------------------------
# LEARN: when a message has both a ``sent`` row and a ``delivered`` row
# (and sometimes a ``read`` row), we keep the highest-priority one.
# ``read`` means the lead actually opened the message — it is the *most
# recent* event in the lifecycle we care about, so it wins. Any value
# outside this mapping falls back to 0 in the dedup expression.
STATUS_PRIORITY: Final[dict[str, int]] = {
    "read": 3,
    "delivered": 2,
    "sent": 1,
}
"""Integer priority used by ``silver.dedup`` when collapsing duplicates."""
