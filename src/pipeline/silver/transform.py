"""Silver orchestrator — compose every per-column transform.

This module turns a Bronze :class:`polars.LazyFrame` into a Silver
:class:`polars.LazyFrame` that exactly matches
:data:`pipeline.schemas.silver.SILVER_SCHEMA`. Every step lives in its
own module and is tested in isolation; here we only wire them in the
right order.

Sequence
--------

1. **Dedup** (``silver/dedup.py``) — collapse duplicates to one row per
   ``(conversation_id, message_id)``. Must come first so every
   subsequent transform runs over the smallest possible dataset.
2. **Per-row transforms** (``silver/normalize.py``, ``silver/pii.py``,
   ``silver/lead.py``) — applied as a single ``select`` so Polars can
   fuse them into one physical pass over the frame.
3. **Lead-level reconciliation** (``silver/reconcile.py``) — runs AFTER
   ``lead_id`` is available because it groups by it.
4. **Column order** — final ``select`` over ``SILVER_SCHEMA.names()`` so
   the emitted frame always matches the declared schema column-for-
   column (Parquet is order-sensitive).

The function is **pure** — no I/O, no logging. The CLI layer
(``pipeline.cli.silver``) owns the Bronze read and the Silver write.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

from pipeline.errors import SchemaDriftError
from pipeline.schemas.silver import SILVER_COLUMNS, SILVER_SCHEMA
from pipeline.silver.audio import audio_confidence_expr
from pipeline.silver.dedup import dedup_events
from pipeline.silver.extract import (
    extract_cep_prefix_expr,
    extract_email_domain_expr,
    extract_has_cpf_expr,
    extract_has_phone_mention_expr,
    extract_plate_format_expr,
)
from pipeline.silver.lead import derive_lead_id_expr
from pipeline.silver.normalize import (
    normalize_name_expr,
    parse_metadata_expr,
    parse_timestamp_utc,
)
from pipeline.silver.pii import mask_all_pii, mask_phone_only
from pipeline.silver.reconcile import reconcile_name_by_lead

__all__ = ["assert_silver_schema", "silver_transform"]


# ---------------------------------------------------------------------------
# Private per-row wrappers for the PII maskers.
# ---------------------------------------------------------------------------


def _mask_phone_expr(expr: pl.Expr) -> pl.Expr:
    """Polars wrapper around :func:`mask_phone_only`.

    Returns ``null`` when the input is ``null`` so the Silver column
    preserves the contract-violation signal instead of substituting a
    fake masked placeholder.
    """
    return expr.map_elements(
        lambda raw: mask_phone_only(raw) if raw else None,
        return_dtype=pl.String,
        skip_nulls=True,
    )


def _mask_body_expr(expr: pl.Expr) -> pl.Expr:
    """Polars wrapper around :func:`mask_all_pii`, returning only the
    masked string (the counts dict is discarded — Silver logs it from
    the CLI layer after collecting the frame)."""

    def _mask(raw: str | None) -> str | None:
        if raw is None:
            return None
        masked, _counts = mask_all_pii(raw)
        return masked

    return expr.map_elements(_mask, return_dtype=pl.String, skip_nulls=True)


def _has_content_expr(body_expr: pl.Expr) -> pl.Expr:
    """Flag rows whose ``message_body`` carries any non-whitespace text.

    PRD §6.2 calls out stickers/images without captions as the main
    signal this column is meant to capture. We generalize to "any empty
    or null body" because the analytical consumer only cares whether
    the message has inspectable content — the message_type column
    already carries the media vs text distinction.
    """
    stripped_len = body_expr.str.strip_chars().str.len_chars()
    return body_expr.is_not_null() & (stripped_len > 0)


# ---------------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------------


def silver_transform(
    lf: pl.LazyFrame,
    *,
    secret: str,
    silver_batch_id: str,
    transformed_at: datetime,
) -> pl.LazyFrame:
    """Build the Silver LazyFrame from a Bronze LazyFrame.

    Parameters
    ----------
    lf
        A ``LazyFrame`` whose schema matches
        :data:`pipeline.schemas.bronze.BRONZE_SCHEMA` (i.e., Bronze
        parquet scanned via ``pl.scan_parquet``).
    secret
        The raw HMAC key for :func:`derive_lead_id`. Callers unwrap
        :class:`pydantic.SecretStr` via ``get_secret_value()`` at the
        composition boundary so tests can pass a plain string.
    silver_batch_id
        Value for the ``silver_batch_id`` column. Typically equal to
        Bronze's ``batch_id`` — Silver is 1:1 with the Bronze batch it
        consumes, so reusing the id keeps manifest joins trivial.
    transformed_at
        UTC-aware ``datetime`` for the ``transformed_at`` column. One
        value per run — ``datetime.now(tz=UTC)`` at the CLI layer.
    """
    # Step 1 — dedup on (conversation_id, message_id).
    deduped = dedup_events(lf)

    # Step 2 — per-row transforms, fused into a single select pass.
    # ``pl.lit(value).cast(dtype)`` materializes a constant column of
    # exactly the dtype the Silver schema declares; without the cast
    # Polars may infer a narrower type (e.g. ``Datetime("us")`` naive
    # for a ``datetime.now(tz=UTC)`` literal on older Polars builds).
    projected = deduped.select(
        [
            pl.col("message_id"),
            pl.col("conversation_id"),
            parse_timestamp_utc(pl.col("timestamp")).alias("timestamp"),
            _mask_phone_expr(pl.col("sender_phone")).alias("sender_phone_masked"),
            normalize_name_expr(pl.col("sender_name")).alias(
                "sender_name_normalized"
            ),
            derive_lead_id_expr(pl.col("sender_phone"), secret).alias("lead_id"),
            _mask_body_expr(pl.col("message_body")).alias("message_body_masked"),
            _has_content_expr(pl.col("message_body")).alias("has_content"),
            # LEARN: extract runs against the *raw* ``message_body``,
            # not the masked column. Both expressions read the same
            # source column; Polars fuses the passes so the regex
            # work only walks the text once per row regardless of how
            # many extractors we chain.
            extract_email_domain_expr(pl.col("message_body")).alias("email_domain"),
            extract_has_cpf_expr(pl.col("message_body")).alias("has_cpf"),
            extract_cep_prefix_expr(pl.col("message_body")).alias("cep_prefix"),
            extract_has_phone_mention_expr(pl.col("message_body")).alias(
                "has_phone_mention"
            ),
            extract_plate_format_expr(pl.col("message_body")).alias("plate_format"),
            audio_confidence_expr(
                pl.col("message_type"), pl.col("message_body")
            ).alias("audio_confidence"),
            # LEARN: six placeholder columns for the LLM extraction
            # lane. The CLI layer fills them by calling
            # :func:`pipeline.silver.llm_extract.apply_llm_extraction`
            # on the collected DataFrame — see the "Option A" decision
            # in ``docs/f2-llm-handoff.md``. Typed null literals keep
            # the SILVER_SCHEMA contract honored even when the LLM
            # lane is skipped, times out, or exhausts its budget.
            pl.lit(None, dtype=pl.String).alias("veiculo_marca"),
            pl.lit(None, dtype=pl.String).alias("veiculo_modelo"),
            pl.lit(None, dtype=pl.Int32).alias("veiculo_ano"),
            pl.lit(None, dtype=pl.String).alias("concorrente_mencionado"),
            pl.lit(None, dtype=pl.Float64).alias("valor_pago_atual_brl"),
            pl.lit(None, dtype=pl.Boolean).alias("sinistro_historico"),
            pl.col("campaign_id"),
            pl.col("agent_id"),
            pl.col("direction"),
            pl.col("message_type"),
            pl.col("status"),
            pl.col("channel"),
            pl.col("conversation_outcome"),
            parse_metadata_expr(pl.col("metadata")).alias("metadata"),
            pl.col("ingested_at"),
            pl.lit(silver_batch_id, dtype=pl.String).alias("silver_batch_id"),
            pl.col("source_file_hash"),
            pl.lit(transformed_at)
            .cast(pl.Datetime("us", time_zone="UTC"))
            .alias("transformed_at"),
        ]
    )

    # Step 3 — reconcile canonical name per lead (needs lead_id).
    reconciled = reconcile_name_by_lead(projected)

    # Step 4 — pin column order to the declared Silver schema. Parquet
    # is order-sensitive; any drift here would make downstream
    # consumers break on a schema comparison.
    return reconciled.select(list(SILVER_COLUMNS))


def assert_silver_schema(df: pl.DataFrame) -> None:
    """Raise :class:`SchemaDriftError` unless ``df.schema`` equals
    :data:`SILVER_SCHEMA` column-for-column.

    Belt-and-braces check run just before ``write_silver``. Keeping it
    as a named helper (mirroring ``ingest.transform.assert_bronze_schema``)
    makes failures surface with a consistent error class and message
    across the pipeline.
    """
    actual = df.schema
    if actual == SILVER_SCHEMA:
        return
    mismatches: list[str] = []
    for name, dtype in SILVER_SCHEMA.items():
        got = actual.get(name)
        if got is None:
            mismatches.append(f"missing column {name!r}")
        elif got != dtype:
            mismatches.append(f"{name!r}: expected {dtype}, got {got}")
    extras = [c for c in actual if c not in SILVER_SCHEMA]
    for extra in extras:
        mismatches.append(f"unexpected column {extra!r}")
    raise SchemaDriftError(
        "Silver schema mismatch: " + "; ".join(mismatches)
    )
