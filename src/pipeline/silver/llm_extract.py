"""LLM entity extraction — F2 Silver lane (PRD §6.2, §18.1, §18.4).

This module turns each Silver row's ``message_body_masked`` into six
analytical columns (``veiculo_marca``, ``veiculo_modelo``,
``veiculo_ano``, ``concorrente_mencionado``, ``valor_pago_atual_brl``,
``sinistro_historico``) by asking the LLM to fill a fixed JSON schema.

Why it lives outside :mod:`pipeline.silver.transform`
-----------------------------------------------------

Every other Silver step is a pure Polars expression that Polars can
fuse into one physical pass over the frame. The LLM call is I/O-bound:
it needs a collaborator (:class:`LLMClient`), respects a per-batch
budget (PRD §18.4), and dedupes by body so the same text never round-
trips twice in one run. Folding that into a ``pl.Expr`` would tangle
the pure transform with an I/O-bound subject. Keeping it separate
lets :func:`pipeline.silver.transform.silver_transform` stay trivially
testable without an LLM fake.

Error-handling contract
-----------------------

- Invalid JSON / missing fields     → :meth:`ExtractedEntities.null`.
- :class:`LLMCallError` after retries → :meth:`ExtractedEntities.null`.
- Budget exhausted                  → remaining rows stay null, one
  ``llm.budget_exhausted`` log line per batch.

LLM unavailability is treated as a transient operational event, not a
row contract violation — the Silver run keeps going and the affected
rows get null entity columns, which analytics can filter on.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Final

import polars as pl

from pipeline.errors import LLMCallError
from pipeline.llm.client import LLMClient
from pipeline.logging import get_logger

# LEARN: ``PROMPT_VERSION`` is embedded in :data:`SYSTEM_PROMPT` below so
# the SHA-256 cache key (model + system + user + …) naturally busts when
# we bump the version — no separate ``LLMCache.invalidate`` dance. To
# force a re-run under the same prompt, operators can still call
# ``invalidate(prefix=...)`` with the model-scoped cache-key prefix.
PROMPT_VERSION: Final[str] = "v1"

SYSTEM_PROMPT: Final[str] = (
    "You extract structured data from Brazilian Portuguese "
    "insurance-sales chat messages.\n\n"
    f"PROMPT_VERSION={PROMPT_VERSION}\n\n"
    "Return ONLY a single JSON object (no prose, no markdown fences) "
    "with exactly these keys:\n\n"
    '- "veiculo_marca" (string | null): vehicle brand when mentioned '
    '(e.g. "Toyota", "Fiat").\n'
    '- "veiculo_modelo" (string | null): vehicle model when mentioned '
    '(e.g. "Corolla", "Argo").\n'
    '- "veiculo_ano" (integer | null): 4-digit model year when clearly '
    "stated.\n"
    '- "concorrente_mencionado" (string | null): name of a competing '
    "insurer if the lead mentions one.\n"
    '- "valor_pago_atual_brl" (number | null): BRL amount the lead '
    "currently pays for insurance, as a number "
    "(no currency symbol).\n"
    '- "sinistro_historico" (boolean | null): true if the lead '
    "explicitly states a prior accident or claim, false if they "
    "explicitly deny one, null otherwise.\n\n"
    "Use null for any key you cannot populate with high confidence. "
    "Never guess. Do not invent brands, models, years, or amounts "
    "that are not present in the message.\n"
)

# LEARN: listing the schema keys as a tuple (not a set) gives us a
# *stable* iteration order for the "missing any required key" check.
_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "veiculo_marca",
    "veiculo_modelo",
    "veiculo_ano",
    "concorrente_mencionado",
    "valor_pago_atual_brl",
    "sinistro_historico",
)


@dataclass(frozen=True, slots=True)
class ExtractedEntities:
    """Parsed result of one entity-extraction LLM call."""

    veiculo_marca: str | None
    veiculo_modelo: str | None
    veiculo_ano: int | None
    concorrente_mencionado: str | None
    valor_pago_atual_brl: float | None
    sinistro_historico: bool | None

    @classmethod
    def null(cls) -> ExtractedEntities:
        """Zero-information value used on every failure path."""
        return cls(None, None, None, None, None, None)


def extract_entities_from_body(
    body: str, *, client: LLMClient
) -> ExtractedEntities:
    """Single JSON-mode LLM call. Returns an all-null dataclass on any
    parse or provider failure; never raises."""
    logger = get_logger("pipeline.silver.llm")
    try:
        response = client.cached_call(system=SYSTEM_PROMPT, user=body)
    except LLMCallError as exc:
        logger.warning(
            "llm.extraction.failed",
            error=str(exc),
            body_preview=body[:200],
        )
        return ExtractedEntities.null()
    return _parse_response(response.text, body=body)


def apply_llm_extraction(
    df: pl.DataFrame,
    *,
    client: LLMClient,
    max_calls: int | None = None,
) -> pl.DataFrame:
    """Fill the six LLM-extracted columns on a collected Silver frame.

    Dedupes by ``message_body_masked`` so the same text is never called
    twice in one run. Respects ``max_calls``: once the cache-miss count
    hits the budget, remaining unique bodies get an all-null dataclass
    and one ``llm.budget_exhausted`` log line fires. ``max_calls=None``
    disables the cap.
    """
    logger = get_logger("pipeline.silver.llm")
    bodies: list[str | None] = df["message_body_masked"].to_list()

    # LEARN: dedup by body text, preserving first-seen order so log
    # output is deterministic. ``set`` alone would reorder.
    unique_bodies: list[str] = []
    seen: set[str] = set()
    for body in bodies:
        if body is None or not body.strip():
            continue
        if body in seen:
            continue
        seen.add(body)
        unique_bodies.append(body)

    lookup: dict[str, ExtractedEntities] = {}
    skipped_bodies: set[str] = set()
    calls_made = 0
    cache_hits = 0
    input_tokens_total = 0
    output_tokens_total = 0

    logger.info(
        "llm.extraction.start",
        total_rows=len(bodies),
        unique_bodies=len(unique_bodies),
        max_calls=max_calls,
    )

    for body in unique_bodies:
        # LEARN: budget is enforced against *cache misses*. Cache hits
        # are declared "zero cost" in PRD §18.4, so they never consume
        # the batch budget even when ``max_calls`` is tight.
        if max_calls is not None and calls_made >= max_calls:
            lookup[body] = ExtractedEntities.null()
            skipped_bodies.add(body)
            continue
        try:
            response = client.cached_call(system=SYSTEM_PROMPT, user=body)
        except LLMCallError as exc:
            logger.warning(
                "llm.extraction.failed",
                error=str(exc),
                body_preview=body[:200],
            )
            lookup[body] = ExtractedEntities.null()
            # A failed round-trip still burned a call from the budget.
            calls_made += 1
            continue
        lookup[body] = _parse_response(response.text, body=body)
        input_tokens_total += response.input_tokens
        output_tokens_total += response.output_tokens
        if response.cache_hit:
            cache_hits += 1
        else:
            calls_made += 1

    if skipped_bodies:
        rows_skipped = sum(1 for b in bodies if b in skipped_bodies)
        logger.warning(
            "llm.budget_exhausted",
            max_calls=max_calls,
            bodies_skipped=len(skipped_bodies),
            rows_skipped=rows_skipped,
        )

    per_row: list[ExtractedEntities] = [
        lookup[body] if body is not None and body in lookup
        else ExtractedEntities.null()
        for body in bodies
    ]

    logger.info(
        "llm.extraction.complete",
        calls_made=calls_made,
        cache_hits=cache_hits,
        input_tokens_total=input_tokens_total,
        output_tokens_total=output_tokens_total,
    )

    # LEARN: ``with_columns`` replaces columns in place when they
    # already exist (they do: Silver transform seeded six null
    # placeholders). That preserves the Silver column order declared
    # in :data:`SILVER_SCHEMA` so the CLI's post-fill ``assert_silver_schema``
    # still matches.
    return df.with_columns(
        pl.Series(
            "veiculo_marca",
            [e.veiculo_marca for e in per_row],
            dtype=pl.String,
        ),
        pl.Series(
            "veiculo_modelo",
            [e.veiculo_modelo for e in per_row],
            dtype=pl.String,
        ),
        pl.Series(
            "veiculo_ano",
            [e.veiculo_ano for e in per_row],
            dtype=pl.Int32,
        ),
        pl.Series(
            "concorrente_mencionado",
            [e.concorrente_mencionado for e in per_row],
            dtype=pl.String,
        ),
        pl.Series(
            "valor_pago_atual_brl",
            [e.valor_pago_atual_brl for e in per_row],
            dtype=pl.Float64,
        ),
        pl.Series(
            "sinistro_historico",
            [e.sinistro_historico for e in per_row],
            dtype=pl.Boolean,
        ),
    )


# ---------------------------------------------------------------------------
# JSON parsing helpers.
# ---------------------------------------------------------------------------


def _parse_response(text: str, *, body: str) -> ExtractedEntities:
    logger = get_logger("pipeline.silver.llm")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        logger.warning(
            "llm.extraction.invalid_json",
            body_preview=body[:200],
            response_preview=text[:200],
        )
        return ExtractedEntities.null()
    if not isinstance(payload, dict):
        logger.warning(
            "llm.extraction.invalid_json",
            body_preview=body[:200],
            reason="not_an_object",
        )
        return ExtractedEntities.null()
    if any(k not in payload for k in _REQUIRED_FIELDS):
        logger.warning(
            "llm.extraction.invalid_json",
            body_preview=body[:200],
            reason="missing_fields",
        )
        return ExtractedEntities.null()
    return ExtractedEntities(
        veiculo_marca=_coerce_str(payload["veiculo_marca"]),
        veiculo_modelo=_coerce_str(payload["veiculo_modelo"]),
        veiculo_ano=_coerce_int(payload["veiculo_ano"]),
        concorrente_mencionado=_coerce_str(payload["concorrente_mencionado"]),
        valor_pago_atual_brl=_coerce_float(payload["valor_pago_atual_brl"]),
        sinistro_historico=_coerce_bool(payload["sinistro_historico"]),
    )


def _coerce_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _coerce_int(value: Any) -> int | None:
    # LEARN: ``isinstance(True, int)`` is ``True`` in Python — bool is a
    # subclass of int. Exclude it explicitly so the LLM returning
    # ``true`` for ``veiculo_ano`` does not become the year 1.
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _coerce_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None
