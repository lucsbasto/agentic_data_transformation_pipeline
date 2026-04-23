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

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Final

import polars as pl

from pipeline.errors import LLMCallError
from pipeline.llm.client import LLMClient
from pipeline.logging import get_logger


def _body_hash(body: str) -> str:
    """Return a 16-char SHA-256 prefix of ``body``.

    Used in structured log fields instead of a raw body slice — the
    masked body is best-effort PII-free but the LLM response text
    echoed alongside it is not, so we keep only an opaque identifier
    that lets operators correlate log lines without leaking content
    into aggregators.
    """
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]

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
    '- "valor_pago_atual_brl" (number | null): BRL amount the LEAD '
    "(customer) explicitly says they currently pay to their EXISTING "
    "insurer, as a number (no currency symbol). Return null when the "
    "figure comes from the agent/seller quoting or offering a new "
    "price (e.g. 'consigo fazer por R$ X', 'fica R$ X', 'posso "
    "abaixar pra R$ X', 'plano básico R$ X/ano'). Return null when "
    "it is unclear whether the figure is a current lead-paid premium "
    "versus a quoted or discounted offer.\n"
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
            body_hash=_body_hash(body),
            body_len=len(body),
        )
        return ExtractedEntities.null()
    # LEARN: LLMClient is documented to raise only LLMCallError, but a
    # parser edge case (unexpected .text / .usage / .content shape, a
    # future SDK change, a bug) can still leak through. Catching those
    # here keeps the Silver batch alive: one row's entities go null
    # instead of the entire run crashing with an uncaught exception.
    except Exception as exc:
        logger.exception(
            "llm.extraction.unexpected",
            error_type=type(exc).__name__,
            body_hash=_body_hash(body),
            body_len=len(body),
        )
        return ExtractedEntities.null()
    try:
        return _parse_response(response.text, body=body)
    except Exception as exc:
        logger.exception(
            "llm.extraction.unexpected",
            error_type=type(exc).__name__,
            body_hash=_body_hash(body),
            body_len=len(body),
        )
        return ExtractedEntities.null()


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
        entities, metrics = _call_and_parse(body, client=client, logger=logger)
        lookup[body] = entities
        input_tokens_total += metrics.input_tokens
        output_tokens_total += metrics.output_tokens
        if metrics.cache_hit:
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
# Call + parse helper (keeps ``apply_llm_extraction`` under the ruff
# ``PLR0915`` statement budget and centralises the "never let an
# exception escape" contract).
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _CallMetrics:
    """Minimal metrics surface from one attempted LLM round-trip.

    ``cache_hit`` stays ``False`` on every failure path so a null
    result never silently flips the batch-level ``calls_made`` counter
    from "charged" to "free".
    """

    input_tokens: int
    output_tokens: int
    cache_hit: bool


_NULL_METRICS: Final[_CallMetrics] = _CallMetrics(0, 0, False)


def _call_and_parse(
    body: str, *, client: LLMClient, logger: Any
) -> tuple[ExtractedEntities, _CallMetrics]:
    """Run one ``cached_call`` + parse. Return null dataclass + null
    metrics on any failure, never raise. Counts budget as a miss when
    the provider round-tripped; keeps the Silver batch alive
    regardless of parser / SDK edge cases."""
    try:
        response = client.cached_call(system=SYSTEM_PROMPT, user=body)
    except LLMCallError as exc:
        logger.warning(
            "llm.extraction.failed",
            error=str(exc),
            body_hash=_body_hash(body),
            body_len=len(body),
        )
        return ExtractedEntities.null(), _NULL_METRICS
    except Exception as exc:
        logger.exception(
            "llm.extraction.unexpected",
            error_type=type(exc).__name__,
            body_hash=_body_hash(body),
            body_len=len(body),
        )
        return ExtractedEntities.null(), _NULL_METRICS
    try:
        entities = _parse_response(response.text, body=body)
    except Exception as exc:
        logger.exception(
            "llm.extraction.unexpected",
            error_type=type(exc).__name__,
            body_hash=_body_hash(body),
            body_len=len(body),
        )
        entities = ExtractedEntities.null()
    metrics = _CallMetrics(
        input_tokens=response.input_tokens,
        output_tokens=response.output_tokens,
        cache_hit=response.cache_hit,
    )
    return entities, metrics


# ---------------------------------------------------------------------------
# JSON parsing helpers.
# ---------------------------------------------------------------------------


def _parse_response(text: str, *, body: str) -> ExtractedEntities:
    logger = get_logger("pipeline.silver.llm")
    body_hash = _body_hash(body)
    body_len = len(body)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        logger.warning(
            "llm.extraction.invalid_json",
            body_hash=body_hash,
            body_len=body_len,
            response_len=len(text),
            reason="json_decode_error",
        )
        return ExtractedEntities.null()
    if not isinstance(payload, dict):
        logger.warning(
            "llm.extraction.invalid_json",
            body_hash=body_hash,
            body_len=body_len,
            reason="not_an_object",
        )
        return ExtractedEntities.null()
    if any(k not in payload for k in _REQUIRED_FIELDS):
        logger.warning(
            "llm.extraction.invalid_json",
            body_hash=body_hash,
            body_len=body_len,
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
