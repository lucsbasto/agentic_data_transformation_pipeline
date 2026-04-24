"""Shared phrase catalogues for the Gold lane.

Three Gold modules count phrase hits over the lead-side text in
``message_body_masked``:

- ``intent_score`` — closing / evasive / technical-question phrases
  feed the PRD §17.1 score components.
- ``price_sensitivity`` — haggling phrases feed the F3-RF-16 bucket.
- ``insights`` — objection phrases feed the ``objections`` array in
  the summary JSON.

The catalogues live here so the three consumers cannot drift. A
phrase added to ``OBJECTION_PHRASES`` automatically appears in the
insights summary and (where applicable) in the score's evasive
component if it is also added there explicitly. Centralising also
makes the matching helpers reusable: every consumer normalizes input
the same way (case-folded + accent-stripped) so ``"tá caro"`` in the
catalogue matches ``"ta caro"``, ``"TA CARO"``, and ``"tá Caro"`` in
the body without per-consumer special casing.
"""

from __future__ import annotations

# LEARN: ``unicodedata`` is stdlib. ``NFKD`` decomposes accented
# characters into base letter + combining mark; we then drop the
# combining marks to get the unaccented form. Same approach Silver's
# ``normalize_name`` uses for ``sender_name`` reconciliation.
import unicodedata
from collections.abc import Iterable
from functools import lru_cache
from typing import Final

__all__ = [
    "CLOSING_PHRASES",
    "EVASIVE_PHRASES",
    "HAGGLING_PHRASES",
    "OBJECTION_PHRASES",
    "TECHNICAL_QUESTION_PHRASES",
    "count_phrase_hits",
    "matches_any",
    "normalize",
]


# ---------------------------------------------------------------------------
# Catalogues — one tuple per analytical signal.
# ---------------------------------------------------------------------------
# LEARN: tuples (not sets) — order is irrelevant for membership tests
# but immutability + stable iteration order keeps the catalogue
# trivially diffable when someone reviews a phrase change. ``Final``
# stops a future caller from reassigning the names.

OBJECTION_PHRASES: Final[tuple[str, ...]] = (
    "tá caro",
    "vou pensar",
    "depois",
    "me dá desconto",
    "outro lugar",
)
"""Insights ``objections`` array (F3 design §8)."""

HAGGLING_PHRASES: Final[tuple[str, ...]] = (
    "desconto",
    "abaixar",
    "mais barato",
    "outro lugar",
    "tá caro",
    "melhor preço",
)
"""``price_sensitivity`` haggling-phrase counter (F3-RF-16)."""

CLOSING_PHRASES: Final[tuple[str, ...]] = (
    "fechou",
    "manda boleto",
    "tá bom",
)
"""Intent-score ``presenca_termo_fechamento`` source (F3 design §6)."""

EVASIVE_PHRASES: Final[tuple[str, ...]] = (
    "vou pensar",
    "depois te aviso",
    "preciso ver",
)
"""Intent-score ``ausencia_palavras_evasivas`` source (F3 design §6)."""

TECHNICAL_QUESTION_PHRASES: Final[tuple[str, ...]] = (
    "franquia",
    "cobre",
    "assistência",
    "carro reserva",
)
"""Intent-score ``perguntas_tecnicas_normalizado`` source (F3 design §6)."""


# ---------------------------------------------------------------------------
# Matching helpers — case-folded + accent-stripped substring tests.
# ---------------------------------------------------------------------------


def normalize(text: str | None) -> str:
    """Return ``text`` lower-cased, accent-stripped, and trimmed.

    Null / empty / whitespace-only input collapses to ``""`` so the
    counter helpers can short-circuit on the empty string instead of
    walking a meaningless body.
    """
    if not text or not text.strip():
        return ""
    decomposed = unicodedata.normalize("NFKD", text)
    no_accents = "".join(c for c in decomposed if not unicodedata.combining(c))
    return no_accents.casefold().strip()


@lru_cache(maxsize=32)
def _normalize_phrases(phrases: tuple[str, ...]) -> tuple[str, ...]:
    """Cache normalized catalogues so we pay the NFKD cost once per
    tuple identity. Module-level catalogues are immutable tuples, so
    the cache key (their identity-stable hash) stays warm across the
    whole batch."""
    return tuple(normalize(p) for p in phrases)


def count_phrase_hits(body: str | None, phrases: Iterable[str]) -> int:
    """Number of distinct ``phrases`` that appear in ``body``.

    Both sides are normalized (lower + accent-stripped) before the
    substring check, so catalogue entries with Portuguese accents
    match their unaccented counterparts in the body. Empty / null
    bodies return ``0``. Phrases are normalized once via an
    LRU-cached helper — passing the same module-level catalogue twice
    in a hot loop is free after the first call.
    """
    norm_body = normalize(body)
    if not norm_body:
        return 0
    norm_phrases = _normalize_phrases(tuple(phrases))
    return sum(1 for phrase in norm_phrases if phrase in norm_body)


def matches_any(body: str | None, phrases: Iterable[str]) -> bool:
    """``True`` when at least one phrase appears in ``body``."""
    return count_phrase_hits(body, phrases) > 0
