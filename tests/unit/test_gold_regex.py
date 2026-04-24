"""Lock the Gold regex catalogues + matching helpers."""

from __future__ import annotations

import pytest

from pipeline.gold._regex import (
    CLOSING_PHRASES,
    EVASIVE_PHRASES,
    HAGGLING_PHRASES,
    OBJECTION_PHRASES,
    TECHNICAL_QUESTION_PHRASES,
    count_phrase_hits,
    matches_any,
    normalize,
)

# ---------------------------------------------------------------------------
# Catalogues — shape + non-empty + immutable.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "catalogue"),
    [
        ("OBJECTION_PHRASES", OBJECTION_PHRASES),
        ("HAGGLING_PHRASES", HAGGLING_PHRASES),
        ("CLOSING_PHRASES", CLOSING_PHRASES),
        ("EVASIVE_PHRASES", EVASIVE_PHRASES),
        ("TECHNICAL_QUESTION_PHRASES", TECHNICAL_QUESTION_PHRASES),
    ],
)
def test_catalogue_is_a_non_empty_tuple_of_strings(
    name: str, catalogue: tuple[str, ...]
) -> None:
    """Every catalogue must be a tuple (immutable) and contain at
    least one phrase. Fragile defaults like an empty list would make
    every consumer silently report zero hits."""
    assert isinstance(catalogue, tuple), f"{name} must be a tuple"
    assert len(catalogue) > 0, f"{name} cannot be empty"
    assert all(isinstance(p, str) and p for p in catalogue)


def test_objections_and_haggling_share_overlap_intentionally() -> None:
    """`'tá caro'` and `'outro lugar'` deliberately appear in both
    OBJECTION and HAGGLING — they signal distinct analytical things
    (objection rate vs price sensitivity) but the surface text is the
    same. If a future cleanup removes one but not the other, the two
    metrics silently desynchronize."""
    overlap = set(OBJECTION_PHRASES) & set(HAGGLING_PHRASES)
    assert {"tá caro", "outro lugar"}.issubset(overlap)


# ---------------------------------------------------------------------------
# normalize — case + accent collapsing.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("tá caro", "ta caro"),
        ("Tá Caro", "ta caro"),
        ("TÁ CARO", "ta caro"),
        ("franquia", "franquia"),
        ("assistência", "assistencia"),
        ("Vou Pensar", "vou pensar"),
    ],
)
def test_normalize_collapses_case_and_accents(raw: str, expected: str) -> None:
    assert normalize(raw) == expected


@pytest.mark.parametrize("falsy", [None, "", " ", "   ", "\t\n"])
def test_normalize_handles_null_and_empty(falsy: str | None) -> None:
    """Null / empty / whitespace-only inputs collapse to the empty
    string — never raise. The collapse lets ``count_phrase_hits``
    short-circuit on truly empty bodies instead of looping over
    meaningless whitespace."""
    assert normalize(falsy) == ""


# ---------------------------------------------------------------------------
# count_phrase_hits + matches_any — substring + accent insensitive.
# ---------------------------------------------------------------------------


def test_count_phrase_hits_is_accent_insensitive() -> None:
    """Body without an accent must still match a catalogue entry that
    has one ('tá caro' in catalogue, 'ta caro' in body)."""
    body = "achei seu seguro um pouco caro, ta caro mesmo"
    assert count_phrase_hits(body, OBJECTION_PHRASES) == 1


def test_count_phrase_hits_is_case_insensitive() -> None:
    body = "TA CARO e VOU PENSAR antes de fechar"
    assert count_phrase_hits(body, OBJECTION_PHRASES) == 2


def test_count_phrase_hits_counts_distinct_phrases_not_occurrences() -> None:
    """Two occurrences of the same phrase contribute one hit, not
    two. The phrase counter measures coverage, not frequency."""
    body = "vou pensar, vou pensar mais um pouco"
    assert count_phrase_hits(body, OBJECTION_PHRASES) == 1


def test_count_phrase_hits_returns_zero_on_empty_body() -> None:
    assert count_phrase_hits(None, OBJECTION_PHRASES) == 0
    assert count_phrase_hits("", HAGGLING_PHRASES) == 0
    assert count_phrase_hits("    ", CLOSING_PHRASES) == 0


def test_matches_any_returns_true_on_first_hit() -> None:
    body = "manda boleto"
    assert matches_any(body, CLOSING_PHRASES) is True


def test_matches_any_returns_false_when_no_phrase_present() -> None:
    body = "obrigado pela atenção"
    assert matches_any(body, CLOSING_PHRASES) is False


def test_count_phrase_hits_matches_partial_word_substrings() -> None:
    """Substring semantics: ``"desconto"`` in catalogue matches
    ``"descontos"`` and ``"descontão"`` in the body. Locked here so
    a future swap to word-boundary regex is a deliberate decision,
    not an accidental refactor."""
    body = "quero descontos e mais descontão pra fechar"
    assert count_phrase_hits(body, HAGGLING_PHRASES) == 1


def test_count_phrase_hits_handles_punctuation_separated_phrases() -> None:
    """Multi-word phrases survive punctuation: 'vou pensar' in
    'vou pensar...' still counts; 'depois te aviso' in 'depois te
    aviso amanhã' still counts."""
    body = "vou pensar... depois te aviso amanhã"
    assert count_phrase_hits(body, EVASIVE_PHRASES) == 2
