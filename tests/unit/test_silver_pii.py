"""Tests for ``pipeline.silver.pii`` — regex-based PII maskers.

Every test follows the same table-driven shape so adding a new input /
expected output is a one-line change. The tests are split by PII type
and then by intent: *positives* (the masker must rewrite) and
*negatives* (the masker must leave the string alone). Catching negatives
is as important as catching positives — a trigger-happy regex that
eats normal numbers would destroy analytics.
"""

from __future__ import annotations

import pytest

from pipeline.silver.pii import (
    PIICounts,
    empty_counts,
    mask_all_pii,
    mask_phone_only,
)

# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("ana@gmail.com", "a****@g****.com"),
        ("ANA.PAULA@OUTLOOK.COM", "A********@O******.COM"),
        ("lead+promo@contato.empresa.com.br", "l*********@c******.br"),
        ("a@b.co", "a****@b****.co"),  # short input still padded with >=4 stars
    ],
)
def test_email_masked_preserves_first_char_and_tld(raw: str, expected: str) -> None:
    masked, counts = mask_all_pii(raw)
    assert masked == expected
    assert counts["emails"] == 1


@pytest.mark.parametrize(
    "raw",
    [
        "no email here",
        "@ not an email",
        "foo@",
        "bar.com",
        "missing@tld",
    ],
)
def test_email_negatives_left_alone(raw: str) -> None:
    masked, counts = mask_all_pii(raw)
    assert counts["emails"] == 0
    # Text should be unchanged; assert identity is too strong because
    # phone/CPF/CEP may still match — check the email subcomponent:
    assert "****@" not in masked


# ---------------------------------------------------------------------------
# CPF
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "123.456.789-01",
        "123.456.78901",  # dash is optional; dots are required
        "CPF 123.456.789-01 pronto",
    ],
)
def test_cpf_positives(raw: str) -> None:
    masked, counts = mask_all_pii(raw)
    assert counts["cpfs"] == 1
    assert "***.***.***-**" in masked


def test_cpf_bare_digits_treated_as_phone_not_cpf() -> None:
    """Documented trade-off: 11 bare digits in WhatsApp text are almost
    always a mobile phone. CPF regex requires dots to keep the phone
    masker in charge of this shape.
    """
    masked, counts = mask_all_pii("me liga 11987654321")
    assert counts["cpfs"] == 0
    assert counts["phones"] == 1
    assert "**21" in masked


@pytest.mark.parametrize(
    "raw",
    [
        "pedido 1234",
        "quantidade 99",
        "ano 2026",
    ],
)
def test_cpf_negatives_left_alone(raw: str) -> None:
    masked, counts = mask_all_pii(raw)
    assert counts["cpfs"] == 0
    assert "***.***.***-**" not in masked


# ---------------------------------------------------------------------------
# CEP
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "01310-930",
        "01310930",
        "CEP 01310-930",
    ],
)
def test_cep_positives(raw: str) -> None:
    masked, counts = mask_all_pii(raw)
    assert counts["ceps"] == 1
    assert "*****-***" in masked


def test_cep_negative_short_number() -> None:
    masked, counts = mask_all_pii("quarto 1234567")  # 7 digits, not a CEP
    assert counts["ceps"] == 0
    assert "*****-***" not in masked


# ---------------------------------------------------------------------------
# Phone
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "+55 (11) 98765-4321",
        "+55 11 98765 4321",
        "(11) 98765-4321",
        "11 98765 4321",
        "11987654321",
    ],
)
def test_phone_positives_keep_last_two(raw: str) -> None:
    masked, counts = mask_all_pii(raw)
    assert counts["phones"] >= 1
    assert masked.endswith("**21")
    # Sanity: the raw area code and the middle digits are not in the mask.
    assert "98765" not in masked


@pytest.mark.parametrize(
    "raw",
    [
        "ano 2026",
        "pedido 12",
        "12345",
    ],
)
def test_phone_negatives(raw: str) -> None:
    _masked, counts = mask_all_pii(raw)
    assert counts["phones"] == 0


def test_phone_does_not_eat_formatted_cpf() -> None:
    """Ordering invariant: CPF runs before phone, and a formatted CPF
    matches the (stricter) CPF regex first — so a phone masker never
    overwrites a clearly formatted CPF.
    """
    masked, counts = mask_all_pii("CPF 123.456.789-01")
    assert counts["cpfs"] == 1
    assert counts["phones"] == 0
    assert "***.***.***-**" in masked
    assert "(**)" not in masked


def test_mask_phone_only_handles_column_value() -> None:
    # Used by the ``sender_phone`` column — the whole cell is one phone.
    assert mask_phone_only("+5511987654321") == "+55 (**) *****-**21"


# ---------------------------------------------------------------------------
# Licence plate
# ---------------------------------------------------------------------------


def test_plate_mercosul_keeps_format_markers() -> None:
    masked, counts = mask_all_pii("placa ABC1D23 estacionada")
    assert counts["plates"] == 1
    assert "***1D**" in masked
    assert "ABC1D23" not in masked


def test_plate_old_keeps_last_two_digits() -> None:
    masked, counts = mask_all_pii("placa XYZ-1234 confirmada")
    assert counts["plates"] == 1
    assert "***-**34" in masked
    assert "XYZ-1234" not in masked


def test_plate_negative_not_a_plate() -> None:
    _masked, counts = mask_all_pii("ABC no plate here")
    assert counts["plates"] == 0


# ---------------------------------------------------------------------------
# Aggregation + mixed-field behaviour
# ---------------------------------------------------------------------------


def test_mixed_text_collects_counts_and_masks_all() -> None:
    raw = (
        "ola, me chamo ana@gmail.com, CPF 123.456.789-01, "
        "cel +55 11 98765-4321, CEP 01310-930, "
        "placa ABC1D23 e a antiga XYZ-1234."
    )
    masked, counts = mask_all_pii(raw)
    assert counts == PIICounts(
        emails=1, cpfs=1, ceps=1, phones=1, plates=2
    )
    # Every masker contributed its fixed token.
    assert "a****@g****.com" in masked
    assert "***.***.***-**" in masked
    assert "*****-***" in masked
    assert "**21" in masked
    assert "***1D**" in masked
    assert "***-**34" in masked
    # No raw PII survives.
    for token in ("ana@gmail", "123.456.789", "98765", "01310-930", "ABC1D23", "XYZ-1234"):
        assert token not in masked


def test_empty_counts_is_zeroed() -> None:
    counts = empty_counts()
    assert counts == PIICounts(emails=0, cpfs=0, ceps=0, phones=0, plates=0)


def test_mask_all_pii_idempotent_on_already_masked_text() -> None:
    """Running the masker twice on an already masked string should not
    change the masked tokens — ``***.***.***-**`` is not a CPF, etc.
    """
    raw = "ana@gmail.com CPF 12345678901 CEP 01310930 cel +5511987654321"
    once, _ = mask_all_pii(raw)
    twice, counts = mask_all_pii(once)
    assert twice == once
    # Nothing to mask on the second pass.
    assert counts == empty_counts()
