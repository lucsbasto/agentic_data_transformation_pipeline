"""Tests for ``pipeline.silver.extract``."""

from __future__ import annotations

import polars as pl
import pytest

from pipeline.silver.extract import (
    extract_cep_prefix,
    extract_cep_prefix_expr,
    extract_email_domain,
    extract_email_domain_expr,
    extract_has_cpf,
    extract_has_cpf_expr,
    extract_has_phone_mention,
    extract_has_phone_mention_expr,
    extract_plate_format,
    extract_plate_format_expr,
)

# ---------------------------------------------------------------------------
# email_domain
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("please email ana.paula@gmail.com now", "gmail.com"),
        ("send to Ana.Paula@GMAIL.com asap", "gmail.com"),
        ("leads@foo.example.co.uk", "foo.example.co.uk"),
        ("multiple a@x.com and b@y.com", "x.com"),  # first match wins
        ("no email here", None),
        ("", None),
        (None, None),
    ],
)
def test_extract_email_domain(text: str | None, expected: str | None) -> None:
    assert extract_email_domain(text) == expected


def test_extract_email_domain_expr_maps_each_row() -> None:
    df = pl.DataFrame(
        {"body": ["send ana@gmail.com", None, "no email", "x@test.org"]}
    )
    out = df.select(extract_email_domain_expr(pl.col("body")).alias("d"))
    assert out["d"].to_list() == ["gmail.com", None, None, "test.org"]


# ---------------------------------------------------------------------------
# has_cpf
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("meu CPF é 123.456.789-01", True),
        ("cpf: 123.456.78901 ok", True),  # optional dash
        ("no cpf here", False),
        ("bare 12345678901 is a phone not cpf", False),
        ("", False),
        (None, False),
    ],
)
def test_extract_has_cpf(text: str | None, expected: bool) -> None:
    assert extract_has_cpf(text) is expected


def test_extract_has_cpf_expr_null_becomes_false() -> None:
    """Null input maps to ``False``, not ``null`` — the column is a
    presence signal, not a three-valued logic column.
    """
    df = pl.DataFrame({"body": ["has 123.456.789-01", None]})
    out = df.select(extract_has_cpf_expr(pl.col("body")).alias("has_cpf"))
    assert out["has_cpf"].to_list() == [True, False]
    assert out.schema["has_cpf"] == pl.Boolean


# ---------------------------------------------------------------------------
# cep_prefix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("CEP 01310-930 centro", "01310"),
        ("zip 01310930 here", "01310"),  # optional dash
        ("no cep in this body", None),
        ("", None),
        (None, None),
    ],
)
def test_extract_cep_prefix(text: str | None, expected: str | None) -> None:
    assert extract_cep_prefix(text) == expected


def test_extract_cep_prefix_expr_null_passthrough() -> None:
    df = pl.DataFrame({"body": ["cep 01310-930", None]})
    out = df.select(extract_cep_prefix_expr(pl.col("body")).alias("cep"))
    assert out["cep"].to_list() == ["01310", None]


# ---------------------------------------------------------------------------
# has_phone_mention
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("call me +55 11 98765-4321", True),
        ("second number 11987654321", True),
        ("no phone here", False),
        ("", False),
        (None, False),
    ],
)
def test_extract_has_phone_mention(text: str | None, expected: bool) -> None:
    assert extract_has_phone_mention(text) is expected


# ---------------------------------------------------------------------------
# plate_format
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("placa ABC1D23 do carro", "mercosul"),
        ("plate ABC-1234 old style", "old"),
        ("plate ABC1234 joined", "old"),
        ("no plate in sight", None),
        ("", None),
        (None, None),
    ],
)
def test_extract_plate_format(
    text: str | None, expected: str | None
) -> None:
    assert extract_plate_format(text) == expected


def test_extract_plate_format_prefers_mercosul_when_both_shapes_present() -> None:
    """Mercosul must be checked first — otherwise an ``ABC1234`` old
    substring in the same text as a Mercosul plate could flip the
    classification depending on order.
    """
    # A Mercosul plate ABC1D23 does NOT match the old pattern (position
    # 5 is a letter, not a digit). But both patterns are *able* to find
    # *different* plates in the same text; the function returns the
    # Mercosul one when both exist.
    text = "cars: ABC1D23 and XYZ-4567 seen"
    assert extract_plate_format(text) == "mercosul"


def test_extract_plate_format_expr_maps_each_row() -> None:
    df = pl.DataFrame(
        {"body": ["ABC1D23 here", "old ABC-1234", "nothing", None]}
    )
    out = df.select(extract_plate_format_expr(pl.col("body")).alias("pf"))
    assert out["pf"].to_list() == ["mercosul", "old", None, None]


# ---------------------------------------------------------------------------
# Combined: all five extractors on a real-world message.
# ---------------------------------------------------------------------------


def test_all_extractors_on_mixed_body() -> None:
    body = (
        "Oi, sou Ana (ana@gmail.com). CPF 123.456.789-01. "
        "CEP 01310-930. Outro contato: +55 11 98765-4321. "
        "Placa do carro: ABC1D23."
    )
    assert extract_email_domain(body) == "gmail.com"
    assert extract_has_cpf(body) is True
    assert extract_cep_prefix(body) == "01310"
    assert extract_has_phone_mention(body) is True
    assert extract_plate_format(body) == "mercosul"


def test_has_phone_mention_expr_wraps() -> None:
    df = pl.DataFrame({"body": ["call 11987654321", None, "no phone"]})
    out = df.select(
        extract_has_phone_mention_expr(pl.col("body")).alias("has_phone")
    )
    assert out["has_phone"].to_list() == [True, False, False]
