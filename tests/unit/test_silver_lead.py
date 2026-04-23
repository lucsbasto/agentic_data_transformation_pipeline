"""Tests for ``pipeline.silver.lead``."""

from __future__ import annotations

import hmac

import polars as pl
import pytest

from pipeline.silver.lead import (
    LEAD_ID_HEX_CHARS,
    derive_lead_id,
    derive_lead_id_expr,
    normalize_phone_digits,
)

SECRET = "unit-test-secret-never-use-in-prod"


# ---------------------------------------------------------------------------
# normalize_phone_digits
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("+55 11 98765-4321", "11987654321"),
        ("(11) 98765-4321", "11987654321"),
        ("11987654321", "11987654321"),
        ("+551198765432", "1198765432"),  # 10-digit landline after code strip
        ("1198765432", "1198765432"),     # 10-digit landline kept as-is
        ("  +55 (11) 9 8765 4321  ", "11987654321"),
    ],
)
def test_normalize_phone_digits_canonicalizes(raw: str, expected: str) -> None:
    assert normalize_phone_digits(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "abc", "12345", "5512345"])
def test_normalize_phone_digits_returns_none_on_garbage(raw: str | None) -> None:
    assert normalize_phone_digits(raw) is None


def test_normalize_phone_digits_preserves_14_digit_non_br_prefix() -> None:
    """A 14-digit string starting with 55 is too long for BR; we do not
    silently strip the prefix and claim it as a national number."""
    assert normalize_phone_digits("55123456789012") is None


# ---------------------------------------------------------------------------
# derive_lead_id
# ---------------------------------------------------------------------------


def test_derive_lead_id_deterministic() -> None:
    """Same phone + same secret → same id. The stability guarantee."""
    assert derive_lead_id("11987654321", SECRET) == derive_lead_id(
        "11987654321", SECRET
    )


def test_derive_lead_id_formatting_insensitive() -> None:
    """Formatting variations must collide — otherwise we double-count leads."""
    a = derive_lead_id("+55 11 98765-4321", SECRET)
    b = derive_lead_id("(11) 98765-4321", SECRET)
    c = derive_lead_id("11987654321", SECRET)
    assert a == b == c


def test_derive_lead_id_different_phones_differ() -> None:
    assert derive_lead_id("11987654321", SECRET) != derive_lead_id(
        "11987654322", SECRET
    )


def test_derive_lead_id_secret_rotation_changes_hash() -> None:
    """Rotating the secret must change every id — that is exactly why
    the PRD warns operators to keep the secret stable across runs."""
    assert derive_lead_id("11987654321", SECRET) != derive_lead_id(
        "11987654321", "different-secret"
    )


def test_derive_lead_id_length_and_hex() -> None:
    lead = derive_lead_id("11987654321", SECRET)
    assert lead is not None
    assert len(lead) == LEAD_ID_HEX_CHARS
    # Hex-only: passes int parse with base 16
    int(lead, 16)


def test_derive_lead_id_matches_raw_hmac() -> None:
    """Sanity check: the hex prefix is exactly the truncated HMAC-SHA256.
    This pins the algorithm so a future refactor cannot silently change
    ids — rotating the hash function would scatter every Gold join key.
    """
    expected = (
        hmac.new(
            key=SECRET.encode("utf-8"),
            msg=b"11987654321",
            digestmod="sha256",
        )
        .hexdigest()[:LEAD_ID_HEX_CHARS]
    )
    assert derive_lead_id("11987654321", SECRET) == expected


@pytest.mark.parametrize("raw", [None, "", "abc", "123"])
def test_derive_lead_id_null_or_unparseable_returns_none(raw: str | None) -> None:
    assert derive_lead_id(raw, SECRET) is None


def test_derive_lead_id_empty_secret_raises() -> None:
    """An empty secret degrades HMAC to a public hash, which breaks the
    non-reversibility contract. Fail loudly rather than silently.
    """
    with pytest.raises(ValueError, match="PIPELINE_LEAD_SECRET"):
        derive_lead_id("11987654321", "")


# ---------------------------------------------------------------------------
# derive_lead_id_expr
# ---------------------------------------------------------------------------


def test_derive_lead_id_expr_maps_each_row() -> None:
    df = pl.DataFrame(
        {
            "phone": [
                "+55 11 98765-4321",
                "(11) 98765-4321",
                "11987654322",
                None,
            ]
        }
    )
    out = df.select(derive_lead_id_expr(pl.col("phone"), SECRET).alias("lead_id"))
    values = out["lead_id"].to_list()
    # First two collide (same digits), third differs, fourth is null.
    assert values[0] == values[1]
    assert values[0] != values[2]
    assert values[3] is None
    # Non-null ids are 16-hex each.
    for v in values[:3]:
        assert v is not None
        assert len(v) == LEAD_ID_HEX_CHARS
