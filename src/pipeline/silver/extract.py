"""Regex-based entity extraction for the Silver layer.

Where :mod:`pipeline.silver.pii` *removes* identifying substrings from
``message_body``, this module *extracts analytical dimensions* from the
same raw text and promotes them to typed columns.

Design
------

The columns we emit are non-identifying on purpose:

- ``email_domain`` — ``"gmail.com"``, ``"outlook.com.br"``. Domains are
  public data; the local part that *would* identify a lead is dropped.
- ``has_cpf`` — boolean. A CPF value carries no analytical dimension
  (every Brazilian citizen has one, and the digits are a random id),
  so the presence flag is the only useful signal.
- ``cep_prefix`` — first 5 digits of a CEP. The CEP range identifies a
  region, not a person; keeping only the prefix strips the street-level
  location component.
- ``has_phone_mention`` — boolean. A phone-shaped substring in the body
  may be a second contact number shared by the lead; the redacted
  ``sender_phone_masked`` already covers their primary number.
- ``plate_format`` — ``"mercosul"`` / ``"old"`` / ``None``. The format
  class matters for fleet analytics (Mercosul plates are newer cars)
  without leaking the plate itself.

Everything here is a pure function — no I/O, no logging — and each
helper handles ``None``/empty input without raising. Callers decide
whether "no body" should produce a ``null`` dimension (``email_domain``)
or a ``False`` flag (``has_cpf``).

Regex patterns are intentionally duplicated from :mod:`pipeline.silver.pii`
rather than imported as private names: both modules own their regex
surface, and a shared regex module would over-couple extraction to
masking. Drift is guarded by mirrored tests: when one pattern widens,
the other's tests fail first.
"""

from __future__ import annotations

# LEARN: ``re`` is Python's stdlib regex engine. Each pattern is
# compiled at import time — compilation is expensive, matching is
# cheap — so subsequent calls reuse the cached ``Pattern`` object.
import re
from typing import Final, Literal

import polars as pl

__all__ = [
    "extract_cep_prefix",
    "extract_cep_prefix_expr",
    "extract_email_domain",
    "extract_email_domain_expr",
    "extract_has_cpf",
    "extract_has_cpf_expr",
    "extract_has_phone_mention",
    "extract_has_phone_mention_expr",
    "extract_plate_format",
    "extract_plate_format_expr",
]


# ---------------------------------------------------------------------------
# Compiled regexes — mirror ``pii.py`` exactly.
# ---------------------------------------------------------------------------

_EMAIL_RE: Final[re.Pattern[str]] = re.compile(
    r"\b([A-Za-z0-9._%+\-]+)@([A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*)\.([A-Za-z]{2,})\b"
)

_CPF_RE: Final[re.Pattern[str]] = re.compile(
    r"\b\d{3}\.\d{3}\.\d{3}-?\d{2}\b"
)

_CEP_RE: Final[re.Pattern[str]] = re.compile(r"\b(\d{5})-?(\d{3})\b")

_PHONE_RE: Final[re.Pattern[str]] = re.compile(
    r"(?<!\d)"
    r"(?:\+?55\s?-?)?"
    r"\(?\d{2}\)?\s?-?"
    r"9?\d{4}[\s\-]?\d{4}"
    r"(?!\d)"
)

_PLATE_MERCOSUL_RE: Final[re.Pattern[str]] = re.compile(
    r"\b[A-Z]{3}\d[A-Z]\d{2}\b"
)

_PLATE_OLD_RE: Final[re.Pattern[str]] = re.compile(r"\b[A-Z]{3}-?\d{4}\b")


# ---------------------------------------------------------------------------
# Email → domain.
# ---------------------------------------------------------------------------


def extract_email_domain(text: str | None) -> str | None:
    """Return the first email domain found in ``text``, lower-cased.

    ``"send to Ana.Paula@GMAIL.com asap"`` → ``"gmail.com"``.
    Multi-label hosts (``"leads@foo.example.co.uk"``) collapse to
    ``"foo.example.co.uk"``. Returns ``None`` when no email matches so
    callers can distinguish "no signal" from "empty bucket".
    """
    if text is None:
        return None
    match = _EMAIL_RE.search(text)
    if match is None:
        return None
    host = match.group(2)
    tld = match.group(3)
    # LEARN: ``f"{host}.{tld}".lower()`` concatenates host + TLD and
    # then normalizes case. Real-world data mixes ``Gmail.com`` and
    # ``gmail.com`` and ``GMAIL.com`` — lower-casing once here prevents
    # three buckets downstream.
    return f"{host}.{tld}".lower()


def extract_email_domain_expr(expr: pl.Expr) -> pl.Expr:
    """Polars wrapper around :func:`extract_email_domain`."""
    return expr.map_elements(
        extract_email_domain,
        return_dtype=pl.String,
        skip_nulls=True,
    )


# ---------------------------------------------------------------------------
# CPF → presence flag.
# ---------------------------------------------------------------------------


def extract_has_cpf(text: str | None) -> bool:
    """``True`` iff ``text`` contains a CPF-shaped substring.

    Only the formatted form (``123.456.789-01`` or ``123.456.78901``)
    counts — a raw 11-digit run is far more likely to be a mobile
    phone in WhatsApp text, and the masker treats it as such.
    """
    if not text:
        return False
    return _CPF_RE.search(text) is not None


def extract_has_cpf_expr(expr: pl.Expr) -> pl.Expr:
    """Polars wrapper around :func:`extract_has_cpf`.

    Null input collapses to ``False`` (not null) — the column is a
    boolean *signal*, not a three-value presence-or-unknown field.
    """
    return expr.map_elements(
        extract_has_cpf,
        return_dtype=pl.Boolean,
        skip_nulls=False,
    )


# ---------------------------------------------------------------------------
# CEP → region prefix (5 digits).
# ---------------------------------------------------------------------------


def extract_cep_prefix(text: str | None) -> str | None:
    """First 5 digits of the earliest CEP in ``text``, or ``None``.

    Brazilian CEPs look like ``01310-930`` — the first five digits
    encode a region (street-level precision lives in the trailing 3).
    Keeping only the prefix gives analytics a geographic dimension
    without a street-precise locator.
    """
    if text is None:
        return None
    match = _CEP_RE.search(text)
    if match is None:
        return None
    return match.group(1)


def extract_cep_prefix_expr(expr: pl.Expr) -> pl.Expr:
    return expr.map_elements(
        extract_cep_prefix,
        return_dtype=pl.String,
        skip_nulls=True,
    )


# ---------------------------------------------------------------------------
# Phone mention → presence flag.
# ---------------------------------------------------------------------------


def extract_has_phone_mention(text: str | None) -> bool:
    """``True`` iff ``text`` contains any phone-shaped substring.

    The signal here is *a second number shared inside a message*, not
    the sender's own number (which already lives in
    ``sender_phone_masked``). Analytics uses this to detect messages
    where a lead passes an alternative contact.
    """
    if not text:
        return False
    return _PHONE_RE.search(text) is not None


def extract_has_phone_mention_expr(expr: pl.Expr) -> pl.Expr:
    return expr.map_elements(
        extract_has_phone_mention,
        return_dtype=pl.Boolean,
        skip_nulls=False,
    )


# ---------------------------------------------------------------------------
# Licence plate → format class.
# ---------------------------------------------------------------------------

PlateFormat = Literal["mercosul", "old"]


def extract_plate_format(text: str | None) -> PlateFormat | None:
    """Return the format class of the first plate in ``text``.

    - ``"mercosul"`` — ``ABC1D23`` (letter-letter-letter-digit-letter-
      digit-digit). Introduced 2018, so a hit signals a post-2018 car.
    - ``"old"``       — ``ABC-1234`` / ``ABC1234`` (pre-Mercosul).
    - ``None``        — no plate detected.

    Mercosul is checked first because its format is stricter; the old
    pattern would otherwise match the first three letters of a Mercosul
    plate as ``"ABC"``-prefixed and misclassify.
    """
    if text is None:
        return None
    if _PLATE_MERCOSUL_RE.search(text) is not None:
        return "mercosul"
    if _PLATE_OLD_RE.search(text) is not None:
        return "old"
    return None


def extract_plate_format_expr(expr: pl.Expr) -> pl.Expr:
    return expr.map_elements(
        extract_plate_format,
        return_dtype=pl.String,
        skip_nulls=True,
    )
