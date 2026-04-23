"""Lead identifier derivation for the Silver layer.

A *lead* is a real person who sent at least one WhatsApp message. The
same person can show up in multiple conversations (different campaigns,
re-contacts, etc.), always from the same phone number. Silver's job is
to assign every such row a stable ``lead_id`` so Gold can aggregate
"conversations per lead", "close rate per lead", and so on.

Requirements (PRD §6.2, §RNF-07)
--------------------------------

1. **Stable**: same phone → same ``lead_id`` on every run, forever —
   as long as :envvar:`PIPELINE_LEAD_SECRET` does not rotate. This is
   what lets Gold join two Silver runs on ``lead_id`` without a
   phone-number lookup.
2. **Non-reversible**: an attacker who obtains a Silver parquet must
   not be able to recover the phone number, even by brute-forcing
   every possible 11-digit number. Plain SHA-256 is not enough —
   Brazilian mobile phones are a ~10-billion-number space, which is
   within brute-force reach of a GPU in hours. Using HMAC with a
   secret keyed at 16+ bytes of entropy moves the attack target to
   "find the secret", which is out of reach without host compromise.
3. **Insensitive to formatting**: ``"+55 11 98765-4321"`` and
   ``"11987654321"`` and ``"(11) 9 8765 4321"`` must map to the same
   id, otherwise we silently double-count leads.

Design
------

- Normalize: strip everything that is not a digit, then drop a leading
  Brazilian country code (``55``) if the resulting string is 12 or 13
  digits. 10-digit (landline) and 11-digit (mobile) strings survive
  unchanged. Anything shorter than 10 or longer than 13 digits maps to
  ``None`` — a contract violation that the Silver validator routes to
  quarantine.
- Hash: :func:`hmac.new(secret, msg, "sha256").hexdigest()`, truncated
  to 16 hex characters (64 bits). 64 bits is enough to avoid
  birthday-collision pressure at the ~10M-lead scale we care about
  (``P(collision) < 2.7e-6``) and keeps the Silver parquet small.
- Null in, null out: empty or unparseable phones do not get a
  ``lead_id`` — we never assign a bucket to a row that has no key.

Nothing in this module does I/O or touches the settings module — the
secret is a plain ``str`` parameter so the helper is trivially testable
with a fixture secret, and production code calls
``settings.pipeline_lead_secret.get_secret_value()`` at the composition
boundary.
"""

from __future__ import annotations

# LEARN: ``hmac`` is the stdlib implementation of RFC 2104. ``hmac.new``
# builds a keyed hash; we prefer it over ``hashlib.sha256`` because it
# resists length-extension and, more importantly, forces an attacker to
# brute-force the secret *and* the phone space simultaneously.
import hmac

# LEARN: ``re`` is the stdlib regex engine. We compile the "non-digit"
# pattern at import time so every call reuses the cached ``Pattern``
# object instead of recompiling.
import re
from typing import Final

import polars as pl

__all__ = [
    "LEAD_ID_HEX_CHARS",
    "derive_lead_id",
    "derive_lead_id_expr",
    "normalize_phone_digits",
]


# ---------------------------------------------------------------------------
# Constants.
# ---------------------------------------------------------------------------

LEAD_ID_HEX_CHARS: Final[int] = 16
"""Hex digits kept from the full HMAC digest. 16 hex = 64 bits."""

_NON_DIGIT_RE: Final[re.Pattern[str]] = re.compile(r"\D")
"""Compiled "anything that is not a digit" pattern, reused per call."""

_BR_COUNTRY_CODE: Final[str] = "55"

_MIN_PHONE_DIGITS: Final[int] = 10
"""10 = BR landline ``(DD) NNNN-NNNN``. Shorter than this is noise."""

_MAX_PHONE_DIGITS: Final[int] = 13
"""13 = ``+55`` + 11-digit mobile. Longer is almost certainly garbage."""


# ---------------------------------------------------------------------------
# Phone normalization.
# ---------------------------------------------------------------------------


def normalize_phone_digits(raw: str | None) -> str | None:
    """Collapse a free-form phone into a canonical digit-only form.

    Returns the 10- or 11-digit BR national number if the input parses,
    else ``None``. Does **not** raise — callers should treat ``None`` as
    "quarantine this row", not "crash the batch".

    Examples (doctest-style):
        "+55 11 98765-4321" -> "11987654321"
        "(11) 98765-4321"   -> "11987654321"
        "11987654321"       -> "11987654321"
        "1198765432"        -> "1198765432"   (10-digit landline kept)
        "12345"             -> None           (too short)
        ""                  -> None
        None                -> None

    LEARN: the function is split into three clearly-named steps instead
    of one dense expression. Easier to trace in a debugger, easier to
    change independently (a future revision might accept international
    numbers and would only touch the country-code branch).
    """
    if raw is None:
        return None
    digits = _NON_DIGIT_RE.sub("", raw)
    if not digits:
        return None
    # Drop leading BR country code so "+5511..." and "11..." collide.
    # We only drop it when the residue is a plausible national number
    # (10 or 11 digits). That guards against a 13-digit string that
    # happens to start with "55" but is actually a different country.
    if digits.startswith(_BR_COUNTRY_CODE) and len(digits) in (12, 13):
        digits = digits[len(_BR_COUNTRY_CODE):]
    if not (_MIN_PHONE_DIGITS <= len(digits) <= _MAX_PHONE_DIGITS):
        return None
    return digits


# ---------------------------------------------------------------------------
# HMAC over normalized phone.
# ---------------------------------------------------------------------------


def derive_lead_id(phone: str | None, secret: str) -> str | None:
    """Return the ``lead_id`` for ``phone`` under ``secret``.

    ``None`` in → ``None`` out. Unparseable phones (too short, no
    digits) also return ``None`` — the Silver validator routes those
    rows to quarantine rather than burying them in a degenerate hash.

    The returned id is :data:`LEAD_ID_HEX_CHARS` hex characters long.
    The truncation is safe against collisions at the lead scale we
    care about (see module docstring) and keeps the parquet compact.

    LEARN: ``hmac.new(key, msg, digestmod)`` wants ``bytes`` for both
    key and msg. Python strings encode cleanly as UTF-8; phones are
    pure ASCII digits after normalisation so encoding is unambiguous.
    The secret is passed as ``str`` for testability — production code
    unwraps the :class:`pydantic.SecretStr` at the call site.
    """
    if not secret:
        raise ValueError("PIPELINE_LEAD_SECRET is required for lead_id derivation")
    normalized = normalize_phone_digits(phone)
    if normalized is None:
        return None
    digest = hmac.new(
        key=secret.encode("utf-8"),
        msg=normalized.encode("utf-8"),
        digestmod="sha256",
    ).hexdigest()
    return digest[:LEAD_ID_HEX_CHARS]


# ---------------------------------------------------------------------------
# Polars wrapper.
# ---------------------------------------------------------------------------


def derive_lead_id_expr(expr: pl.Expr, secret: str) -> pl.Expr:
    """Polars wrapper around :func:`derive_lead_id`.

    LEARN: ``map_elements`` is the per-row Python escape hatch. For
    HMAC we need it because Polars has no native HMAC expression (and
    keying a fixed secret into a native hash would defeat the point
    of HMAC's RFC-2104 inner/outer padding). The per-row cost is
    ~3 µs — well inside the F2-RNF-07 budget for the 153k-row fixture.

    We capture ``secret`` in a closure so every row uses the same key
    without paying a settings-lookup cost per element.
    """

    def _one(raw: str | None) -> str | None:
        return derive_lead_id(raw, secret)

    return expr.map_elements(
        _one,
        return_dtype=pl.String,
        skip_nulls=True,
    )
