"""PII maskers for the Silver layer.

LGPD asks for two things that are in tension:

1. *Do not keep the personal data in analytical files* — raw email,
   CPF, phone, CEP, licence plate must not survive to Silver.
2. *Keep the dimensions that make the data analytically useful* — e.g.,
   a marketing team still wants to know "most leads use Gmail" even
   though they no longer know the address.

The functions in this module apply *positional masking* to resolve the
tension: every match is replaced with a fixed-structure token that
keeps shape, separators, and (in the case of email) the first character
of the local part / host and the full TLD. That is enough to keep
downstream analytics useful (provider bucketing, TLD distribution,
format-class counts) without keeping a single identifying digit or
letter that an attacker could use.

Everything here is a pure function: ``in -> out``, no I/O, no
side-effects. The Polars wrapper lives in ``silver/transform.py``.

Calling :func:`mask_all_pii` on a string returns the masked string **and**
a counts dict. The counts are emitted as structured log fields by the
Silver orchestrator so operators can spot regressions (e.g., "yesterday
we masked 5000 phones, today we masked 0 — probably a regex change").
"""

from __future__ import annotations

# LEARN: ``re`` is the standard-library regex engine. We compile every
# pattern once at import time — compilation is expensive, matching is
# cheap — and reuse the compiled objects on every call.
import re

# LEARN: ``TypedDict`` is a Python typing gadget that types a dict by
# its KEYS, not by letting the values vary freely. Mypy will complain
# if we return ``{"email": 1}`` instead of ``{"emails": 1}``. Keeps the
# logging contract honest.
from typing import Final, TypedDict

# ---------------------------------------------------------------------------
# Compiled regexes.
# ---------------------------------------------------------------------------
# LEARN: every pattern is anchored with word boundaries (``\b``) or
# explicit digit look-around so a match cannot spill into an adjacent
# token. Example: without ``\b``, ``123.456.789-01abc`` would still be
# treated as a CPF; with ``\b`` it is left alone.

# Email: matches ``local@host.tld`` conservatively. Host can be
# multi-level (``foo.bar.example.com``) because the outer replacement
# only cares about the first host label and the trailing TLD.
_EMAIL_RE: Final[re.Pattern[str]] = re.compile(
    r"\b([A-Za-z0-9._%+\-]+)@([A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*)\.([A-Za-z]{2,})\b"
)

# CPF (formatted): ``123.456.789-01`` or ``123.456.78901``. We require
# at least the two dots so an unformatted 11-digit run (which is far
# more likely to be a mobile phone in WhatsApp text) is not eaten by
# the CPF masker. Unformatted CPFs are rare in real conversation data —
# if an operator needs stricter detection later, they can widen the
# regex and re-process Silver.
_CPF_RE: Final[re.Pattern[str]] = re.compile(
    r"\b\d{3}\.\d{3}\.\d{3}-?\d{2}\b"
)

# CEP (Brazilian postal code, 8 digits): ``01310-930`` or ``01310930``.
_CEP_RE: Final[re.Pattern[str]] = re.compile(r"\b\d{5}-?\d{3}\b")

# Phone (Brazil): covers ``+55 (11) 98765-4321``, ``55 11 98765 4321``,
# ``11987654321``, ``(11) 8765-4321`` etc. Digit look-around keeps us
# from eating longer numeric strings.
_PHONE_RE: Final[re.Pattern[str]] = re.compile(
    r"(?<!\d)"                      # no digit immediately before
    r"(?:\+?55\s?-?)?"              # optional country code "+55"
    r"\(?\d{2}\)?\s?-?"             # DDD, maybe in parens, maybe a separator
    r"9?\d{4}[\s\-]?\d{4}"          # 8- or 9-digit subscriber
    r"(?!\d)"                       # no digit immediately after
)

# Licence plate — Mercosul (``ABC1D23``).
_PLATE_MERCOSUL_RE: Final[re.Pattern[str]] = re.compile(
    r"\b([A-Z]{3})(\d)([A-Z])(\d{2})\b"
)

# Licence plate — pre-Mercosul (``ABC-1234`` or ``ABC1234``).
_PLATE_OLD_RE: Final[re.Pattern[str]] = re.compile(
    r"\b([A-Z]{3})-?(\d{4})\b"
)


# ---------------------------------------------------------------------------
# Masker callables. Each one consumes a ``re.Match`` and returns the
# replacement string. ``re.Pattern.sub`` accepts a callable — this is
# how we keep the positional logic in Python instead of cramming it
# into a back-reference string.
# ---------------------------------------------------------------------------


def _mask_email(match: re.Match[str]) -> str:
    """Mask ``a****@g****.com`` — keep first letter of local + host; full TLD."""
    local, host, tld = match.group(1), match.group(2), match.group(3)
    # LEARN: ``max(len - 1, 4)`` guarantees at least four ``*`` even for
    # very short local parts. Without the floor, ``a@b.com`` would show
    # up as ``a@b.com`` post-mask (zero stars — literally the same).
    local_stars = "*" * max(len(local) - 1, 4)
    host_head = host.split(".", 1)[0]  # first label only — multi-level hosts still collapse cleanly
    host_stars = "*" * max(len(host_head) - 1, 4)
    return f"{local[0]}{local_stars}@{host_head[0]}{host_stars}.{tld}"


def _mask_cpf(_match: re.Match[str]) -> str:
    """Mask CPF to the fixed template ``***.***.***-**``.

    The CPF value itself has no analytical dimension we want to keep
    (every Brazilian citizen has one; the digits are not categorical),
    so a constant mask is the right trade-off.
    """
    return "***.***.***-**"


def _mask_cep(_match: re.Match[str]) -> str:
    """Mask CEP to the fixed template ``*****-***``."""
    return "*****-***"


_PHONE_TAIL_KEPT_DIGITS: Final[int] = 2
"""How many trailing digits of a phone number survive the mask."""


def _mask_phone(match: re.Match[str]) -> str:
    """Mask to ``+55 (**) *****-**<last2>`` — keeps last 2 digits only."""
    # LEARN: ``re.sub(r"\D", "", s)`` deletes everything that is not a
    # digit. Normalises the match so ``(11) 98765-4321`` and
    # ``11987654321`` both end up as ``11987654321`` for counting.
    digits = re.sub(r"\D", "", match.group(0))
    last_two = (
        digits[-_PHONE_TAIL_KEPT_DIGITS:]
        if len(digits) >= _PHONE_TAIL_KEPT_DIGITS
        else "**"
    )
    return f"+55 (**) *****-**{last_two}"


def _mask_plate_mercosul(match: re.Match[str]) -> str:
    """Mask Mercosul plate ``XYZ1D23`` -> ``***1D**``.

    Positions 4 (digit) and 5 (letter) are the *format discriminators*
    between Mercosul and pre-Mercosul plates — keeping them preserves
    the format dimension without leaking any identifying character.
    """
    _, digit, letter, _ = match.group(1), match.group(2), match.group(3), match.group(4)
    return f"***{digit}{letter}**"


def _mask_plate_old(match: re.Match[str]) -> str:
    """Mask pre-Mercosul plate ``XYZ-1234`` -> ``***-**<last2>``."""
    _, num = match.group(1), match.group(2)
    return f"***-**{num[-2:]}"


# ---------------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------------


class PIICounts(TypedDict):
    """How many matches each masker rewrote. Emitted as log fields."""

    emails: int
    cpfs: int
    ceps: int
    phones: int
    plates: int


def empty_counts() -> PIICounts:
    """Return a fresh zeroed counts dict. Used by callers aggregating
    across many rows, e.g. summing into structured log fields after a
    Polars ``map_batches`` pass.
    """
    return PIICounts(emails=0, cpfs=0, ceps=0, phones=0, plates=0)


def mask_all_pii(text: str) -> tuple[str, PIICounts]:
    """Apply every masker in a safe order and return ``(masked, counts)``.

    Order matters — we run the digit-heavy *fixed-template* patterns
    (CPF, CEP) before the generic phone pattern so an 11-digit CPF is
    never mistaken for a phone. Plates come last because their letter-
    led shape cannot collide with the others.
    """
    counts = empty_counts()
    text, n = _EMAIL_RE.subn(_mask_email, text)
    counts["emails"] = n
    text, n = _CPF_RE.subn(_mask_cpf, text)
    counts["cpfs"] = n
    text, n = _CEP_RE.subn(_mask_cep, text)
    counts["ceps"] = n
    text, n = _PHONE_RE.subn(_mask_phone, text)
    counts["phones"] = n
    text, n_mer = _PLATE_MERCOSUL_RE.subn(_mask_plate_mercosul, text)
    text, n_old = _PLATE_OLD_RE.subn(_mask_plate_old, text)
    counts["plates"] = n_mer + n_old
    return text, counts


def mask_phone_only(text: str) -> str:
    """Mask every phone-shaped substring in ``text`` and nothing else.

    Used by the Silver orchestrator to redact the ``sender_phone``
    column into ``sender_phone_masked`` without touching anything else.
    Keeping it separate from :func:`mask_all_pii` avoids paying the cost
    of the other regexes on a column we know only holds a phone number.
    """
    return _PHONE_RE.sub(_mask_phone, text)
