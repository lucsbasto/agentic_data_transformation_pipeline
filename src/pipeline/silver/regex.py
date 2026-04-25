"""Override-aware regex lookup for the Silver layer (F4 design §17 O2).

The agent's ``regex_break`` fix (F4.6) writes LLM-regenerated patterns
to ``state/regex_overrides.json`` keyed by ``batch_id`` + pattern name.
Silver code that wants to honor those overrides imports
:func:`load_override` and falls back to its hard-coded default when
the helper returns ``None``.

This module is intentionally read-only — writes are owned by
``pipeline.agent.fixes.regex_break``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Final

DEFAULT_OVERRIDES_PATH: Final[Path] = Path("state/regex_overrides.json")


def load_override(
    batch_id: str,
    pattern_name: str,
    *,
    overrides_path: Path = DEFAULT_OVERRIDES_PATH,
) -> str | None:
    """Return the override regex for ``(batch_id, pattern_name)`` or
    ``None`` when no override exists / the JSON cannot be read.

    The JSON shape matches the writer in
    :mod:`pipeline.agent.fixes.regex_break`::

        {
          "<batch_id>": {
            "<pattern_name>": "<regex_string>"
          }
        }

    Missing-file / parse-error / wrong-shape all collapse to
    ``None`` — the caller is expected to fall back to its compiled
    default, never to crash on a malformed override store.
    """
    try:
        text = overrides_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    batch_entry = data.get(batch_id)
    if not isinstance(batch_entry, dict):
        return None
    value = batch_entry.get(pattern_name)
    return value if isinstance(value, str) else None
