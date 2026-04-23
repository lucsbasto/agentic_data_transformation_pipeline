"""Project path helpers.

Resolves the repository root by walking up from the current file until a
marker (``pyproject.toml``) is found. Using a marker makes the resolution
robust to install layouts (editable vs wheel vs zipapp).
"""

from __future__ import annotations

from pathlib import Path

_MARKER = "pyproject.toml"


def project_root() -> Path:
    """Return the repo root (directory containing ``pyproject.toml``)."""
    here = Path(__file__).resolve()
    for parent in (here, *here.parents):
        if (parent / _MARKER).is_file():
            return parent
    raise RuntimeError(
        f"project root not found: no '{_MARKER}' in {here} or its parents"
    )


def data_raw_dir() -> Path:
    return project_root() / "data" / "raw"


def data_bronze_dir() -> Path:
    return project_root() / "data" / "bronze"


def data_silver_dir() -> Path:
    return project_root() / "data" / "silver"


def data_gold_dir() -> Path:
    return project_root() / "data" / "gold"


def state_dir() -> Path:
    return project_root() / "state"
