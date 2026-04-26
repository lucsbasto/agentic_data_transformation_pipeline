"""Project path helpers.

Resolves the repository root by walking up from the current file until a
marker (``pyproject.toml``) is found. Using a marker makes the resolution
robust to install layouts (editable vs wheel vs zipapp).
"""

from __future__ import annotations

# LEARN: ``pathlib.Path`` is the modern, object-oriented way to handle
# filesystem paths. Operators like ``/`` join segments safely across
# Windows/Linux/macOS. Prefer ``Path`` over ``os.path`` in new code — the
# repo's ruff config (``PTH`` rules) even enforces that.
from pathlib import Path

# LEARN: a single leading underscore marks a name as "module-private by
# convention". Python does not hide it, but other code should not import
# it. ``_MARKER`` is a constant — the file we search for to locate the
# repo root. Keeping it in one place means a rename only touches this
# module, not every caller.
_MARKER = "pyproject.toml"


def project_root() -> Path:
    """Return the repo root (directory containing ``pyproject.toml``)."""
    # LEARN: ``Path(__file__)`` turns this file's path into a ``Path``
    # object. ``.resolve()`` expands it to an absolute, symlink-free
    # path, so the walk-up below works from any install layout.
    here = Path(__file__).resolve()
    # LEARN: ``here.parents`` is an iterator over *ancestor* directories
    # (skipping ``here`` itself). The ``(here, *here.parents)`` tuple
    # prepends ``here`` so the loop also checks the file's own directory
    # before moving up. ``*`` inside a tuple literal is "unpack the
    # sequence into this position" — a handy Python 3 feature.
    for parent in (here, *here.parents):
        # LEARN: ``parent / _MARKER`` uses ``Path.__truediv__`` to build
        # a child path. ``.is_file()`` returns True iff that child is a
        # regular file (not a directory, not missing, not a symlink to
        # nothing). First hit wins — we return the closest root.
        if (parent / _MARKER).is_file():
            return parent
    # LEARN: if no marker is found we refuse to guess. ``RuntimeError``
    # is Python's generic "the program reached an impossible state"
    # exception. The f-string (``f"..."``) interpolates ``{here}`` at
    # runtime so the error message shows the exact search root.
    raise RuntimeError(
        f"project root not found: no '{_MARKER}' in {here} or its parents"
    )


# LEARN: the following helpers are tiny because they encode *convention*.
# Centralising "where does Bronze live?" in one function means a future
# refactor that moves the data directory is a one-line change. Every
# caller asks ``data_bronze_dir()`` rather than hard-coding the string.


def data_raw_dir() -> Path:
    """Return the raw source drop zone (``data/raw/``)."""
    return project_root() / "data" / "raw"


def data_bronze_dir() -> Path:
    """Return the Bronze landing directory (``data/bronze/``)."""
    return project_root() / "data" / "bronze"


def data_silver_dir() -> Path:
    """Return the Silver output directory (``data/silver/``)."""
    return project_root() / "data" / "silver"


def data_gold_dir() -> Path:
    """Return the Gold output directory (``data/gold/``)."""
    return project_root() / "data" / "gold"


def state_dir() -> Path:
    """Return the state directory that holds ``manifest.db`` and lock files."""
    return project_root() / "state"
