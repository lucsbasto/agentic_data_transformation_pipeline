"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def project_root() -> Path:
    """Absolute path to the repo root (two levels above this file)."""
    return Path(__file__).resolve().parent.parent
