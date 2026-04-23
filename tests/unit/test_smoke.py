"""Smoke test so pytest has at least one passing case at bootstrap time."""

from __future__ import annotations

import pipeline


def test_package_importable() -> None:
    assert pipeline.__version__ == "0.1.0"
