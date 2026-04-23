"""Tests for ``pipeline.paths``."""

from __future__ import annotations

from pipeline.paths import (
    data_bronze_dir,
    data_gold_dir,
    data_raw_dir,
    data_silver_dir,
    project_root,
    state_dir,
)


def test_project_root_has_pyproject() -> None:
    root = project_root()
    assert (root / "pyproject.toml").is_file()


def test_layer_dirs_under_project_root() -> None:
    root = project_root()
    assert data_raw_dir() == root / "data" / "raw"
    assert data_bronze_dir() == root / "data" / "bronze"
    assert data_silver_dir() == root / "data" / "silver"
    assert data_gold_dir() == root / "data" / "gold"
    assert state_dir() == root / "state"
