"""Coverage for the read-only override loader (F4.6).

These tests pin the contract that the agent's ``regex_break`` fix
writes a JSON file Silver can later consume. Every malformed shape
must collapse to ``None`` so a torn / corrupt overrides file never
crashes the Silver path."""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.silver.regex import DEFAULT_OVERRIDES_PATH, load_override


def test_load_override_returns_none_when_file_missing(tmp_path: Path) -> None:
    assert load_override("bid01", "phone", overrides_path=tmp_path / "missing.json") is None


def test_load_override_returns_string_when_present(tmp_path: Path) -> None:
    path = tmp_path / "regex_overrides.json"
    path.write_text(
        json.dumps({"bid01": {"phone": r"\d{2}-\d{8}"}}), encoding="utf-8"
    )
    assert load_override("bid01", "phone", overrides_path=path) == r"\d{2}-\d{8}"


def test_load_override_returns_none_when_batch_id_unknown(tmp_path: Path) -> None:
    path = tmp_path / "regex_overrides.json"
    path.write_text(json.dumps({"bid02": {"phone": "x"}}), encoding="utf-8")
    assert load_override("bid01", "phone", overrides_path=path) is None


def test_load_override_returns_none_when_pattern_name_unknown(tmp_path: Path) -> None:
    path = tmp_path / "regex_overrides.json"
    path.write_text(json.dumps({"bid01": {"email": "x"}}), encoding="utf-8")
    assert load_override("bid01", "phone", overrides_path=path) is None


def test_load_override_collapses_malformed_json_to_none(tmp_path: Path) -> None:
    path = tmp_path / "regex_overrides.json"
    path.write_text("not-json", encoding="utf-8")
    assert load_override("bid01", "phone", overrides_path=path) is None


def test_load_override_collapses_wrong_top_level_shape_to_none(tmp_path: Path) -> None:
    path = tmp_path / "regex_overrides.json"
    path.write_text(json.dumps(["array", "instead", "of", "dict"]), encoding="utf-8")
    assert load_override("bid01", "phone", overrides_path=path) is None


def test_load_override_collapses_non_string_value_to_none(tmp_path: Path) -> None:
    """A numeric value where a regex string is expected — refuse rather
    than try to coerce."""
    path = tmp_path / "regex_overrides.json"
    path.write_text(json.dumps({"bid01": {"phone": 42}}), encoding="utf-8")
    assert load_override("bid01", "phone", overrides_path=path) is None


def test_default_overrides_path_matches_design() -> None:
    assert Path("state/regex_overrides.json") == DEFAULT_OVERRIDES_PATH
