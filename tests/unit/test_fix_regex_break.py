"""Coverage for the F4 ``regex_break`` fix (F4.6).

The fix has three moving parts: LLM regeneration, baseline
validation, and override persistence. Each is tested in isolation
and once end-to-end via :func:`build_fix`. The LLM is faked so no
network calls fire."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from pipeline.agent.fixes.regex_break import (
    PROMPT_VERSION_REGEX_BREAK,
    REGEX_BREAK_SYSTEM_PROMPT,
    RegexBreakFixError,
    build_fix,
    load_overrides,
    regenerate_regex,
    save_override,
    validate_regex,
)

# ---------------------------------------------------------------------------
# Fake LLM client.
# ---------------------------------------------------------------------------


@dataclass
class _FakeResponse:
    text: str


class _FakeLLM:
    def __init__(self, replies: list[str]) -> None:
        self._replies = list(replies)
        self.calls: list[dict[str, Any]] = []

    def cached_call(self, **kwargs: Any) -> _FakeResponse:
        self.calls.append(kwargs)
        if not self._replies:
            raise AssertionError("FakeLLM ran out of scripted replies")
        return _FakeResponse(text=self._replies.pop(0))


# ---------------------------------------------------------------------------
# regenerate_regex.
# ---------------------------------------------------------------------------


def test_regenerate_regex_returns_pattern_from_llm() -> None:
    fake = _FakeLLM(replies=[json.dumps({"regex": r"R\$\s?\d+(?:[\.,]\d+)*"})])
    pattern = regenerate_regex(
        pattern_name="brl_amount",
        samples=["R$ 1.500,00", "R$10"],
        llm_client=fake,
    )
    assert pattern == r"R\$\s?\d+(?:[\.,]\d+)*"
    call = fake.calls[0]
    assert call["system"] == REGEX_BREAK_SYSTEM_PROMPT
    assert '<samples untrusted="true">' in call["user"]
    assert "brl_amount" in call["user"]
    assert call["temperature"] == 0.0


def test_regenerate_regex_raises_when_reply_unparseable() -> None:
    fake = _FakeLLM(replies=["definitely not json"])
    with pytest.raises(RegexBreakFixError, match="unparseable reply"):
        regenerate_regex(
            pattern_name="x",
            samples=["sample"],
            llm_client=fake,
        )


def test_regenerate_regex_raises_when_reply_lacks_regex_field() -> None:
    fake = _FakeLLM(replies=[json.dumps({"answer": "yes"})])
    with pytest.raises(RegexBreakFixError, match="unparseable reply"):
        regenerate_regex(
            pattern_name="x",
            samples=["sample"],
            llm_client=fake,
        )


def test_regenerate_regex_raises_when_pattern_does_not_compile() -> None:
    fake = _FakeLLM(replies=[json.dumps({"regex": "[unbalanced"})])
    with pytest.raises(RegexBreakFixError, match="does not compile"):
        regenerate_regex(
            pattern_name="x",
            samples=["sample"],
            llm_client=fake,
        )


def test_regenerate_regex_raises_when_samples_empty() -> None:
    fake = _FakeLLM(replies=[])
    with pytest.raises(RegexBreakFixError, match="requires at least one sample"):
        regenerate_regex(pattern_name="x", samples=[], llm_client=fake)
    assert fake.calls == []


# ---------------------------------------------------------------------------
# validate_regex.
# ---------------------------------------------------------------------------


def test_validate_regex_returns_true_when_all_samples_match() -> None:
    assert validate_regex(r"\d+", ["123", "abc456"]) is True


def test_validate_regex_returns_false_on_any_baseline_miss() -> None:
    assert validate_regex(r"\d+", ["123", "no-digits-here"]) is False


def test_validate_regex_returns_true_for_empty_baseline() -> None:
    """No baseline -> no regression bar -> trivially valid."""
    assert validate_regex(r".*", []) is True


def test_validate_regex_returns_false_when_pattern_does_not_compile() -> None:
    assert validate_regex("[unbalanced", ["x"]) is False


# ---------------------------------------------------------------------------
# Override store I/O.
# ---------------------------------------------------------------------------


def test_save_override_creates_json_file_atomically(tmp_path: Path) -> None:
    path = tmp_path / "state" / "regex_overrides.json"
    save_override(batch_id="bid01", pattern_name="phone", regex=r"\d{8}", path=path)
    assert path.exists()
    assert load_overrides(path) == {"bid01": {"phone": r"\d{8}"}}
    # No leftover .tmp sidecar
    assert list(path.parent.glob("*.tmp")) == []


def test_save_override_merges_with_existing_entries(tmp_path: Path) -> None:
    path = tmp_path / "state" / "regex_overrides.json"
    save_override(batch_id="bid01", pattern_name="phone", regex=r"\d{8}", path=path)
    save_override(batch_id="bid01", pattern_name="email", regex=r".+@.+", path=path)
    save_override(batch_id="bid02", pattern_name="phone", regex=r"\d{9}", path=path)
    overrides = load_overrides(path)
    assert overrides == {
        "bid01": {"phone": r"\d{8}", "email": r".+@.+"},
        "bid02": {"phone": r"\d{9}"},
    }


def test_save_override_overwrites_existing_pattern(tmp_path: Path) -> None:
    """Re-running the fix on the same (batch_id, pattern_name) must
    replace the previous attempt's regex, not stack a list."""
    path = tmp_path / "state" / "regex_overrides.json"
    save_override(batch_id="bid01", pattern_name="phone", regex=r"old", path=path)
    save_override(batch_id="bid01", pattern_name="phone", regex=r"new", path=path)
    overrides = load_overrides(path)
    assert overrides == {"bid01": {"phone": "new"}}


def test_load_overrides_returns_empty_dict_when_file_missing(tmp_path: Path) -> None:
    assert load_overrides(tmp_path / "missing.json") == {}


def test_load_overrides_collapses_malformed_json(tmp_path: Path) -> None:
    path = tmp_path / "regex_overrides.json"
    path.write_text("not-json", encoding="utf-8")
    assert load_overrides(path) == {}


def test_load_overrides_drops_non_string_entries(tmp_path: Path) -> None:
    """Defensive: a hand-edited file with numeric values should not
    propagate non-string regex values into the rest of the system."""
    path = tmp_path / "regex_overrides.json"
    path.write_text(
        json.dumps({"bid01": {"phone": 123, "email": ".+@.+"}}),
        encoding="utf-8",
    )
    assert load_overrides(path) == {"bid01": {"email": ".+@.+"}}


# ---------------------------------------------------------------------------
# build_fix end-to-end.
# ---------------------------------------------------------------------------


def test_build_fix_apply_persists_validated_override(tmp_path: Path) -> None:
    overrides_path = tmp_path / "state" / "regex_overrides.json"
    fake = _FakeLLM(replies=[json.dumps({"regex": r"\d{4}"})])
    fix = build_fix(
        batch_id="bid01",
        pattern_name="postal_code",
        samples=["1234", "5678"],
        baseline_samples=["9999"],
        llm_client=fake,
        overrides_path=overrides_path,
    )
    assert fix.kind == "regenerate_regex"
    assert fix.requires_llm is True
    fix.apply()
    overrides = load_overrides(overrides_path)
    assert overrides == {"bid01": {"postal_code": r"\d{4}"}}
    # Override is itself a valid regex matching the baseline.
    assert re.search(overrides["bid01"]["postal_code"], "9999") is not None


def test_build_fix_apply_raises_on_baseline_regression(tmp_path: Path) -> None:
    """LLM proposes a regex that fails the baseline; fix must raise
    AND must not persist the rejected pattern."""
    overrides_path = tmp_path / "state" / "regex_overrides.json"
    fake = _FakeLLM(replies=[json.dumps({"regex": r"^foo$"})])
    fix = build_fix(
        batch_id="bid01",
        pattern_name="postal_code",
        samples=["1234"],
        baseline_samples=["9999"],
        llm_client=fake,
        overrides_path=overrides_path,
    )
    with pytest.raises(RegexBreakFixError, match="regressed the baseline"):
        fix.apply()
    assert load_overrides(overrides_path) == {}


def test_build_fix_apply_returns_none(tmp_path: Path) -> None:
    """`Fix.apply` is typed as `Callable[[], None]` — wrapper must
    not leak any internal value."""
    overrides_path = tmp_path / "state" / "regex_overrides.json"
    fake = _FakeLLM(replies=[json.dumps({"regex": r"\d+"})])
    fix = build_fix(
        batch_id="bid01",
        pattern_name="x",
        samples=["1"],
        baseline_samples=[],
        llm_client=fake,
        overrides_path=overrides_path,
    )
    assert fix.apply() is None


# ---------------------------------------------------------------------------
# Prompt invariants.
# ---------------------------------------------------------------------------


def test_system_prompt_embeds_prompt_version() -> None:
    assert (
        f"PROMPT_VERSION_REGEX_BREAK={PROMPT_VERSION_REGEX_BREAK}"
        in REGEX_BREAK_SYSTEM_PROMPT
    )


def test_system_prompt_warns_against_prompt_injection() -> None:
    assert "prompt injection" in REGEX_BREAK_SYSTEM_PROMPT.casefold()
    assert '<samples untrusted="true">' in REGEX_BREAK_SYSTEM_PROMPT
