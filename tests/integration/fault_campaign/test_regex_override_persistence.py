"""FIC.21 — Regex override survives agent restart.

Process 1 triggers regex_break recovery; ``save_override`` writes
``state/regex_overrides.json``.  Process 2 opens a fresh connection
and loads the overrides from disk.

WIRING-3 gap: ``git log | grep -i wiring-3`` returned nothing, so
WIRING-3 has NOT shipped.  F2 PII regex callers do not yet consult
``load_override``; we assert the *file* was written and ``load_overrides``
returns the persisted entry, but we do NOT assert that process 2 skips
the LLM call (the re-classification would still invoke the LLM since
the Silver layer doesn't read the override yet).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.agent.fixes.regex_break import load_overrides, save_override

from .conftest import FakeLLMClient


@pytest.mark.fault_campaign
def test_regex_override_written_and_persisted(tmp_path: Path) -> None:
    """Process 1 writes override; a fresh load_overrides call returns it."""
    overrides_path = tmp_path / "state" / "regex_overrides.json"
    batch_id = "batch_fic21_001"
    pattern_name = "phone_br"
    regex_value = r"\+55\d{10,11}"

    # Process 1: write the override (simulates the fix apply step).
    save_override(
        batch_id=batch_id,
        pattern_name=pattern_name,
        regex=regex_value,
        path=overrides_path,
    )

    assert overrides_path.exists(), "regex_overrides.json must exist after save_override"

    # Verify no .tmp leftover.
    tmp_files = list(overrides_path.parent.glob("*.tmp"))
    assert tmp_files == [], f"unexpected .tmp files: {tmp_files}"

    # Process 2: fresh load — must return the persisted entry.
    loaded = load_overrides(overrides_path)
    assert batch_id in loaded, (
        f"batch_id {batch_id!r} not found in persisted overrides: {loaded}"
    )
    assert loaded[batch_id].get(pattern_name) == regex_value, (
        f"persisted regex mismatch: {loaded[batch_id]}"
    )


@pytest.mark.fault_campaign
def test_wiring3_gap_documented(tmp_path: Path) -> None:
    """Documents the WIRING-3 gap: overrides on disk are NOT consulted by
    Silver regex callers because WIRING-3 has not shipped.

    We confirm load_overrides returns the data correctly (the storage
    layer works), but we cannot assert a second agent run skips the LLM
    call — that requires WIRING-3 (F2 callers consulting load_override).
    """
    overrides_path = tmp_path / "state" / "regex_overrides.json"
    batch_id = "batch_fic21_002"
    pattern_name = "cpf"
    regex_value = r"\d{3}\.\d{3}\.\d{3}-\d{2}"

    save_override(
        batch_id=batch_id,
        pattern_name=pattern_name,
        regex=regex_value,
        path=overrides_path,
    )

    loaded = load_overrides(overrides_path)

    # Storage layer works correctly.
    assert loaded == {batch_id: {pattern_name: regex_value}}

    # WIRING-3 gap: we CANNOT assert FakeLLMClient.calls == 0 for a second run
    # because F2 Silver callers don't consult load_override yet.
    # This test documents the gap rather than asserting absent behavior.
    # When WIRING-3 ships, add: assert fake_llm.calls == [] after second run.
    fake_llm = FakeLLMClient()
    assert fake_llm.calls == [], "no LLM calls should happen in this gap-documentation test"
