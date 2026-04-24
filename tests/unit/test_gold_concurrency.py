"""Tests for the F3.10 persona concurrency lane (``gold/concurrency.py``).

The three F3.10 regressions we lock in:

1. ``pipeline_llm_concurrency`` caps the number of in-flight tasks.
   With a ceiling of 3 and 10 aggregates waiting to classify, at
   most 3 workers can be inside ``cached_call`` at once.
2. Cache hits refund the budget so PRD §18.4's "5000 max calls"
   counts cache *misses* only.
3. Once the budget is spent, the remaining aggregates short-circuit
   to ``PersonaResult.skipped()`` without dispatching a single extra
   provider call.

Workers instantiate their own ``LLMClient`` + ``LLMCache`` via
``_init_worker``, so the tests swap both names at the module level
with fakes that record what the real code would have paid for.
"""

from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeline.errors import ConfigError
from pipeline.gold import concurrency as mod
from pipeline.gold.persona import LeadAggregate
from pipeline.llm.client import LLMResponse
from pipeline.settings import Settings

_BASE_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Settings / aggregate helpers.
# ---------------------------------------------------------------------------


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Clear pipeline env vars and chdir to a tmp dir so .env is not loaded."""
    for key in [
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_BASE_URL",
        "LLM_MODEL_PRIMARY",
        "LLM_MODEL_FALLBACK",
        "PIPELINE_RETRY_BUDGET",
        "PIPELINE_LOOP_SLEEP_SECONDS",
        "PIPELINE_STATE_DB",
        "PIPELINE_LOG_LEVEL",
        "PIPELINE_LEAD_SECRET",
        "PIPELINE_LLM_MAX_CALLS_PER_BATCH",
        "PIPELINE_LLM_CONCURRENCY",
    ]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _load_settings(monkeypatch: pytest.MonkeyPatch, *, concurrency: int = 3) -> Settings:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PIPELINE_LEAD_SECRET", "test-lead-secret-0123456789abcdef")
    monkeypatch.setenv("PIPELINE_LLM_CONCURRENCY", str(concurrency))
    try:
        return Settings.load()
    except ConfigError as exc:  # pragma: no cover - diagnostic only
        pytest.fail(f"Settings.load() failed in test fixture: {exc}")


def _agg(lead_id: str, *, num_msgs: int = 20) -> LeadAggregate:
    """Default aggregate shape: NO rule fires so classify_with_overrides
    falls through to the LLM path. ``num_msgs=20`` skips R2 and R1;
    ``forneceu_dado_pessoal=True`` skips R3; ``outcome='em_negociacao'``
    rules R2 out."""
    return LeadAggregate(
        lead_id=lead_id,
        num_msgs=num_msgs,
        num_msgs_inbound=num_msgs - 1,
        num_msgs_outbound=1,
        outcome="em_negociacao",
        mencionou_concorrente=False,
        competitor_count_distinct=0,
        forneceu_dado_pessoal=True,
        last_message_at=_BASE_TS,
        conversation_text="msg",
    )


# ---------------------------------------------------------------------------
# Fake LLM collaborators — swapped at module level so ``_init_worker``
# builds these on each worker thread instead of the real SDK clients.
# ---------------------------------------------------------------------------


class _FakeCache:
    """Stand-in for :class:`LLMCache`. Its only contract here is
    ``__init__(path)`` and ``.open()`` returning something truthy —
    the fake client never calls into it."""

    def __init__(self, _path: Path | str) -> None:
        self._path = _path

    def open(self) -> _FakeCache:
        return self


def _install_fakes(monkeypatch: pytest.MonkeyPatch, client_factory: Any) -> None:
    monkeypatch.setattr(mod, "LLMCache", _FakeCache)
    monkeypatch.setattr(mod, "LLMClient", client_factory)


def _response(text: str = "indeciso", *, cache_hit: bool = False) -> LLMResponse:
    return LLMResponse(
        text=text,
        model="stub",
        input_tokens=0,
        output_tokens=0,
        cache_hit=cache_hit,
    )


# ---------------------------------------------------------------------------
# 1. Semaphore caps the number of in-flight provider calls.
# ---------------------------------------------------------------------------


def test_semaphore_caps_in_flight_count(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """With ``pipeline_llm_concurrency=3`` and 10 leads to classify,
    no more than 3 provider calls are ever in-flight at once."""
    settings = _load_settings(monkeypatch, concurrency=3)

    state_lock = threading.Lock()
    observed = {"current": 0, "peak": 0}

    class _CountingClient:
        def __init__(self, _settings: Settings, _cache: _FakeCache) -> None:
            pass

        def cached_call(self, **_kwargs: Any) -> LLMResponse:
            with state_lock:
                observed["current"] += 1
                observed["peak"] = max(observed["peak"], observed["current"])
            # Hold the slot long enough for other threads to pile up.
            # Short enough to keep the test under a second.
            time.sleep(0.05)
            with state_lock:
                observed["current"] -= 1
            return _response()

    _install_fakes(monkeypatch, _CountingClient)

    aggregates = [_agg(f"L{i}") for i in range(10)]
    budget = mod._BudgetCounter(settings.pipeline_llm_max_calls_per_batch)

    results = asyncio.run(
        mod.classify_all(
            aggregates,
            settings=settings,
            db_path=tmp_path / "cache.db",
            budget=budget,
            batch_latest_timestamp=_BASE_TS,
        )
    )

    assert len(results) == 10
    assert observed["peak"] <= settings.pipeline_llm_concurrency
    # With 10 tasks and a ceiling of 3 we expect to hit the cap at
    # least once; otherwise the semaphore is effectively unbounded.
    assert observed["peak"] == settings.pipeline_llm_concurrency
    # Every result parsed cleanly; no budget skips on this path.
    assert all(r.persona_source == "llm" for r in results.values())


# ---------------------------------------------------------------------------
# 2. Cache hits refund the optimistic budget charge.
# ---------------------------------------------------------------------------


def test_cache_hits_refund_budget(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Every call is a cache hit → budget ends fully refunded.

    PRD §18.4's 5000-call cap is a cap on cache *misses*. This test
    locks that contract: five all-hit calls leave ``calls_made=0``
    and ``cache_hits=5``."""
    settings = _load_settings(monkeypatch, concurrency=2)

    class _AllHitClient:
        def __init__(self, _settings: Settings, _cache: _FakeCache) -> None:
            pass

        def cached_call(self, **_kwargs: Any) -> LLMResponse:
            return _response(cache_hit=True)

    _install_fakes(monkeypatch, _AllHitClient)

    aggregates = [_agg(f"L{i}") for i in range(5)]
    budget = mod._BudgetCounter(10)  # plenty of room — not the constraint

    results = asyncio.run(
        mod.classify_all(
            aggregates,
            settings=settings,
            db_path=tmp_path / "cache.db",
            budget=budget,
            batch_latest_timestamp=_BASE_TS,
        )
    )

    assert len(results) == 5
    assert all(r.persona_source == "llm" for r in results.values())
    assert budget.cache_hits == 5
    assert budget.calls_made == 0


# ---------------------------------------------------------------------------
# 3. Budget exhaustion short-circuits further provider work.
# ---------------------------------------------------------------------------


def test_budget_exhaustion_short_circuits_provider_work(
    clean_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Once the budget hits zero, remaining leads return
    ``PersonaResult.skipped()`` without dispatching more provider
    calls."""
    settings = _load_settings(monkeypatch, concurrency=1)
    # Serial execution (concurrency=1) keeps the order deterministic:
    # the first two aggregates charge, the next three find an empty
    # budget and skip. No race between charge and check.

    call_lock = threading.Lock()
    provider_calls = {"n": 0}

    class _CountingMissClient:
        def __init__(self, _settings: Settings, _cache: _FakeCache) -> None:
            pass

        def cached_call(self, **_kwargs: Any) -> LLMResponse:
            with call_lock:
                provider_calls["n"] += 1
            return _response(cache_hit=False)

    _install_fakes(monkeypatch, _CountingMissClient)

    aggregates = [_agg(f"L{i}") for i in range(5)]
    budget = mod._BudgetCounter(2)  # only two calls allowed

    results = asyncio.run(
        mod.classify_all(
            aggregates,
            settings=settings,
            db_path=tmp_path / "cache.db",
            budget=budget,
            batch_latest_timestamp=_BASE_TS,
        )
    )

    assert len(results) == 5

    skipped = [r for r in results.values() if r.persona_source == "skipped"]
    classified = [r for r in results.values() if r.persona_source == "llm"]

    assert len(skipped) == 3
    assert len(classified) == 2
    assert all(r.persona is None for r in skipped)
    # Provider only saw the two budgeted calls; the skipped three
    # never reached the SDK path.
    assert provider_calls["n"] == 2
    assert budget.calls_made == 2
    assert budget.cache_hits == 0


# ---------------------------------------------------------------------------
# 4. _BudgetCounter in isolation — direct thread-safety sanity check.
# ---------------------------------------------------------------------------


def test_budget_counter_try_charge_returns_false_when_drained() -> None:
    budget = mod._BudgetCounter(2)
    assert budget.try_charge_provider_call() is True
    assert budget.try_charge_provider_call() is True
    assert budget.try_charge_provider_call() is False
    assert budget.calls_made == 2


def test_budget_counter_refund_cache_hit_reopens_capacity() -> None:
    budget = mod._BudgetCounter(1)
    assert budget.try_charge_provider_call() is True
    assert budget.try_charge_provider_call() is False  # drained
    budget.refund_cache_hit()
    assert budget.cache_hits == 1
    # Refund reopens a slot so the next caller can charge again.
    assert budget.try_charge_provider_call() is True


def test_budget_counter_concurrent_try_charge_respects_cap() -> None:
    """Fire 32 threads at a budget of 8; exactly 8 succeed."""
    budget = mod._BudgetCounter(8)
    results: list[bool] = []
    results_lock = threading.Lock()

    def worker() -> None:
        ok = budget.try_charge_provider_call()
        with results_lock:
            results.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 8
    assert results.count(False) == 24
    assert budget.calls_made == 8
