"""F3.10 — thread-safe persona concurrency lane.

The persona classifier is the single LLM-intensive step in Gold.
Sequential ``classify_with_overrides`` over 5000 leads would blow
the M2 15-minute SLA (F3-RNF-06); this module fans the per-lead
calls out across a small pool of worker threads, each carrying its
own :class:`LLMClient` and :class:`LLMCache` connection.

Why a thread pool and not ``asyncio`` directly? The Anthropic SDK
used by :class:`LLMClient` is synchronous, and :class:`LLMCache`
opens a SQLite connection that refuses cross-thread access with its
default settings. Threads give us drop-in concurrency without
rewriting either collaborator.

Spec drivers
------------
- F3-RF-18 / F3-RNF-08 — bounded ``pipeline_llm_concurrency`` as the
  single in-flight knob.
- F3-RNF-06 — 15-minute end-to-end budget on 153k rows.
- PRD §18.4 — ``pipeline_llm_max_calls_per_batch`` = 5000 *cache
  misses*; cache hits do not count.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline.gold.persona import (
    LeadAggregate,
    PersonaResult,
    classify_with_overrides,
    evaluate_rules,
)
from pipeline.llm.cache import LLMCache
from pipeline.llm.client import LLMClient, LLMResponse
from pipeline.logging import get_logger
from pipeline.settings import Settings

__all__ = ["classify_all"]


_thread_local = threading.local()


class _CacheHitTrackingClient:
    """Thin wrapper around :class:`LLMClient` that remembers whether
    the last ``cached_call`` was served from cache.

    :class:`PersonaResult` does not expose ``cache_hit`` (design §9.4
    referred to a field that does not exist on the F3.9 dataclass),
    so we capture the signal at the boundary where it is still
    available: ``LLMResponse.cache_hit``. Workers read
    ``last_cache_hit`` after classification to decide whether the
    optimistic budget charge should be refunded.
    """

    __slots__ = ("_inner", "last_cache_hit")

    def __init__(self, inner: LLMClient) -> None:
        self._inner = inner
        self.last_cache_hit = False

    def cached_call(self, **kwargs: Any) -> LLMResponse:
        response = self._inner.cached_call(**kwargs)
        self.last_cache_hit = response.cache_hit
        return response


class _BudgetCounter:
    """Thread-safe per-batch cap on *billable* LLM calls.

    Charge is optimistic: a worker charges the budget before calling
    the provider, then refunds on a cache hit. That removes the race
    where two threads simultaneously observe "budget ok" past the
    cap. Cache hits are free (PRD §18.4), so the counter represents
    the number of provider round-trips actually issued.
    """

    __slots__ = (
        "_lock",
        "_remaining",
        "budget_exhausted_logged",
        "cache_hits",
        "calls_made",
    )

    def __init__(self, max_provider_calls: int) -> None:
        self._lock = threading.Lock()
        self._remaining = max_provider_calls
        self.calls_made = 0
        self.cache_hits = 0
        self.budget_exhausted_logged = False

    def try_charge_provider_call(self) -> bool:
        """Return ``True`` if a slot was charged; ``False`` once the
        budget is spent. Callers that get ``False`` must short-circuit
        with :meth:`PersonaResult.skipped`."""
        with self._lock:
            if self._remaining <= 0:
                return False
            self._remaining -= 1
            self.calls_made += 1
            return True

    def refund_cache_hit(self) -> None:
        """Undo a charge because the response came from cache.

        Bumps the ``cache_hits`` counter so operators can read hit
        rate straight from the budget object after the run.
        """
        with self._lock:
            self._remaining += 1
            self.calls_made -= 1
            self.cache_hits += 1


def _init_worker(settings: Settings, db_path: Path) -> None:
    """Build this worker thread's own :class:`LLMClient` + cache.

    ``LLMClient.__init__`` documents *"not thread-safe: give each
    concurrent consumer its own instance"*; SQLite connections opened
    by :class:`LLMCache` likewise default to single-thread access.
    Each worker therefore owns its own pair, backed by the same
    on-disk file (WAL + ``busy_timeout=5000`` from
    :meth:`LLMCache.open` keep concurrent writes safe).

    No explicit shutdown hook is needed: when the worker thread
    exits at ``executor.shutdown(wait=True)`` time, ``_thread_local``
    storage is garbage-collected and the cache connection closes
    with it.
    """
    cache = LLMCache(db_path).open()
    inner = LLMClient(settings, cache)
    _thread_local.cache = cache
    _thread_local.client = _CacheHitTrackingClient(inner)


def _classify_one_sync(
    agg: LeadAggregate,
    budget: _BudgetCounter,
    batch_latest_timestamp: datetime,
) -> tuple[str, PersonaResult]:
    """Runs on a worker thread. Reads ``_thread_local.client``.

    Only LLM-needing leads reach this function — rule-hit leads are
    resolved synchronously by :func:`classify_all` before dispatch so
    they never consume a budget slot or a thread-pool worker. Budget
    semantics: we charge before calling the classifier, and refund
    on cache hit after the fact.
    """
    if not budget.try_charge_provider_call():
        return agg.lead_id, PersonaResult.skipped()
    client: _CacheHitTrackingClient = _thread_local.client
    client.last_cache_hit = False
    result = classify_with_overrides(
        agg,
        batch_latest_timestamp=batch_latest_timestamp,
        client=client,  # type: ignore[arg-type]
    )
    if client.last_cache_hit:
        budget.refund_cache_hit()
    return agg.lead_id, result


async def classify_all(
    aggregates: list[LeadAggregate],
    *,
    settings: Settings,
    db_path: Path,
    budget: _BudgetCounter,
    batch_latest_timestamp: datetime,
) -> dict[str, PersonaResult]:
    """Classify every aggregate under a bounded concurrency ceiling.

    Flow per lead:

    1. :class:`asyncio.Semaphore` caps in-flight tasks at
       ``settings.pipeline_llm_concurrency``.
    2. The sync :func:`_classify_one_sync` runs on a worker thread
       via :meth:`loop.run_in_executor`.
    3. Results assemble into ``{lead_id: PersonaResult}``.

    ``batch_latest_timestamp`` threads through to
    :func:`classify_with_overrides` so the R1 staleness rule stays
    deterministic (design D12).
    """
    logger = get_logger("pipeline.gold.concurrency")

    # Pre-filter rule-hit leads synchronously so they never consume
    # a budget slot or a thread-pool worker (design §9.3 follow-up).
    rule_results: dict[str, PersonaResult] = {}
    llm_needed: list[LeadAggregate] = []
    for agg in aggregates:
        hit = evaluate_rules(agg, batch_latest_timestamp=batch_latest_timestamp)
        if hit is not None:
            rule_results[agg.lead_id] = hit
        else:
            llm_needed.append(agg)

    concurrency = settings.pipeline_llm_concurrency
    sem = asyncio.Semaphore(concurrency)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(
        max_workers=concurrency,
        initializer=_init_worker,
        initargs=(settings, db_path),
        thread_name_prefix="gold-persona",
    )

    async def one(agg: LeadAggregate) -> tuple[str, PersonaResult]:
        async with sem:
            return await loop.run_in_executor(
                executor,
                _classify_one_sync,
                agg,
                budget,
                batch_latest_timestamp,
            )

    try:
        llm_results = await asyncio.gather(*[one(a) for a in llm_needed])
    finally:
        executor.shutdown(wait=True)

    logger.info(
        "gold.persona.batch_complete",
        total=len(aggregates),
        rule_hits=len(rule_results),
        llm_dispatched=len(llm_needed),
        provider_calls=budget.calls_made,
        cache_hits=budget.cache_hits,
        concurrency=concurrency,
    )
    return {**rule_results, **dict(llm_results)}
