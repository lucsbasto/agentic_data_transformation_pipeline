"""LLM client facade.

F1 (this commit) only exposes the import-time shape: :class:`LLMClient`
and :class:`LLMResponse`. Every method raises :class:`NotImplementedError`
so that any caller that tries to use the client without the real wiring
fails loudly and immediately. The real Anthropic-SDK-over-DashScope
implementation, together with the sqlite-backed cache and retry logic,
lands in F1.7.

The facade exists in F1 so that:
- Downstream call sites (agent loop, Silver enrichment) can import the
  types today without a stub scattered across multiple modules.
- The CLI (F1.6) can already depend on :class:`Settings` resolving the
  LLM-related env vars without crashing on a missing class.
- Tests that need to substitute a fake client have a public Protocol-
  compatible target from the start.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pipeline.errors import LLMError
from pipeline.settings import Settings


@dataclass(frozen=True, slots=True, kw_only=True)
class LLMResponse:
    """Structured result of a single LLM call.

    ``kw_only=True`` is load-bearing: F1.7 will add ``retry_count``,
    ``cost_usd_estimate``, ``actual_model_used``, and a ``latency_ms``
    field (see ``.claude/skills/llm-client-anthropic-compat/SKILL.md``).
    Forcing keyword construction now means existing call sites don't
    break when those fields arrive — new callers fill them explicitly,
    old callers keep working.
    """

    text: str
    model: str
    input_tokens: int
    output_tokens: int
    cache_hit: bool


class LLMClient:
    """Thin wrapper around the Anthropic Python SDK pointed at DashScope.

    F1 ships only the class shape; every method raises.
    """

    def __init__(self, settings: Settings, **_: Any) -> None:
        self._settings = settings

    @property
    def settings(self) -> Settings:
        return self._settings

    def cached_call(
        self,
        *,
        system: str,
        user: str,
        model: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse:
        """Planned entrypoint for every LLM call in the pipeline.

        Raises :class:`LLMError` wrapping a :class:`NotImplementedError`
        until F1.7 implements the SDK call plus the sqlite cache.
        """
        del system, user, model, max_tokens, temperature
        raise LLMError(
            "LLMClient.cached_call is not implemented until F1.7; "
            "see .specs/features/F1/DESIGN.md §6."
        )

    def invalidate(self, *, prefix: str | None = None) -> int:
        """Cache invalidation hook. Raises until F1.7."""
        del prefix
        raise LLMError(
            "LLMClient.invalidate is not implemented until F1.7."
        )
