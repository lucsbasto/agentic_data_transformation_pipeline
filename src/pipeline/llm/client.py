"""LLM client — Anthropic SDK pointed at DashScope's compatible endpoint.

Every LLM call in the pipeline goes through :meth:`LLMClient.cached_call`:

- the request signature is hashed into a cache key and looked up in the
  SQLite cache (see :mod:`pipeline.llm.cache`);
- a miss round-trips to the Anthropic SDK with ``base_url`` pointed at
  DashScope, respecting the configured retry budget and an exponential
  backoff with jitter;
- ``RateLimitError`` / ``APIConnectionError`` / ``APITimeoutError`` are
  the only retryable failure classes; ``AuthenticationError``,
  ``BadRequestError`` and ``PermissionDeniedError`` fail immediately;
- after retries on the primary model are exhausted for a retryable
  class, one final attempt runs on the fallback model;
- every call emits structlog events (``llm.cache_hit``,
  ``llm.cache_miss``, ``llm.retry``, ``llm.failed``,
  ``llm.completed``) so the agent loop can observe cost and latency.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Protocol

from anthropic import (
    Anthropic,
    APIConnectionError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    PermissionDeniedError,
    RateLimitError,
)

from pipeline.errors import LLMCallError
from pipeline.llm.cache import LLMCache, compute_cache_key
from pipeline.logging import get_logger
from pipeline.settings import Settings

_RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
    RateLimitError,
    APIConnectionError,
    APITimeoutError,
)

_FATAL_EXCEPTIONS: tuple[type[Exception], ...] = (
    AuthenticationError,
    BadRequestError,
    PermissionDeniedError,
)


class _MessagesLike(Protocol):
    """Shape of the ``messages`` attribute on the Anthropic client."""

    def create(self, **kwargs: Any) -> Any: ...


class _AnthropicLike(Protocol):
    """Subset of the Anthropic SDK that :class:`LLMClient` relies on.

    Defined as a :class:`typing.Protocol` so tests can pass a
    structurally compatible fake without subclassing ``Anthropic``.
    """

    messages: _MessagesLike


@dataclass(frozen=True, slots=True, kw_only=True)
class LLMResponse:
    """Structured result of a single LLM call.

    ``kw_only=True`` is load-bearing: future fields (cost estimate,
    per-call retry count, actual-model-used after fallback) are added
    without breaking existing callers. Listed today:

    - ``text``: the assistant's response text.
    - ``model``: the model name that actually produced this response
      (may differ from the requested one if a fallback kicked in).
    - ``input_tokens`` / ``output_tokens``: as reported by the provider
      (or stored in the cache on a hit).
    - ``cache_hit``: ``True`` if the response was served from the cache.
    - ``retry_count``: how many retryable failures preceded this result
      (0 on the first attempt, ``>= 1`` after backoff).
    """

    text: str
    model: str
    input_tokens: int
    output_tokens: int
    cache_hit: bool
    retry_count: int = 0


class LLMClient:
    """Single entry point for every LLM call in the pipeline.

    The client is cheap to build but not thread-safe: give each
    concurrent consumer its own instance.
    """

    def __init__(
        self,
        settings: Settings,
        cache: LLMCache,
        *,
        anthropic_client: _AnthropicLike | None = None,
        sleeper: Any = time.sleep,
    ) -> None:
        self._settings = settings
        self._cache = cache
        self._anthropic = anthropic_client or self._build_anthropic(settings)
        self._sleep = sleeper
        self._logger = get_logger("pipeline.llm")

    # ------------------------------------------------------------------ config

    @property
    def settings(self) -> Settings:
        return self._settings

    @staticmethod
    def _build_anthropic(settings: Settings) -> Anthropic:
        return Anthropic(
            api_key=settings.anthropic_api_key.get_secret_value(),
            base_url=str(settings.anthropic_base_url),
        )

    # ------------------------------------------------------------------ public

    def cached_call(
        self,
        *,
        system: str,
        user: str,
        model: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse:
        """Return a cached response if available; otherwise call the SDK."""
        primary_model = model or self._settings.llm_model_primary
        cache_key = compute_cache_key(
            model=primary_model,
            system=system,
            user=user,
            max_tokens=max_tokens,
            temperature=temperature,
        )

        cached = self._cache.get(cache_key)
        if cached is not None:
            self._logger.info(
                "llm.cache_hit",
                model=cached.model,
                cache_key=cache_key[:16],
                input_tokens=cached.input_tokens,
                output_tokens=cached.output_tokens,
            )
            return LLMResponse(
                text=cached.response_text,
                model=cached.model,
                input_tokens=cached.input_tokens,
                output_tokens=cached.output_tokens,
                cache_hit=True,
            )

        self._logger.info(
            "llm.cache_miss",
            model=primary_model,
            cache_key=cache_key[:16],
        )

        response = self._call_with_retries(
            system=system,
            user=user,
            primary_model=primary_model,
            max_tokens=max_tokens,
            temperature=temperature,
        )

        self._cache.put(
            cache_key=cache_key,
            model=response.model,
            response_text=response.text,
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
        )
        return response

    def invalidate(self, *, prefix: str | None = None) -> int:
        """Drop cached rows, optionally limited to keys starting with ``prefix``."""
        removed = self._cache.invalidate(prefix=prefix)
        self._logger.info(
            "llm.cache_invalidated",
            prefix=prefix,
            removed=removed,
        )
        return removed

    # ------------------------------------------------------------------ internals

    def _call_with_retries(
        self,
        *,
        system: str,
        user: str,
        primary_model: str,
        max_tokens: int,
        temperature: float,
    ) -> LLMResponse:
        budget = self._settings.pipeline_retry_budget
        last_exc: Exception | None = None
        for attempt in range(1, budget + 1):
            try:
                return self._one_call(
                    system=system,
                    user=user,
                    model=primary_model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    attempt=attempt,
                )
            except _FATAL_EXCEPTIONS as exc:
                self._logger.exception(
                    "llm.failed",
                    model=primary_model,
                    error_type=type(exc).__name__,
                    attempt=attempt,
                )
                raise LLMCallError(
                    f"non-retryable LLM error ({type(exc).__name__}): {exc}"
                ) from exc
            except _RETRYABLE_EXCEPTIONS as exc:
                last_exc = exc
                self._logger.warning(
                    "llm.retry",
                    model=primary_model,
                    error_type=type(exc).__name__,
                    attempt=attempt,
                    budget=budget,
                )
                if attempt < budget:
                    self._sleep(_backoff_delay(attempt))

        # Primary exhausted — one shot on the fallback.
        fallback = self._settings.llm_model_fallback
        self._logger.warning(
            "llm.fallback",
            primary=primary_model,
            fallback=fallback,
            attempts=budget,
        )
        try:
            return self._one_call(
                system=system,
                user=user,
                model=fallback,
                max_tokens=max_tokens,
                temperature=temperature,
                attempt=budget + 1,
            )
        except Exception as exc:
            self._logger.exception(
                "llm.failed",
                model=fallback,
                error_type=type(exc).__name__,
                attempt=budget + 1,
            )
            raise LLMCallError(
                f"LLM call failed after retries on primary ({primary_model}) "
                f"and fallback ({fallback}): {exc}"
            ) from last_exc or exc

    def _one_call(
        self,
        *,
        system: str,
        user: str,
        model: str,
        max_tokens: int,
        temperature: float,
        attempt: int,
    ) -> LLMResponse:
        started = time.monotonic()
        message = self._anthropic.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            temperature=temperature,
            messages=[{"role": "user", "content": user}],
        )
        latency_ms = int((time.monotonic() - started) * 1000)

        text = _first_text_block(message)
        if not text:
            raise LLMCallError(
                f"LLM returned no text block (model={model!r}, attempt={attempt}); "
                "refusing to cache an empty response"
            )
        cap = self._settings.pipeline_llm_response_cap
        if len(text) > cap:
            raise LLMCallError(
                f"LLM response of {len(text)} chars exceeds "
                f"pipeline_llm_response_cap={cap} (model={model!r}, "
                f"attempt={attempt}); refusing to cache"
            )
        usage = getattr(message, "usage", None)
        input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)

        self._logger.info(
            "llm.completed",
            model=model,
            attempt=attempt,
            latency_ms=latency_ms,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

        return LLMResponse(
            text=text,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_hit=False,
            retry_count=attempt - 1,
        )


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff capped at 30 s with 10% jitter."""
    base: float = float(min(2 ** (attempt - 1), 30))
    jitter: float = random.random() * 0.1 * base
    return base + jitter


def _first_text_block(message: Any) -> str:
    """Extract the first text block from an Anthropic Message.

    The SDK returns ``message.content`` as a list; each element either has
    a ``.text`` attribute (text block) or is tool-use metadata. F1 only
    consumes text; tool-use arrives in a later feature.
    """
    content = getattr(message, "content", None) or []
    for block in content:
        text = getattr(block, "text", None)
        if isinstance(text, str):
            return text
    return ""
