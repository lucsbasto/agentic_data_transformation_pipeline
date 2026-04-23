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

# LEARN: two stdlib imports central to retry logic:
#   - ``random`` — we add *jitter* to the backoff so multiple clients
#     that fail together do not all retry at the exact same moment (a
#     "thundering herd"). A random 10% extra delay spreads them out.
#   - ``time``   — ``time.sleep`` pauses the thread; ``time.monotonic``
#     measures durations without being affected by clock changes.
import random
import time
from dataclasses import dataclass
from typing import Any, Protocol

# LEARN: we import specific error classes from the Anthropic SDK so we
# can decide which ones are worth retrying. The SDK exposes a
# well-designed hierarchy: transient network issues vs permanent
# request issues. Lumping them together into a single ``except
# Exception`` would either burn the retry budget on errors we can
# never recover from, OR give up on errors that a second try would
# fix. Split carefully.
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

# LEARN: tuples of exception classes. Python's ``except (A, B, C)``
# takes any iterable of types and matches instances of any of them.
# Naming the two groups as module-level constants keeps the retry
# policy explicit and greppable ("why did we retry on X?").
_RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
    RateLimitError,        # provider says "slow down" — wait + retry
    APIConnectionError,    # TCP/DNS wobble — retry probably fixes it
    APITimeoutError,       # response took too long — retry with backoff
)

_FATAL_EXCEPTIONS: tuple[type[Exception], ...] = (
    AuthenticationError,   # wrong/missing API key — retrying cannot fix
    BadRequestError,       # malformed request — retrying is pointless
    PermissionDeniedError, # account-level deny — needs operator action
)


# LEARN: ``Protocol`` is Python's *structural subtyping* — "if it walks
# like a duck". The real Anthropic SDK has a ``.messages`` attribute
# with a ``.create(...)`` method. Tests want to hand us a fake object
# that has the same shape without inheriting from the SDK. A Protocol
# says "anything that has these attributes is acceptable" — mypy
# verifies the shape, runtime does not care.
class _MessagesLike(Protocol):
    """Shape of the ``messages`` attribute on the Anthropic client."""

    # LEARN: a method declared in a Protocol ends with ``...`` (the
    # Ellipsis literal). No body required — it is a pure type signature.
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
        # LEARN: *dependency injection* — the class receives its
        # collaborators (settings, cache, SDK, sleeper) through the
        # constructor instead of reaching for globals. Benefits:
        #   - tests can pass a fake SDK and a ``lambda _s: None`` as
        #     sleeper so they finish instantly;
        #   - behaviour changes at the wiring site, not deep in the
        #     class body.
        self._settings = settings
        self._cache = cache
        # LEARN: ``x or y`` returns ``x`` if truthy, else ``y``. So
        # ``anthropic_client or self._build_anthropic(settings)``
        # means "use the injected client if given, otherwise build a
        # real one from settings".
        self._anthropic = anthropic_client or self._build_anthropic(settings)
        self._sleep = sleeper
        self._logger = get_logger("pipeline.llm")

    # ------------------------------------------------------------------ config

    @property
    def settings(self) -> Settings:
        return self._settings

    # LEARN: ``@staticmethod`` is a pure function attached to the class
    # — no ``self``, no ``cls``. Use it when the function fits
    # conceptually with the class but does not need any of its state.
    @staticmethod
    def _build_anthropic(settings: Settings) -> Anthropic:
        # LEARN: ``SecretStr.get_secret_value()`` returns the plaintext
        # key. We only reach into it at this exact boundary so the
        # secret never leaks through ``repr(settings)`` or logs. The
        # ``base_url`` override is what points the Anthropic SDK at
        # DashScope's Anthropic-compatible endpoint — same SDK, same
        # API shape, different provider. Cheap portability.
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
        # LEARN: the core LLM-engineering move — check the cache before
        # paying for an API call. A stable cache key + deterministic
        # inputs = pipeline reproducibility + cost savings. The primary
        # model name is part of the key, so a fallback response is
        # stored under its OWN (primary) key — intentional, documented
        # in the F1.7 review.
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
            # LEARN: cache HIT log. Observability is part of LLM
            # engineering — you need to know your hit rate to know
            # whether the cache is earning its keep. Logging only the
            # first 16 chars of the key keeps lines short; full keys
            # are in the DB if you need them.
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

        # LEARN: cache MISS log. Paired with ``llm.cache_hit`` it lets
        # operators compute hit rate as a simple log query.
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

        # LEARN: on a successful call, persist the response so next time
        # this exact request becomes a cache hit. Note we key by the
        # PRIMARY model even if a fallback served the response — that
        # is the documented "replay what the caller asked for" contract.
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
        # LEARN: the retry-budget idea is standard for provider-facing
        # code. ``pipeline_retry_budget = 3`` means we will try the
        # primary model up to 3 times before giving up on it. Each
        # retry waits longer than the last (exponential backoff) with
        # a little randomness (jitter) to avoid synchronized retries.
        budget = self._settings.pipeline_retry_budget
        last_exc: Exception | None = None
        # LEARN: ``range(1, budget + 1)`` yields 1..budget inclusive.
        # Using 1-based counters for "attempt number" reads cleaner in
        # logs ("attempt 1 of 3") than 0-based ones.
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
                # LEARN: fatal error — no point retrying. Log once,
                # then translate to our domain error and bail out.
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
                # LEARN: retryable error — remember the last one (we'll
                # include it in the final error if everything fails),
                # log a warning, sleep, and try again.
                last_exc = exc
                self._logger.warning(
                    "llm.retry",
                    model=primary_model,
                    error_type=type(exc).__name__,
                    attempt=attempt,
                    budget=budget,
                )
                # LEARN: only sleep BETWEEN retries, not after the last
                # one — that sleep would be wasted time right before
                # the fallback attempt below.
                if attempt < budget:
                    self._sleep(_backoff_delay(attempt))

        # Primary exhausted — one shot on the fallback.
        # LEARN: after the primary model has burned its retry budget
        # we swap to the fallback model for ONE last attempt. Fallback
        # is typically cheaper/faster (``qwen3-coder-plus`` for us) and
        # has different rate-limit quotas, so it often succeeds where
        # the primary was being throttled.
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
            # LEARN: ``raise X from last_exc or exc`` chains the
            # original retryable cause if one was captured, otherwise
            # the fallback's own exception. Preserving the first cause
            # helps debug "why did the primary give up?".
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
        # LEARN: ``time.monotonic`` measures wall time for latency; use
        # it for "how long did this call take" dashboards, never for
        # timestamps — a monotonic clock has no calendar meaning.
        started = time.monotonic()
        # LEARN: the Anthropic SDK call. ``messages`` is a list of
        # role/content pairs; F1 sends a single ``user`` message with
        # the full prompt. ``system`` is the "instructions" text that
        # steers the model. ``temperature=0`` makes responses as
        # deterministic as the model allows — a prerequisite for the
        # cache to produce consistent hits.
        message = self._anthropic.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            temperature=temperature,
            messages=[{"role": "user", "content": user}],
        )
        latency_ms = int((time.monotonic() - started) * 1000)

        # LEARN: guard against two failure modes before we cache the
        # result. First: the provider returned *no* text block (e.g.
        # tool-use only). Second: the response is pathologically long
        # (hostile upstream, runaway generation) — we refuse to poison
        # the SQLite cache with an unbounded blob.
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
        # LEARN: ``getattr(obj, name, default)`` reads an attribute but
        # returns ``default`` if it is missing. Here we defensively
        # handle the case where ``message.usage`` might be absent in
        # a future SDK version or a non-standard response shape.
        usage = getattr(message, "usage", None)
        # LEARN: ``int(value or 0)`` turns ``None`` / ``""`` / ``0``
        # into ``0`` and coerces numbers to int. Harmless belt-and-
        # braces against provider responses that report tokens as
        # ``None``.
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
            # LEARN: ``attempt - 1`` because attempt 1 = zero prior
            # retries, attempt 2 = one prior retry, etc. Off-by-one
            # arithmetic that is easy to get wrong — worth a comment.
            retry_count=attempt - 1,
        )


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff capped at 30 s with 10% jitter."""
    # LEARN: ``2 ** (attempt - 1)`` doubles each time: 1, 2, 4, 8, ...
    # ``min(..., 30)`` caps the wait at 30 seconds so a long-running
    # pipeline does not stall forever on a flaky endpoint.
    base: float = float(min(2 ** (attempt - 1), 30))
    # LEARN: ``random.random()`` returns a uniform float in [0, 1).
    # Multiplying by 0.1 gives 0-10% extra delay — the "jitter" that
    # prevents a stampede of clients all retrying on the same tick.
    jitter: float = random.random() * 0.1 * base
    return base + jitter


def _first_text_block(message: Any) -> str:
    """Extract the first text block from an Anthropic Message.

    The SDK returns ``message.content`` as a list; each element either has
    a ``.text`` attribute (text block) or is tool-use metadata. F1 only
    consumes text; tool-use arrives in a later feature.
    """
    # LEARN: defensive ``or []`` — if ``content`` is ``None`` (or any
    # falsy value) we treat it as an empty list so the loop below
    # terminates immediately without raising ``TypeError``.
    content = getattr(message, "content", None) or []
    for block in content:
        # LEARN: ``getattr(block, "text", None)`` asks for a ``.text``
        # attribute without caring whether the block is a text block
        # or a tool-use block. The isinstance check makes sure we
        # return an actual string, not some other truthy value.
        text = getattr(block, "text", None)
        if isinstance(text, str):
            return text
    return ""
