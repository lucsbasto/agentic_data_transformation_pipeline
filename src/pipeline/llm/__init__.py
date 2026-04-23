"""LLM client package."""

# LEARN: the LLM layer has two halves, exposed here as one API:
#   - ``LLMCache``  — a tiny SQLite table that stores request -> response
#                     pairs keyed by a hash of the request. Cheap to
#                     hit; no network.
#   - ``LLMClient`` — the provider-facing client. On every call it
#                     checks the cache first; on a miss it hits the
#                     Anthropic SDK (pointed at DashScope), retries on
#                     transient errors, falls back to a cheaper model if
#                     the primary keeps failing, then stores the result
#                     in the cache for next time.
# Callers should use this package via ``from pipeline.llm import
# LLMClient`` rather than reaching into submodules. If we ever split
# the two files further, callers stay untouched.
from pipeline.llm.cache import CachedResponse, LLMCache, compute_cache_key
from pipeline.llm.client import LLMClient, LLMResponse

__all__ = [
    "CachedResponse",
    "LLMCache",
    "LLMClient",
    "LLMResponse",
    "compute_cache_key",
]
