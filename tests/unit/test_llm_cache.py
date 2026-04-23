"""Tests for ``pipeline.llm.cache``."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.errors import LLMCacheError
from pipeline.llm.cache import (
    CachedResponse,
    LLMCache,
    compute_cache_key,
)


@pytest.fixture
def cache() -> LLMCache:
    with LLMCache(":memory:") as c:
        yield c


def _put(cache: LLMCache, key: str = "k1") -> None:
    cache.put(
        cache_key=key,
        model="qwen3-max",
        response_text="hello",
        input_tokens=5,
        output_tokens=3,
    )


def test_compute_cache_key_is_deterministic() -> None:
    a = compute_cache_key(
        model="m", system="s", user="u", max_tokens=100, temperature=0.0
    )
    b = compute_cache_key(
        model="m", system="s", user="u", max_tokens=100, temperature=0.0
    )
    assert a == b
    assert len(a) == 64  # sha256 hex


def test_compute_cache_key_differs_per_field() -> None:
    base = {"model": "m", "system": "s", "user": "u", "max_tokens": 100, "temperature": 0.0}
    ref = compute_cache_key(**base)
    for field, other in [
        ("model", "m2"),
        ("system", "s2"),
        ("user", "u2"),
        ("max_tokens", 200),
        ("temperature", 0.7),
    ]:
        variant = {**base, field: other}
        assert compute_cache_key(**variant) != ref


def test_get_missing_returns_none(cache: LLMCache) -> None:
    assert cache.get("does-not-exist") is None


def test_put_and_get_roundtrip(cache: LLMCache) -> None:
    _put(cache, "k1")
    row = cache.get("k1")
    assert row is not None
    assert isinstance(row, CachedResponse)
    assert row.model == "qwen3-max"
    assert row.response_text == "hello"
    assert row.input_tokens == 5
    assert row.output_tokens == 3


def test_put_is_insert_or_ignore(cache: LLMCache) -> None:
    _put(cache, "k1")
    # Try to overwrite with a different body — must not replace the first row.
    cache.put(
        cache_key="k1",
        model="qwen3-max",
        response_text="SECOND",
        input_tokens=99,
        output_tokens=99,
    )
    row = cache.get("k1")
    assert row is not None
    assert row.response_text == "hello"
    assert row.input_tokens == 5


def test_invalidate_all(cache: LLMCache) -> None:
    _put(cache, "a")
    _put(cache, "b")
    removed = cache.invalidate()
    assert removed == 2
    assert cache.get("a") is None
    assert cache.get("b") is None


def test_invalidate_prefix(cache: LLMCache) -> None:
    _put(cache, "abc1")
    _put(cache, "abc2")
    _put(cache, "xyz")
    removed = cache.invalidate(prefix="abc")
    assert removed == 2
    assert cache.get("abc1") is None
    assert cache.get("xyz") is not None


def test_requires_open_connection() -> None:
    c = LLMCache(":memory:")
    with pytest.raises(LLMCacheError, match="not open"):
        c.get("x")


def test_file_backed_cache_creates_parent_dir(tmp_path: Path) -> None:
    nested = tmp_path / "sub" / "dir" / "cache.db"
    with LLMCache(nested) as c:
        _put(c, "k1")
    assert nested.is_file()
