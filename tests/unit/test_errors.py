"""Tests for the exception hierarchy in ``pipeline.errors``."""

from __future__ import annotations

from pipeline.errors import (
    ConfigError,
    IngestError,
    LLMCacheError,
    LLMCallError,
    LLMError,
    ManifestError,
    PipelineError,
    SchemaDriftError,
)


def test_every_specific_error_extends_pipeline_error() -> None:
    for cls in (
        ConfigError,
        SchemaDriftError,
        IngestError,
        ManifestError,
        LLMError,
        LLMCacheError,
        LLMCallError,
    ):
        assert issubclass(cls, PipelineError)


def test_llm_subclasses_extend_llm_error() -> None:
    assert issubclass(LLMCacheError, LLMError)
    assert issubclass(LLMCallError, LLMError)


def _chain_exception() -> None:
    try:
        raise ValueError("boom")
    except ValueError as exc:
        raise IngestError("ingest failed") from exc


def test_exception_chaining_preserves_cause() -> None:
    try:
        _chain_exception()
    except IngestError as final:
        assert isinstance(final.__cause__, ValueError)
        assert str(final.__cause__) == "boom"
