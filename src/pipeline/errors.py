"""Exception hierarchy for the pipeline.

All pipeline failures derive from :class:`PipelineError`. Callers outside
the package catch the root class; inside the package, narrow subclasses
enable precise handling.
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for every pipeline error. Never raised directly."""


class ConfigError(PipelineError):
    """Invalid or missing configuration at startup."""


class SchemaDriftError(PipelineError):
    """The source parquet no longer matches the declared Bronze schema."""


class IngestError(PipelineError):
    """Non-schema failure during ingest (I/O, atomic rename, validation)."""


class ManifestError(PipelineError):
    """SQLite manifest access failed or the row shape is unexpected."""


class LLMError(PipelineError):
    """Base class for LLM client failures after retries are exhausted."""


class LLMCacheError(LLMError):
    """The LLM cache store failed to read or write."""


class LLMCallError(LLMError):
    """The LLM provider returned a non-retryable error."""
