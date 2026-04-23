"""Exception hierarchy for the pipeline.

All pipeline failures derive from :class:`PipelineError`. Callers outside
the package catch the root class; inside the package, narrow subclasses
enable precise handling.
"""

# LEARN: see ``__init__.py`` for why ``from __future__ import annotations``
# appears at the top of every module in this repo.
from __future__ import annotations


# LEARN: a class that inherits from ``Exception`` becomes a custom error
# type you can ``raise`` and ``except`` like any built-in. Defining a root
# class (``PipelineError``) and subclassing it for each failure mode is a
# Python idiom called a "typed exception hierarchy". Callers at the edge
# of the system write ``except PipelineError`` once to catch everything
# we throw; code deeper inside can narrow to ``except SchemaDriftError``
# when it wants to handle only that failure. The empty ``"""..."""``
# below each ``class`` line is a docstring — Python stores it on
# ``cls.__doc__`` and it shows up in help() / IDE tooltips.
class PipelineError(Exception):
    """Base class for every pipeline error. Never raised directly."""


class ConfigError(PipelineError):
    """Invalid or missing configuration at startup."""


class SchemaDriftError(PipelineError):
    """The source parquet no longer matches the declared Bronze schema."""


class IngestError(PipelineError):
    """Non-schema failure during ingest (I/O, atomic rename, validation)."""


class SilverError(PipelineError):
    """Non-schema failure during the Bronze → Silver transform."""


class ManifestError(PipelineError):
    """SQLite manifest access failed or the row shape is unexpected."""


# LEARN: ``LLMError`` is itself a base — ``LLMCacheError`` and
# ``LLMCallError`` extend it. That lets calling code say
# ``except LLMError`` to handle any LLM problem, or
# ``except LLMCallError`` to react only to provider-side failures.
class LLMError(PipelineError):
    """Base class for LLM client failures after retries are exhausted."""


class LLMCacheError(LLMError):
    """The LLM cache store failed to read or write."""


class LLMCallError(LLMError):
    """The LLM provider returned a non-retryable error."""
