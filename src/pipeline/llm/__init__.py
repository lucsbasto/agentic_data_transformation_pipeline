"""LLM client package.

F1 ships a stub so call sites can import the real shape even though the
network-facing implementation lands in F1.7. See
:mod:`pipeline.llm.client` for details.
"""

from pipeline.llm.client import LLMClient, LLMResponse

__all__ = ["LLMClient", "LLMResponse"]
