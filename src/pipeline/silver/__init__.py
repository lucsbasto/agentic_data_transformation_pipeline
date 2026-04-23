"""Silver layer transforms.

This package owns the Bronze -> Silver step: dedup, normalization, PII
masking, lead_id derivation. Every submodule is written as a pure
function over :class:`polars.LazyFrame` or plain Python strings — I/O
stays in the CLI layer (``pipeline.cli.silver``) so the transforms
themselves are trivially testable.
"""

from __future__ import annotations
