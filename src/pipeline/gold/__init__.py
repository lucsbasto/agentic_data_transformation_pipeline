"""Gold layer analytics.

This package owns the Silver -> Gold step: four parquet tables
(``conversation_scores``, ``lead_profile``, ``agent_performance``,
``competitor_intel``) plus a non-obvious-insights JSON summary.
Persona classification (PRD §17, §18.2) and the deterministic intent
score (PRD §17.1) live here. I/O stays in ``pipeline.cli.gold`` so
every builder is a pure function over Polars frames or plain Python
inputs.
"""

from __future__ import annotations
