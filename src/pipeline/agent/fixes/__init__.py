"""Per-`ErrorKind` deterministic fixes for the agent loop (F4 design §8).

Each submodule owns the recovery action for one ``ErrorKind`` and
exposes a small ``build_fix(...)`` factory that returns a
:class:`pipeline.agent.types.Fix`. The executor (F4.11) is the only
caller — fixes are not part of the public ``pipeline.agent`` surface.
"""

from __future__ import annotations
