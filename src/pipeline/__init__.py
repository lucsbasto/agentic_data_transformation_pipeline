"""Agentic data transformation pipeline."""

# LEARN: ``from __future__ import annotations`` turns every type hint in this
# file into a plain string at parse time. Python only inspects the strings
# when a tool (mypy, Pydantic, FastAPI) asks for them. Benefit for beginners:
# you can reference a class name before it is defined, and you avoid slow
# import cycles. Required convention in this repo — keep the line on top.
from __future__ import annotations

# LEARN: dunder (double-underscore) names such as ``__version__`` are a
# Python convention for package metadata. Tools (pip, setuptools, anyone
# running ``import pipeline; print(pipeline.__version__)``) read this value
# to know which release they are looking at. Keep it in sync with
# ``pyproject.toml``'s ``project.version`` when you cut a release.
__version__ = "0.1.0"
