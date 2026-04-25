"""Namespace package for perf scenarios.

Each scenario is a single module that exposes a module-level
``SCENARIO`` attribute satisfying :class:`pipeline.perf.harness.Scenario`.
The discovery helper picks them up automatically via
``pkgutil.iter_modules`` — no edits to this ``__init__`` are needed
when adding scenarios.
"""
