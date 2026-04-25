"""Performance benchmarking harness for the F4 agent loop (PERF lane).

Submodules:
    :mod:`pipeline.perf.harness` — public types, scenario protocol,
    JSONL writer, and the ``discover_scenarios`` registry helper.

    :mod:`pipeline.perf.scenarios` — namespace package; future PERF.*
    tasks self-register by dropping in a module that exposes a
    module-level ``SCENARIO`` constant.
"""
