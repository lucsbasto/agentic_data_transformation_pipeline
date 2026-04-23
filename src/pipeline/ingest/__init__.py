"""Bronze ingest pipeline: read, transform, write."""

# LEARN: a package ``__init__.py`` is Python's "front door" for an
# import path. By re-exporting the useful names here, callers can write
# ``from pipeline.ingest import scan_source`` instead of the longer
# ``from pipeline.ingest.reader import scan_source``. Good for public
# API surface; keeps refactors that move modules around from breaking
# every caller.
from pipeline.ingest.batch import BatchIdentity, compute_batch_identity
from pipeline.ingest.reader import scan_source, validate_source_columns
from pipeline.ingest.transform import transform_to_bronze
from pipeline.ingest.writer import write_bronze

# LEARN: ``__all__`` declares "this is my public API". Two effects:
#   - ``from pipeline.ingest import *`` only pulls these names;
#   - linters and IDEs use it to warn on accidental private leaks.
# Sorting alphabetically keeps diffs tidy when a new export lands.
__all__ = [
    "BatchIdentity",
    "compute_batch_identity",
    "scan_source",
    "transform_to_bronze",
    "validate_source_columns",
    "write_bronze",
]
