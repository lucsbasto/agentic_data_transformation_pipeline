"""Batch identity for idempotent ingest.

A ``BatchIdentity`` is derived deterministically from the source file:
hashing the content plus the file mtime gives the same id whenever the
same file with the same content is re-ingested. Re-running the pipeline
on an unchanged source therefore short-circuits to a manifest hit
instead of double-writing Bronze.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

_HASH_CHUNK_BYTES = 1 << 20  # 1 MiB


@dataclass(frozen=True, slots=True)
class BatchIdentity:
    """Deterministic identity for a single ingest batch."""

    batch_id: str
    source_hash: str
    source_mtime: int


def compute_batch_identity(source: Path) -> BatchIdentity:
    """Hash ``source`` and combine with its mtime to derive a batch id.

    ``batch_id`` is the first 12 hex chars (48 bits) of the sha256 of
    ``"{source_hash}:{source_mtime}"``. That is enough to be practically
    collision-free for this pipeline (worst-case N unique batches where
    2^24 ≈ 16M is the birthday bound — far beyond our volume).
    """
    stat = source.stat()
    source_mtime = int(stat.st_mtime)

    content_hash = hashlib.sha256()
    with source.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_HASH_CHUNK_BYTES), b""):
            content_hash.update(chunk)
    source_hash = content_hash.hexdigest()

    combined = f"{source_hash}:{source_mtime}".encode()
    batch_id = hashlib.sha256(combined).hexdigest()[:12]

    return BatchIdentity(
        batch_id=batch_id,
        source_hash=source_hash,
        source_mtime=source_mtime,
    )
