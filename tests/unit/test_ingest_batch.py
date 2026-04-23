"""Tests for ``pipeline.ingest.batch.compute_batch_identity``."""

from __future__ import annotations

import os
from pathlib import Path

from pipeline.ingest.batch import compute_batch_identity


def _write(tmp_path: Path, content: bytes, mtime: int) -> Path:
    path = tmp_path / "src.parquet"
    path.write_bytes(content)
    os.utime(path, (mtime, mtime))
    return path


def test_identity_is_deterministic(tmp_path: Path) -> None:
    p = _write(tmp_path, b"abc", 1_700_000_000)
    first = compute_batch_identity(p)
    second = compute_batch_identity(p)
    assert first == second


def test_identity_changes_when_content_changes(tmp_path: Path) -> None:
    p = _write(tmp_path, b"abc", 1_700_000_000)
    before = compute_batch_identity(p)
    p.write_bytes(b"abd")
    os.utime(p, (1_700_000_000, 1_700_000_000))
    after = compute_batch_identity(p)
    assert before.batch_id != after.batch_id
    assert before.source_hash != after.source_hash


def test_identity_changes_when_mtime_changes(tmp_path: Path) -> None:
    p = _write(tmp_path, b"abc", 1_700_000_000)
    before = compute_batch_identity(p)
    os.utime(p, (1_700_000_123, 1_700_000_123))
    after = compute_batch_identity(p)
    assert before.batch_id != after.batch_id
    assert before.source_hash == after.source_hash
    assert after.source_mtime == 1_700_000_123


def test_batch_id_length_is_12_hex_chars(tmp_path: Path) -> None:
    p = _write(tmp_path, b"abc", 1_700_000_000)
    ident = compute_batch_identity(p)
    assert len(ident.batch_id) == 12
    int(ident.batch_id, 16)  # must be valid hex


def test_source_hash_is_sha256_hex(tmp_path: Path) -> None:
    p = _write(tmp_path, b"abc", 1_700_000_000)
    ident = compute_batch_identity(p)
    # sha256 hex = 64 chars
    assert len(ident.source_hash) == 64
    int(ident.source_hash, 16)
