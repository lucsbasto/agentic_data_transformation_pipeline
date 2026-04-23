"""Tests for ``pipeline.schemas.manifest``."""

from __future__ import annotations

import sqlite3

import pytest

from pipeline.schemas.manifest import (
    ALL_DDL,
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_FAILED,
    BATCH_STATUS_IN_PROGRESS,
    BATCH_STATUSES,
    BATCHES_DDL,
    BATCHES_INDEXES,
    LLM_CACHE_DDL,
    RUNS_DDL,
)


def _apply(all_sql: tuple[str, ...]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    for stmt in all_sql:
        conn.execute(stmt)
    conn.commit()
    return conn


def test_ddl_applies_cleanly_on_empty_db() -> None:
    conn = _apply(ALL_DDL)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert {"batches", "runs", "llm_cache"} <= tables


def test_ddl_is_idempotent() -> None:
    conn = _apply(ALL_DDL)
    # Re-apply — must not raise.
    for stmt in ALL_DDL:
        conn.execute(stmt)
    conn.commit()


def test_batches_indexes_exist() -> None:
    conn = _apply(ALL_DDL)
    indexes = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
    }
    assert {"idx_batches_status", "idx_batches_started"} <= indexes
    assert len(BATCHES_INDEXES) == 2


def test_batches_status_check_constraint() -> None:
    conn = _apply(ALL_DDL)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO batches (batch_id, source_path, source_hash, "
            "source_mtime, status, started_at) "
            "VALUES ('b1','src','h',1,'BOGUS','2026-04-22T00:00:00Z');"
        )


def test_runs_layer_check_constraint() -> None:
    conn = _apply(ALL_DDL)
    conn.execute(
        "INSERT INTO batches (batch_id, source_path, source_hash, "
        "source_mtime, status, started_at) "
        "VALUES ('b1','src','h',1,'COMPLETED','2026-04-22T00:00:00Z');"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO runs (run_id, batch_id, layer, status, started_at) "
            "VALUES ('r1','b1','platinum','COMPLETED','2026-04-22T00:00:00Z');"
        )


def test_runs_status_check_constraint() -> None:
    conn = _apply(ALL_DDL)
    conn.execute(
        "INSERT INTO batches (batch_id, source_path, source_hash, "
        "source_mtime, status, started_at) "
        "VALUES ('b1','src','h',1,'COMPLETED','2026-04-22T00:00:00Z');"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO runs (run_id, batch_id, layer, status, started_at) "
            "VALUES ('r1','b1','bronze','BOGUS','2026-04-22T00:00:00Z');"
        )


def test_runs_indexes_exist() -> None:
    conn = _apply(ALL_DDL)
    indexes = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
    }
    assert {"idx_runs_batch_id", "idx_runs_status", "idx_runs_layer"} <= indexes


def test_llm_cache_primary_key() -> None:
    conn = _apply(ALL_DDL)
    conn.execute(
        "INSERT INTO llm_cache (cache_key, model, response_text, "
        "input_tokens, output_tokens, created_at) "
        "VALUES ('k1','m','r',1,2,'2026-04-22T00:00:00Z');"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO llm_cache (cache_key, model, response_text, "
            "input_tokens, output_tokens, created_at) "
            "VALUES ('k1','m','r',1,2,'2026-04-22T00:00:00Z');"
        )


def test_status_constants_exposed() -> None:
    assert BATCH_STATUS_IN_PROGRESS == "IN_PROGRESS"
    assert BATCH_STATUS_COMPLETED == "COMPLETED"
    assert BATCH_STATUS_FAILED == "FAILED"
    assert set(BATCH_STATUSES) == {
        BATCH_STATUS_IN_PROGRESS,
        BATCH_STATUS_COMPLETED,
        BATCH_STATUS_FAILED,
    }


def test_individual_ddl_strings_exported() -> None:
    assert "CREATE TABLE" in BATCHES_DDL
    assert "CREATE TABLE" in RUNS_DDL
    assert "CREATE TABLE" in LLM_CACHE_DDL
