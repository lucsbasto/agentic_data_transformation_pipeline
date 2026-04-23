"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pipeline.schemas.bronze import (
    DIRECTION_VALUES,
    MESSAGE_TYPE_VALUES,
    SOURCE_COLUMNS,
    STATUS_VALUES,
)


@pytest.fixture
def project_root() -> Path:
    """Absolute path to the repo root (two levels above this file)."""
    return Path(__file__).resolve().parent.parent


@pytest.fixture
def tiny_source_df() -> pl.DataFrame:
    """A small synthetic source DataFrame that exercises every enum value.

    Rows are generated deterministically and cover:
      - both ``direction`` values;
      - every ``message_type`` variant (8);
      - every ``status`` variant (3);
      - multi-message conversations so downstream grouping has signal.
    """
    rows: list[dict[str, str]] = []
    conv_id = 0
    msg_seq = 0
    for mtype in MESSAGE_TYPE_VALUES:
        for direction in DIRECTION_VALUES:
            for status in STATUS_VALUES:
                conv_id += 1
                for k in range(2):  # two messages per conversation
                    msg_seq += 1
                    rows.append(
                        {
                            "message_id": f"msg_{msg_seq:05d}",
                            "conversation_id": f"conv_{conv_id:05d}",
                            "timestamp": f"2026-02-14 12:{k:02d}:00",
                            "direction": direction,
                            "sender_phone": f"+55119{msg_seq:07d}",
                            "sender_name": f"Person {msg_seq % 5}",
                            "message_type": mtype,
                            "message_body": f"hello {msg_seq}",
                            "status": status,
                            "channel": "whatsapp",
                            "campaign_id": "camp_test_fev2026",
                            "agent_id": f"agent_test_{msg_seq % 3:02d}",
                            "conversation_outcome": "venda_fechada",
                            "metadata": '{"device": "desktop"}',
                        }
                    )
    df = pl.DataFrame(rows)
    # Match the source schema: everything is String.
    return df.select([pl.col(c).cast(pl.String) for c in SOURCE_COLUMNS])


@pytest.fixture
def tiny_source_parquet(tiny_source_df: pl.DataFrame, tmp_path: Path) -> Path:
    """Write ``tiny_source_df`` as parquet to ``tmp_path`` and return the path."""
    path = tmp_path / "tiny_conversations.parquet"
    tiny_source_df.write_parquet(path)
    return path
