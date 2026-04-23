"""Tests for ``pipeline.silver.audio``."""

from __future__ import annotations

import polars as pl
import pytest

from pipeline.schemas.bronze import MESSAGE_TYPE_VALUES
from pipeline.silver.audio import (
    AUDIO_CONFIDENCE_HIGH,
    AUDIO_CONFIDENCE_LOW,
    audio_confidence_expr,
)


def _frame(rows: list[tuple[str, str | None]]) -> pl.DataFrame:
    """Build a minimal frame with typed ``message_type`` + ``message_body``."""
    return pl.DataFrame(
        {
            "message_type": [r[0] for r in rows],
            "message_body": [r[1] for r in rows],
        },
        schema={
            "message_type": pl.Enum(list(MESSAGE_TYPE_VALUES)),
            "message_body": pl.String(),
        },
    )


def _confidence(rows: list[tuple[str, str | None]]) -> list[str | None]:
    df = _frame(rows)
    out = df.select(
        audio_confidence_expr(
            pl.col("message_type"), pl.col("message_body")
        ).alias("c")
    )
    return out["c"].to_list()


# ---------------------------------------------------------------------------
# Non-audio rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "message_type", ["text", "image", "sticker", "video", "document", "contact"]
)
def test_non_audio_rows_return_null(message_type: str) -> None:
    assert _confidence([(message_type, "oi quero cotar")]) == [None]


def test_non_audio_row_with_null_body_returns_null() -> None:
    assert _confidence([("text", None)]) == [None]


# ---------------------------------------------------------------------------
# Audio rows — high confidence
# ---------------------------------------------------------------------------


def test_audio_long_clean_body_is_high() -> None:
    body = "oi, quero cotar um seguro para meu corolla 2022 por favor"
    assert _confidence([("audio", body)]) == [AUDIO_CONFIDENCE_HIGH]


# ---------------------------------------------------------------------------
# Audio rows — low via noise markers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        "oi [inaudível] queria cotar",
        "oi [inaudivel] queria cotar",  # no accent
        "OI [INAUDÍVEL] QUERIA COTAR",  # uppercase still folds
        "[ruído] muito forte aqui",
        "[ruido] na gravação",
        "é [unintelligible] assim",
        "then [...] missing",
        "tenho interesse ??? em seguro",
    ],
)
def test_audio_noise_markers_trigger_low(body: str) -> None:
    assert _confidence([("audio", body)]) == [AUDIO_CONFIDENCE_LOW]


# ---------------------------------------------------------------------------
# Audio rows — low via ellipsis
# ---------------------------------------------------------------------------


def test_audio_ellipsis_triggers_low() -> None:
    assert _confidence([("audio", "oi... quero... cotar seguro")]) == [
        AUDIO_CONFIDENCE_LOW
    ]


def test_audio_two_dots_is_not_ellipsis() -> None:
    """Two dots alone are not an ellipsis — only 3+ count."""
    body = "oi. eu queria cotar um seguro para meu carro. obrigado"
    assert _confidence([("audio", body)]) == [AUDIO_CONFIDENCE_HIGH]


# ---------------------------------------------------------------------------
# Audio rows — low via short body
# ---------------------------------------------------------------------------


def test_audio_short_body_is_low() -> None:
    assert _confidence([("audio", "oi")]) == [AUDIO_CONFIDENCE_LOW]


def test_audio_whitespace_body_is_low() -> None:
    assert _confidence([("audio", "   ")]) == [AUDIO_CONFIDENCE_LOW]


def test_audio_null_body_is_low() -> None:
    assert _confidence([("audio", None)]) == [AUDIO_CONFIDENCE_LOW]


# ---------------------------------------------------------------------------
# Mixed frames — independent row classification
# ---------------------------------------------------------------------------


def test_mixed_rows_independent() -> None:
    assert _confidence(
        [
            ("text", "long text message body here really"),
            ("audio", "oi [inaudível] algo"),
            ("audio", "tudo bem queria fazer uma cotação por favor"),
            ("audio", None),
            ("sticker", None),
        ]
    ) == [
        None,
        AUDIO_CONFIDENCE_LOW,
        AUDIO_CONFIDENCE_HIGH,
        AUDIO_CONFIDENCE_LOW,
        None,
    ]
