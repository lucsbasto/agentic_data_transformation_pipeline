"""Synthetic fault injection for the F4 agent demo (design §14).

Drop a single fault into a target file/dir so the agent loop can be
observed recovering without a real Bronze drift event. Each kind
maps 1:1 to one of the four auto-correctable :class:`ErrorKind`
values (``schema_drift``, ``regex_break``, ``partition_missing``,
``out_of_range``).

Usage::

    python scripts/inject_fault.py --kind schema_drift \
        --target data/bronze/batch_id=demo01/part-0.parquet

    python scripts/inject_fault.py --kind partition_missing \
        --target data/bronze/batch_id=demo01

    python scripts/inject_fault.py --kind regex_break \
        --target data/raw/conversations.parquet

    python scripts/inject_fault.py --kind out_of_range \
        --target data/raw/conversations.parquet

The script is also importable — each ``inject_*`` function is a
plain helper so unit tests can drive the mutations directly.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Final

import click
import polars as pl

from pipeline.agent.types import ErrorKind

KIND_CHOICES: Final[tuple[str, ...]] = (
    ErrorKind.SCHEMA_DRIFT.value,
    ErrorKind.REGEX_BREAK.value,
    ErrorKind.PARTITION_MISSING.value,
    ErrorKind.OUT_OF_RANGE.value,
    ErrorKind.UNKNOWN.value,
)


# ---------------------------------------------------------------------------
# Per-kind injection helpers.
# ---------------------------------------------------------------------------


def inject_schema_drift(parquet_path: Path) -> None:
    """Add an unexpected column to a Bronze parquet so the next read
    triggers a ``polars.SchemaError`` (or simply diverges from
    :data:`pipeline.schemas.bronze.BRONZE_SCHEMA`)."""
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    df = pl.read_parquet(parquet_path)
    drifted = df.with_columns(
        pl.lit("injected", dtype=pl.String).alias("injected_col")
    )
    drifted.write_parquet(parquet_path)


def inject_regex_break(parquet_path: Path) -> None:
    """Replace the body of the first row of a source parquet with a
    novel currency format (``❌ R$ 1.500,00``) that the F2 regex
    won't match."""
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    df = pl.read_parquet(parquet_path)
    if df.height == 0:
        raise ValueError("source parquet is empty; nothing to mutate")
    df = df.with_columns(
        pl.when(pl.int_range(0, df.height) == 0)
        .then(pl.lit("❌ R$ 1.500,00"))
        .otherwise(pl.col("message_body"))
        .alias("message_body")
    )
    df.write_parquet(parquet_path)


def inject_partition_missing(target: Path) -> None:
    """Delete a Bronze partition directory (or single parquet file)
    so the next layer hits :class:`FileNotFoundError`."""
    if not target.exists():
        raise FileNotFoundError(target)
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()


_MIN_PARQUET_BYTES: Final[int] = 8
"""Minimum file size required for a meaningful truncation."""


def inject_unknown(parquet_path: Path) -> None:
    """Corrupt a parquet file by truncating it mid-page so that polars
    raises a generic read error (``InvalidOperationError`` or similar)
    that does not match any deterministic pattern in
    :func:`pipeline.agent.diagnoser.classify`.

    This forces the agent diagnoser to fall through to stage-2 LLM
    fallback (or return ``ErrorKind.UNKNOWN`` when the budget is zero
    or no LLM client is provided).
    """
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    data = parquet_path.read_bytes()
    if len(data) < _MIN_PARQUET_BYTES:
        raise ValueError("parquet file too small to truncate meaningfully")
    # Keep only the first half of the raw bytes — this splits the file
    # mid-row-group which polars cannot parse.
    truncated = data[: len(data) // 2]
    parquet_path.write_bytes(truncated)


def inject_out_of_range(parquet_path: Path) -> None:
    """Append one row whose ``message_body`` carries a negative
    monetary value — out of range for the F2 quarantine guard."""
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    df = pl.read_parquet(parquet_path)
    if df.height == 0:
        raise ValueError("source parquet is empty; nothing to append")
    template = df.row(0, named=True)
    template["message_body"] = "valor_pago_atual_brl=-999"
    if "message_id" in template:
        template["message_id"] = (template.get("message_id") or "m") + "_oor"
    new_row = pl.DataFrame([template], schema=df.schema)
    pl.concat([df, new_row]).write_parquet(parquet_path)


_DISPATCH = {
    ErrorKind.SCHEMA_DRIFT.value: inject_schema_drift,
    ErrorKind.REGEX_BREAK.value: inject_regex_break,
    ErrorKind.PARTITION_MISSING.value: inject_partition_missing,
    ErrorKind.OUT_OF_RANGE.value: inject_out_of_range,
    ErrorKind.UNKNOWN.value: inject_unknown,
}


def inject(kind: str, target: Path) -> None:
    """Dispatch by ``kind`` — raises :class:`click.UsageError` for
    an unknown value (so the same logic powers both CLI and unit
    tests)."""
    handler = _DISPATCH.get(kind)
    if handler is None:
        raise click.UsageError(
            f"unknown --kind {kind!r}; expected one of {KIND_CHOICES}"
        )
    handler(target)


# ---------------------------------------------------------------------------
# CLI entrypoint.
# ---------------------------------------------------------------------------


@click.command()
@click.option(
    "--kind",
    type=click.Choice(KIND_CHOICES, case_sensitive=False),
    required=True,
    help="Failure class to inject (one of the five ErrorKind values).",
)
@click.option(
    "--target",
    type=click.Path(path_type=Path),
    required=True,
    help="Target file (or directory, for partition_missing).",
)
def main(kind: str, target: Path) -> None:
    """Inject one synthetic fault into ``target`` for the agent demo."""
    try:
        inject(kind, target)
    except FileNotFoundError as exc:
        raise click.ClickException(f"target does not exist: {exc}") from exc
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"injected {kind} into {target}")


if __name__ == "__main__":
    main()
    sys.exit(0)
