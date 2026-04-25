"""``SCHEMA_DRIFT`` fix — re-emit a Bronze partition under the
canonical schema (F4 design §8.1).

The repair is intentionally narrow:

- Extra columns are dropped.
- Missing columns are filled with a typed ``NULL``.
- Existing columns are cast to the canonical dtype with
  ``strict=False`` so ``Enum`` values that no longer fit collapse to
  ``NULL`` rather than abort the whole batch.

The rewritten parquet replaces the original via the same temp-then-
rename atomic pattern Bronze / Silver / Gold use elsewhere — readers
either see the previous bytes or the new bytes, never a torn write.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from pipeline.agent.types import Fix
from pipeline.errors import AgentError
from pipeline.schemas.bronze import BRONZE_SCHEMA

_FIX_KIND: str = "schema_drift_repair"
_DELTA_MSG_MAX: int = 512


class SchemaDriftFixError(AgentError):
    """Raised when the partition expected by the fix is missing."""


@dataclass(frozen=True)
class SchemaDelta:
    """Diff between the Bronze partition's actual schema and the
    canonical :data:`BRONZE_SCHEMA`."""

    extra_cols: tuple[str, ...] = field(default_factory=tuple)
    missing_cols: tuple[str, ...] = field(default_factory=tuple)
    type_mismatches: tuple[tuple[str, str, str], ...] = field(default_factory=tuple)
    """Tuple of ``(column, actual_dtype, expected_dtype)`` triples."""

    @property
    def is_empty(self) -> bool:
        return not (self.extra_cols or self.missing_cols or self.type_mismatches)


def detect_delta(actual: pl.Schema, expected: pl.Schema = BRONZE_SCHEMA) -> SchemaDelta:
    """Return a :class:`SchemaDelta` describing how ``actual`` differs
    from ``expected``."""
    actual_names = set(actual.names())
    expected_names = set(expected.names())
    extra = tuple(sorted(actual_names - expected_names))
    missing = tuple(sorted(expected_names - actual_names))
    mismatches: list[tuple[str, str, str]] = []
    for name in sorted(actual_names & expected_names):
        if actual[name] != expected[name]:
            mismatches.append((name, str(actual[name]), str(expected[name])))
    return SchemaDelta(
        extra_cols=extra,
        missing_cols=missing,
        type_mismatches=tuple(mismatches),
    )


def format_delta_message(delta: SchemaDelta) -> str:
    """Render a human-readable summary, hard-capped to 512 chars so it
    safely fits ``agent_failures.last_error_msg`` (spec §4.2)."""
    parts: list[str] = []
    if delta.extra_cols:
        parts.append("extra=" + ",".join(delta.extra_cols))
    if delta.missing_cols:
        parts.append("missing=" + ",".join(delta.missing_cols))
    if delta.type_mismatches:
        joined = ",".join(
            f"{name}({actual}->{expected})"
            for name, actual, expected in delta.type_mismatches
        )
        parts.append("type_mismatch=" + joined)
    if not parts:
        return "schema_drift: no delta detected"
    return ("schema_drift: " + "; ".join(parts))[:_DELTA_MSG_MAX]


def repair_bronze_partition(
    parquet_path: Path, *, expected: pl.Schema = BRONZE_SCHEMA
) -> SchemaDelta:
    """Conform the parquet at ``parquet_path`` to ``expected``.

    Idempotent — calling twice on an already-canonical parquet
    returns an empty :class:`SchemaDelta` and rewrites the same bytes.
    Raises :class:`SchemaDriftFixError` if the file does not exist.
    """
    if not parquet_path.exists():
        raise SchemaDriftFixError(
            f"bronze partition missing for schema_drift repair: {parquet_path}"
        )
    df = pl.read_parquet(parquet_path)
    delta = detect_delta(df.schema, expected=expected)
    repaired = _conform_to_schema(df, expected)
    _atomic_write_parquet(repaired, parquet_path)
    return delta


def build_fix(parquet_path: Path) -> Fix:
    """Wrap :func:`repair_bronze_partition` in a :class:`Fix` the
    executor can ``apply()`` directly."""
    return Fix(
        kind=_FIX_KIND,
        description=f"re-emit Bronze partition under canonical schema: {parquet_path}",
        apply=lambda: _apply_repair(parquet_path),
        requires_llm=False,
    )


# ---------------------------------------------------------------------------
# Internals.
# ---------------------------------------------------------------------------


def _apply_repair(parquet_path: Path) -> None:
    """Adapter so :class:`Fix` callable returns ``None`` (the dataclass
    field's declared return type) while the underlying function still
    returns the delta for tests and the executor's logging hook."""
    repair_bronze_partition(parquet_path)


def _conform_to_schema(df: pl.DataFrame, expected: pl.Schema) -> pl.DataFrame:
    """Build a new frame whose columns + dtypes match ``expected``.

    Strategy:

    1. Drop columns that are not in ``expected``.
    2. For each canonical column, either cast the existing column to
       the expected dtype (``strict=False`` so out-of-Enum values
       become ``NULL`` instead of aborting) or insert a typed null
       column when the input lacks it.
    3. Reorder to ``expected.names()`` so the rewritten parquet is
       byte-stable across repairs.
    """
    actual_names = set(df.columns)
    selections: list[pl.Expr] = []
    for name in expected.names():
        dtype = expected[name]
        if name in actual_names:
            selections.append(pl.col(name).cast(dtype, strict=False).alias(name))
        else:
            selections.append(pl.lit(None).cast(dtype).alias(name))
    return df.select(selections)


def _atomic_write_parquet(df: pl.DataFrame, dest: Path) -> None:
    """Write ``df`` to ``dest`` via the temp-then-rename pattern used
    elsewhere in the pipeline. ``Path.replace`` is atomic on POSIX.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    df.write_parquet(tmp)
    tmp.replace(dest)
