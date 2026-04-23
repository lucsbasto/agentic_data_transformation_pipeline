---
name: polars-lazy-pipeline
description: Polars lazy-first pipeline patterns for Bronze→Silver→Gold transforms. Use when writing any data transform, reading parquet, or designing a new layer. Trigger keywords: polars, LazyFrame, scan_parquet, collect, streaming, DataFrame, parquet, bronze, silver, gold, transform.
---

# Polars lazy pipeline

Binding patterns for all Polars code in this pipeline (per PRD ADR-001).

## 1. Lazy-first, always

- Start every chain with `pl.scan_*` (parquet, csv, ndjson). **Never** `pl.read_*` in pipeline code.
- Chain transforms on `LazyFrame`. End with exactly one `.collect()` at the sink boundary.
- `.collect(streaming=True)` when dataset > RAM. Default otherwise.
- `eager=False` everywhere it's an option.

```python
# Good
lf = (
    pl.scan_parquet(bronze_path)
      .filter(pl.col("status") == "active")
      .with_columns(created_at=pl.col("created_at").str.to_datetime())
      .group_by("persona").agg(pl.len().alias("n"))
)
df = lf.collect()

# Bad
df = pl.read_parquet(bronze_path)   # eager load
df = df.filter(...)                  # no pushdown possible
```

## 2. Read only what you need

- Pass `columns=[...]` to `scan_parquet` when you know the subset — belt-and-suspenders with projection pushdown.
- Partition reads: `pl.scan_parquet("path/batch_id=*/part-*.parquet")` gets Polars' Hive-style partition discovery.

## 3. Expressions > apply

- Prefer native expressions (`pl.col(...)`, `when/then/otherwise`, `str.*`, `dt.*`, `list.*`, `struct.*`) over `.map_elements` (Python UDF).
- UDFs kill parallelism. Only use for LLM calls (where latency dominates anyway) and mark them explicitly.

## 4. Schema contracts per layer

Each layer has a declared schema. Enforce at write:

```python
from pipeline.schemas import BRONZE_SCHEMA

def write_bronze(lf: pl.LazyFrame, out: Path) -> None:
    df = lf.collect()
    assert df.schema == BRONZE_SCHEMA, f"schema drift: {df.schema}"
    df.write_parquet(out, compression="zstd", statistics=True)
```

Schema lives in `pipeline/schemas/<layer>.py` as `pl.Schema` literals or `{"col": pl.Dtype}` dicts. Change schema = write migration note in STATE.md.

## 5. Write: parquet with proper knobs

- `compression="zstd"` (better ratio + decode speed than snappy for this workload).
- `statistics=True` enables predicate pushdown on reads.
- Partition Bronze by `batch_id` (ingest batch). Partition Silver by a time key when queries filter on it.

## 6. No pandas conversion

- Never `.to_pandas()` except at an external integration boundary (e.g., a lib that only accepts pandas).
- Never round-trip through pandas for "familiarity". Use Polars expressions.

## 7. Nulls

- Polars distinguishes `Null` from `NaN`. Always `.is_null()`, not `== None`.
- Use `fill_null(...)` explicitly where semantics demand; otherwise leave null and document downstream handling.

## 8. Joins

- Declare join strategy: `how="left" | "inner" | "semi" | "anti" | "outer"` — never rely on default.
- Left side = larger; Polars optimizes but be explicit for clarity.
- `validate="1:1" | "1:m" | "m:1"` when you know the expected cardinality — fails loudly on violation.

## 9. Window / group-by

- `group_by(...).agg(...)` is the idiom. Avoid `groupby_dynamic` unless doing time bucketing.
- Named expressions: `.agg(pl.len().alias("n"))`, not positional.
- `over(...)` for window functions — cheaper than a self-join.

## 10. Testing transforms

- Test on small in-memory `pl.LazyFrame` built from `pl.DataFrame({...}).lazy()`.
- Snapshot the output schema (`df.schema`) and a tiny sample (head 5) as part of the test.
- Use `pytest.approx` for float columns; exact equality for ints and strings.

## 11. Performance sanity checks

- `lf.explain()` prints optimized plan. Use in unit tests to assert pushdowns applied.
- `lf.profile()` times each node. Use when a step feels slow.

## 12. Anti-patterns (do not)

- No `for row in df.iter_rows():` unless truly unavoidable (and document why).
- No `.collect()` mid-chain to "check" — use `.fetch(n=10)` or `.head(10).collect()`.
- No `df.filter(df["col"] > 0)` (pandas style). Use `pl.col("col")`.
- No materializing a `LazyFrame` just to pass it to another function that re-lazies it.
