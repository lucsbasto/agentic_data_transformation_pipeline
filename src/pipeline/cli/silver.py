"""``python -m pipeline silver`` — Bronze → Silver transform entrypoint.

The Silver CLI is the first layer that *depends on* a prior batch: it
reads the Bronze partition keyed by ``--batch-id`` and writes a Silver
partition under ``silver_root/batch_id=<id>/part-0.parquet``. The
manifest gets a ``runs`` row (``layer='silver'``) that records
``rows_in`` / ``rows_out`` / ``rows_deduped`` / ``output_path`` and a
``COMPLETED`` or ``FAILED`` status.

Idempotence
-----------

Like Bronze, Silver is a manifest-first CLI:
- An already ``COMPLETED`` run for ``(batch_id, 'silver')`` short-
  circuits with an operator-visible log line. Re-running is free.
- Any prior stale runs (``IN_PROGRESS`` from a crash, ``FAILED`` from
  a bug) are wiped before the new ``insert_run`` so a human can just
  rerun the command without manual DB fiddling.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import click
import polars as pl

from pipeline.errors import PipelineError, SilverError
from pipeline.logging import bind_context, clear_context, configure_logging, get_logger
from pipeline.paths import data_bronze_dir, data_silver_dir
from pipeline.schemas.manifest import RUN_LAYER_SILVER
from pipeline.settings import Settings
from pipeline.silver.quarantine import partition_rows
from pipeline.silver.transform import assert_silver_schema, silver_transform
from pipeline.silver.writer import write_rejected, write_silver
from pipeline.state.manifest import ManifestDB


@click.command(name="silver")
@click.option(
    "--batch-id",
    required=True,
    help="Bronze batch_id to transform (must be COMPLETED in the manifest).",
)
@click.option(
    "--bronze-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=data_bronze_dir,
    show_default=True,
    help="Root directory containing Bronze ``batch_id=<id>/`` partitions.",
)
@click.option(
    "--silver-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=data_silver_dir,
    show_default=True,
    help="Root directory under which Silver partitions are written.",
)
def silver(batch_id: str, bronze_root: Path, silver_root: Path) -> None:
    """Transform a completed Bronze batch into a Silver partition."""
    bronze_root = _safe_resolve(bronze_root, flag="--bronze-root")
    silver_root = _safe_resolve(silver_root, flag="--silver-root")
    _validate_batch_id(batch_id)

    settings = Settings.load()
    configure_logging(settings.pipeline_log_level)
    logger = get_logger("pipeline.silver")

    run_id = uuid.uuid4().hex[:12]
    clear_context()
    bind_context(run_id=run_id, batch_id=batch_id)

    logger.info(
        "silver.start",
        batch_id=batch_id,
        bronze_root=str(bronze_root),
        silver_root=str(silver_root),
    )

    try:
        _run_silver(
            run_id=run_id,
            batch_id=batch_id,
            bronze_root=bronze_root,
            silver_root=silver_root,
            settings=settings,
        )
    except PipelineError as exc:
        logger.exception(
            "silver.failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise click.ClickException(str(exc)) from exc
    finally:
        clear_context()


def _safe_resolve(path: Path, *, flag: str) -> Path:
    if any(part == ".." for part in path.parts):
        raise click.ClickException(
            f"'..' segments are not allowed in {flag}: {path!s}"
        )
    return path.resolve()


def _validate_batch_id(batch_id: str) -> None:
    """Reject path-shaped batch_ids before they reach the filesystem.

    The CLI uses ``batch_id`` to build a partition directory name
    (``batch_id=<id>``) and a logger field. A value containing path
    separators or ``..`` would escape ``bronze_root`` — treat it as
    operator error and fail before we touch anything else.
    """
    if not batch_id or "/" in batch_id or "\\" in batch_id or ".." in batch_id:
        raise click.ClickException(
            f"invalid --batch-id value {batch_id!r}: must be a bare identifier"
        )


def _run_silver(
    *,
    run_id: str,
    batch_id: str,
    bronze_root: Path,
    silver_root: Path,
    settings: Settings,
) -> None:
    logger = get_logger("pipeline.silver")
    bronze_part = bronze_root / f"batch_id={batch_id}" / "part-0.parquet"
    if not bronze_part.exists():
        raise SilverError(
            f"Bronze partition not found for batch {batch_id!r}: {bronze_part}"
        )

    with ManifestDB(settings.state_db_path()) as manifest:
        batch = manifest.get_batch(batch_id)
        if batch is None:
            raise SilverError(f"batch {batch_id!r} is not registered in the manifest")
        if not batch.is_completed:
            raise SilverError(
                f"batch {batch_id!r} is not COMPLETED (status={batch.status})"
            )

        if manifest.is_run_completed(batch_id=batch_id, layer=RUN_LAYER_SILVER):
            logger.info("silver.skip.already_completed")
            click.echo(
                f"silver already computed for batch {batch_id}; nothing to do."
            )
            return

        # Clear any stale IN_PROGRESS / FAILED run rows so the new
        # ``insert_run`` does not collide on (batch_id, layer).
        manifest.delete_runs_for(batch_id=batch_id, layer=RUN_LAYER_SILVER)

        started_at = _iso_now()
        started_wall = time.monotonic()
        manifest.insert_run(
            run_id=run_id,
            batch_id=batch_id,
            layer=RUN_LAYER_SILVER,
            started_at=started_at,
        )
        logger.info("silver.run.opened")

        try:
            lf = pl.scan_parquet(bronze_part)
            rows_in = lf.select(pl.len()).collect().item()
            # Single ``now`` is reused for ``transformed_at`` and
            # ``rejected_at`` so both lanes of a run share one wall
            # clock — easier to correlate in logs.
            now = datetime.now(tz=UTC)
            valid_lf, rejected_lf = partition_rows(lf, rejected_at=now)
            rejected_df = rejected_lf.collect()
            silver_lf = silver_transform(
                valid_lf,
                secret=settings.pipeline_lead_secret.get_secret_value(),
                silver_batch_id=batch_id,
                transformed_at=now,
            )
            df = silver_lf.collect()
            assert_silver_schema(df)
            write_result = write_silver(
                df, silver_root=silver_root, batch_id=batch_id
            )
            rejected_written: int = 0
            if rejected_df.height > 0:
                rejected_result = write_rejected(
                    rejected_df, silver_root=silver_root, batch_id=batch_id
                )
                rejected_written = rejected_result.rows_written
                logger.info(
                    "silver.rejected.written",
                    rows_rejected=rejected_written,
                    rejected_path=str(rejected_result.rejected_path),
                )
        except PipelineError as exc:
            duration_ms = int((time.monotonic() - started_wall) * 1000)
            manifest.mark_run_failed(
                run_id=run_id,
                finished_at=_iso_now(),
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            logger.exception(
                "silver.failed",
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
            )
            raise
        except Exception as exc:
            duration_ms = int((time.monotonic() - started_wall) * 1000)
            manifest.mark_run_failed(
                run_id=run_id,
                finished_at=_iso_now(),
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            logger.exception(
                "silver.failed",
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
            )
            raise SilverError(
                f"unexpected {type(exc).__name__} during silver: {exc}"
            ) from exc

        # rows_deduped excludes rejected rows so the three counters
        # (rejected + deduped + out) sum to rows_in.
        rows_deduped = (rows_in - rejected_written) - df.height
        duration_ms = int((time.monotonic() - started_wall) * 1000)
        manifest.mark_run_completed(
            run_id=run_id,
            finished_at=_iso_now(),
            duration_ms=duration_ms,
            rows_in=rows_in,
            rows_out=df.height,
            rows_deduped=rows_deduped,
            rows_rejected=rejected_written,
            output_path=str(write_result.silver_path),
        )

    logger.info(
        "silver.complete",
        rows_in=rows_in,
        rows_out=df.height,
        rows_deduped=rows_deduped,
        rows_rejected=rejected_written,
        output_path=str(write_result.silver_path),
        duration_ms=duration_ms,
    )
    click.echo(
        f"silver wrote {df.height} rows "
        f"(deduped {rows_deduped}, rejected {rejected_written}) "
        f"to {write_result.silver_path} "
        f"(run={run_id}, {duration_ms} ms)"
    )


def _iso_now() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
