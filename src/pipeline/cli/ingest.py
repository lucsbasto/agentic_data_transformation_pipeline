"""``python -m pipeline ingest`` — Bronze ingest entrypoint.

Orchestrates the three-step Bronze flow (scan → transform → write) while
keeping every side effect inside the manifest transaction: the batch row
is created first in ``IN_PROGRESS``, a failure flips it to ``FAILED``
with a typed error, success promotes it to ``COMPLETED``. Re-running
against an unchanged source is a manifest hit, not a double write.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import click

from pipeline.errors import IngestError, PipelineError
from pipeline.ingest import (
    compute_batch_identity,
    scan_source,
    transform_to_bronze,
    validate_source_columns,
    write_bronze,
)
from pipeline.ingest.transform import collect_bronze
from pipeline.logging import bind_context, clear_context, configure_logging, get_logger
from pipeline.paths import data_bronze_dir, data_raw_dir
from pipeline.settings import Settings
from pipeline.state.manifest import ManifestDB


def _default_source() -> Path:
    return data_raw_dir() / "conversations_bronze.parquet"


@click.command(name="ingest")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    default=_default_source,
    show_default=True,
    help="Raw parquet to ingest into Bronze.",
)
@click.option(
    "--bronze-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=data_bronze_dir,
    show_default=True,
    help="Root directory under which Bronze partitions are written.",
)
def ingest(source: Path, bronze_root: Path) -> None:
    """Read the raw parquet, cast to Bronze, and register the batch."""
    source = _safe_resolve(source, flag="--source")
    bronze_root = _safe_resolve(bronze_root, flag="--bronze-root")

    settings = Settings.load()
    configure_logging(settings.pipeline_log_level)
    logger = get_logger("pipeline.ingest")

    run_id = uuid.uuid4().hex[:12]
    clear_context()
    bind_context(run_id=run_id)

    logger.info("ingest.start", source=str(source), bronze_root=str(bronze_root))

    try:
        _run_ingest(
            source=source,
            bronze_root=bronze_root,
            settings=settings,
        )
    except PipelineError as exc:
        logger.exception(
            "ingest.failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise click.ClickException(str(exc)) from exc
    finally:
        clear_context()


def _safe_resolve(path: Path, *, flag: str) -> Path:
    """Reject ``..`` segments in ``path`` and return its canonical form.

    Click's ``exists=True`` on a file option only checks the target
    resolves to a real file; it does not stop an operator from walking
    out of the project tree via ``..``. Mirrors the
    ``_reject_path_traversal`` validator in ``settings.py``.
    """
    if any(part == ".." for part in path.parts):
        raise click.ClickException(
            f"'..' segments are not allowed in {flag}: {path!s}"
        )
    return path.resolve()


def _run_ingest(
    *,
    source: Path,
    bronze_root: Path,
    settings: Settings,
) -> None:
    logger = get_logger("pipeline.ingest")
    identity = compute_batch_identity(source)
    bind_context(batch_id=identity.batch_id)

    with ManifestDB(settings.state_db_path()) as manifest:
        if manifest.is_batch_completed(identity.batch_id):
            logger.info("ingest.skip.already_completed")
            click.echo(f"batch {identity.batch_id} already ingested; nothing to do.")
            return

        prior = manifest.get_batch(identity.batch_id)
        if prior is not None and not prior.is_completed:
            logger.info(
                "ingest.retry.clearing_prior",
                prior_status=prior.status,
                prior_error=prior.error_type,
            )
            manifest.delete_batch(identity.batch_id)

        started_at = _iso_now()
        started_wall = time.monotonic()
        manifest.insert_batch(
            batch_id=identity.batch_id,
            source_path=str(source),
            source_hash=identity.source_hash,
            source_mtime=identity.source_mtime,
            started_at=started_at,
        )
        logger.info(
            "ingest.batch.opened",
            source_hash=identity.source_hash,
            source_mtime=identity.source_mtime,
        )

        try:
            lf = scan_source(source)
            validate_source_columns(lf)
            typed = transform_to_bronze(
                lf,
                batch_id=identity.batch_id,
                source_hash=identity.source_hash,
                ingested_at=datetime.now(tz=UTC),
            )
            df = collect_bronze(typed)
            write_result = write_bronze(
                df, bronze_root=bronze_root, batch_id=identity.batch_id
            )
        except PipelineError as exc:
            duration_ms = int((time.monotonic() - started_wall) * 1000)
            manifest.mark_failed(
                batch_id=identity.batch_id,
                finished_at=_iso_now(),
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            logger.exception(
                "ingest.failed",
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
            )
            raise
        except Exception as exc:
            # Anything not derived from PipelineError (PolarsError, OSError,
            # KeyboardInterrupt derivatives, etc.) would otherwise orphan the
            # IN_PROGRESS row and surface as a raw traceback. Mark the row
            # FAILED with the real class name, then re-raise as IngestError
            # so the outer PipelineError handler formats it uniformly.
            duration_ms = int((time.monotonic() - started_wall) * 1000)
            manifest.mark_failed(
                batch_id=identity.batch_id,
                finished_at=_iso_now(),
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            logger.exception(
                "ingest.failed",
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
            )
            raise IngestError(
                f"unexpected {type(exc).__name__} during ingest: {exc}"
            ) from exc

        duration_ms = int((time.monotonic() - started_wall) * 1000)
        manifest.mark_completed(
            batch_id=identity.batch_id,
            rows_read=df.height,
            rows_written=write_result.rows_written,
            bronze_path=str(write_result.bronze_path),
            finished_at=_iso_now(),
            duration_ms=duration_ms,
        )

    logger.info(
        "ingest.complete",
        rows_written=write_result.rows_written,
        bronze_path=str(write_result.bronze_path),
        duration_ms=duration_ms,
    )
    click.echo(
        f"ingested {write_result.rows_written} rows into {write_result.bronze_path} "
        f"(batch={identity.batch_id}, {duration_ms} ms)"
    )


def _iso_now() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
