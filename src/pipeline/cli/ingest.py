"""``python -m pipeline ingest`` — Bronze ingest entrypoint.

Orchestrates the three-step Bronze flow (scan → transform → write) while
keeping every side effect inside the manifest transaction: the batch row
is created first in ``IN_PROGRESS``, a failure flips it to ``FAILED``
with a typed error, success promotes it to ``COMPLETED``. Re-running
against an unchanged source is a manifest hit, not a double write.
"""

from __future__ import annotations

# LEARN: two stdlib utilities used below:
#   - ``time.monotonic()`` — a clock that NEVER goes backwards, even if
#     the system clock is adjusted. Use it for measuring durations.
#     NEVER use ``datetime.now()`` for "how long did X take" — a clock
#     change mid-run would produce negative durations.
#   - ``uuid.uuid4().hex[:12]`` — short random identifier for logs.
#     ``uuid4`` is cryptographically random; ``[:12]`` keeps log lines
#     readable while staying collision-resistant for operator grepping.
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


# LEARN: ``@click.command(name="ingest")`` turns this function into a
# CLI command named ``ingest``. ``@click.option(...)`` stacks add
# command-line flags. Order matters: decorators apply BOTTOM-UP, so
# ``--bronze-root`` is actually added before ``--source`` at the
# underlying click level — harmless, but worth knowing.
@click.command(name="ingest")
@click.option(
    "--source",
    # LEARN: ``click.Path(path_type=Path, ...)`` converts the raw
    # string to a ``pathlib.Path`` automatically. ``exists=True`` makes
    # click refuse to run if the file is missing. ``dir_okay=False``
    # narrows the accepted inputs to regular files.
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    # LEARN: passing a *callable* as ``default`` defers the lookup
    # until runtime — important because ``project_root()`` walks the
    # filesystem and we do not want that to run at import time.
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
    # LEARN: harden operator input before trusting it anywhere else. We
    # reject ``..`` segments, then canonicalize via ``.resolve()``.
    source = _safe_resolve(source, flag="--source")
    bronze_root = _safe_resolve(bronze_root, flag="--bronze-root")

    # LEARN: load env config once at the entrypoint. Deeper callees
    # receive ``settings`` as an explicit argument — no globals, no
    # hidden reads. Cleaner to test and to reason about.
    settings = Settings.load()
    configure_logging(settings.pipeline_log_level)
    logger = get_logger("pipeline.ingest")

    run_id = uuid.uuid4().hex[:12]
    # LEARN: wipe any leftover context from a previous command (tests
    # share the same process), then bind this run's correlation id. Any
    # log event emitted while this run is active will carry ``run_id``
    # without callees having to pass it explicitly.
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
        # LEARN: ``logger.exception`` logs at ERROR level AND attaches
        # the current traceback. Pair with the existing structured
        # fields so operators get both the why and the where.
        logger.exception(
            "ingest.failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        # LEARN: ``click.ClickException`` is click's "soft" failure —
        # exits with status 1 and prints the message WITHOUT a Python
        # traceback. Better UX for operators than a raw crash.
        raise click.ClickException(str(exc)) from exc
    finally:
        # LEARN: ``finally`` always runs. Clearing the context prevents
        # leaking ``run_id`` into a *next* command in the same process.
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
    # LEARN: compute the deterministic batch id BEFORE opening the DB.
    # If the input file is broken (unreadable), we fail fast without
    # leaving an ``IN_PROGRESS`` row behind.
    identity = compute_batch_identity(source)
    bind_context(batch_id=identity.batch_id)

    # LEARN: ``with ManifestDB(...) as manifest`` uses the context-
    # manager protocol from ``state/manifest.py``. ``__enter__`` opens
    # the connection, ``__exit__`` closes it — even if the ``with``
    # body raises. No manual ``close()`` in the happy path.
    with ManifestDB(settings.state_db_path()) as manifest:
        # LEARN: idempotency check #1 — the exact batch is already
        # COMPLETED. Short-circuit with a log line and an operator-
        # friendly echo. Re-running the ingest CLI on an unchanged file
        # is therefore free (no double write, no wasted time).
        if manifest.is_batch_completed(identity.batch_id):
            logger.info("ingest.skip.already_completed")
            click.echo(f"batch {identity.batch_id} already ingested; nothing to do.")
            return

        # LEARN: idempotency check #2 — the batch exists but is NOT
        # completed (``IN_PROGRESS`` from a crash, ``FAILED`` from a
        # previous bug). ``reset_stale`` sweeps only IN_PROGRESS, so a
        # FAILED row would collide on the primary key here. We delete
        # the stale row explicitly. ON DELETE CASCADE in the DDL means
        # any child ``runs`` rows drop with it — no orphans.
        prior = manifest.get_batch(identity.batch_id)
        if prior is not None and not prior.is_completed:
            logger.info(
                "ingest.retry.clearing_prior",
                prior_status=prior.status,
                prior_error=prior.error_type,
            )
            manifest.delete_batch(identity.batch_id)

        started_at = _iso_now()
        # LEARN: capture ``time.monotonic()`` for a duration stopwatch,
        # but use ``_iso_now()`` (wall clock) for the timestamp shown
        # in logs and the manifest. Two clocks, two purposes.
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
            # LEARN: the Bronze flow — lazy scan, validate columns,
            # build typed lazy plan, collect, write to parquet.
            # Everything stays lazy until ``collect_bronze`` forces
            # execution, which is where Polars actually runs the casts.
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
            # LEARN: first failure branch — our own typed errors. Mark
            # the batch FAILED with the real class name, then re-raise
            # to bubble up to the outer click handler.
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
            # LEARN: second failure branch — exceptions that are NOT
            # ``PipelineError`` (PolarsError, OSError, KeyboardInterrupt
            # derivatives, etc.). Without this branch they would orphan
            # the IN_PROGRESS row and surface as a raw traceback. We
            # mark FAILED with the real class name, then re-raise as
            # ``IngestError`` so the outer handler formats it uniformly.
            # The ``raise ... from exc`` keeps the original traceback.
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

        # LEARN: happy path — commit the batch row as COMPLETED with
        # the row counts, bronze path, and total duration. Only reached
        # when no exception fired inside the ``try`` block above.
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
