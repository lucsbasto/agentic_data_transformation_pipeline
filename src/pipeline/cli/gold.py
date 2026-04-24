"""``python -m pipeline gold`` — Silver → Gold transform entrypoint.

Mirrors the Silver CLI shape (F2). Reads the Silver partition keyed by
``--batch-id`` and writes the four Gold parquet tables plus the
insights JSON under ``gold_root/<table>/batch_id=<id>/...``. The
manifest gets a ``runs`` row (``layer='gold'``) that records
``rows_in`` / ``rows_out`` / ``output_path`` and a ``COMPLETED`` /
``FAILED`` status.

Idempotence
-----------

- An already ``COMPLETED`` run for ``(batch_id, 'gold')`` short-circuits
  with an operator-visible log line. Re-running is free.
- Any prior stale runs (``IN_PROGRESS`` from a crash, ``FAILED`` from
  a bug) are wiped before the new ``insert_run`` so a human can just
  rerun the command without manual DB fiddling.

Refusal contract
----------------

Gold refuses to run unless the Silver run for the same ``batch_id`` is
``COMPLETED`` in the manifest (design §11): a missing or non-completed
Silver lineage is a ``GoldError`` with exit code 1 and no partial
writes.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import click
import polars as pl

from pipeline.errors import GoldError, PipelineError
from pipeline.gold.transform import (
    _default_persona_classifier,
    transform_gold,
)
from pipeline.logging import bind_context, clear_context, configure_logging, get_logger
from pipeline.paths import data_gold_dir, data_silver_dir
from pipeline.schemas.manifest import RUN_LAYER_GOLD, RUN_LAYER_SILVER
from pipeline.settings import Settings
from pipeline.state.manifest import ManifestDB


@click.command(name="gold")
@click.option(
    "--batch-id",
    required=True,
    help="Silver batch_id to transform (must be COMPLETED in the manifest).",
)
@click.option(
    "--silver-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=data_silver_dir,
    show_default=True,
    help="Root directory containing Silver ``batch_id=<id>/`` partitions.",
)
@click.option(
    "--gold-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=data_gold_dir,
    show_default=True,
    help="Root directory under which Gold tables and insights are written.",
)
def gold(batch_id: str, silver_root: Path, gold_root: Path) -> None:
    """Transform a completed Silver batch into the Gold tables + insights."""
    silver_root = _safe_resolve(silver_root, flag="--silver-root")
    gold_root = _safe_resolve(gold_root, flag="--gold-root")
    _validate_batch_id(batch_id)

    settings = Settings.load()
    configure_logging(settings.pipeline_log_level)
    logger = get_logger("pipeline.gold")

    run_id = uuid.uuid4().hex[:12]
    clear_context()
    bind_context(run_id=run_id, batch_id=batch_id)

    logger.info(
        "gold.start",
        batch_id=batch_id,
        silver_root=str(silver_root),
        gold_root=str(gold_root),
    )

    try:
        _run_gold(
            run_id=run_id,
            batch_id=batch_id,
            silver_root=silver_root,
            gold_root=gold_root,
            settings=settings,
        )
    except PipelineError as exc:
        logger.exception(
            "gold.failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise click.ClickException(str(exc)) from exc
    finally:
        clear_context()


def _safe_resolve(path: Path, *, flag: str) -> Path:
    if any(part == ".." for part in path.parts):
        raise click.ClickException(f"'..' segments are not allowed in {flag}: {path!s}")
    return path.resolve()


def _validate_batch_id(batch_id: str) -> None:
    """Reject path-shaped batch_ids before they reach the filesystem.

    Mirrors the Silver CLI guard: ``batch_id`` becomes a partition
    directory name and a logger field, so a value containing path
    separators or ``..`` would escape ``silver_root`` / ``gold_root``.
    """
    if not batch_id or "/" in batch_id or "\\" in batch_id or ".." in batch_id:
        raise click.ClickException(
            f"invalid --batch-id value {batch_id!r}: must be a bare identifier"
        )


def _run_gold(
    *,
    run_id: str,
    batch_id: str,
    silver_root: Path,
    gold_root: Path,
    settings: Settings,
) -> None:
    logger = get_logger("pipeline.gold")
    silver_part = silver_root / f"batch_id={batch_id}" / "part-0.parquet"

    with ManifestDB(settings.state_db_path()) as manifest:
        # Refuse to run unless Silver lineage is COMPLETED. A missing
        # Silver run shows up as ``is_run_completed == False`` and
        # surfaces as a ``GoldError`` with exit 1 (design §11).
        if not manifest.is_run_completed(batch_id=batch_id, layer=RUN_LAYER_SILVER):
            raise GoldError(
                f"silver run for batch {batch_id!r} is not COMPLETED in the "
                f"manifest; run `python -m pipeline silver --batch-id {batch_id}` first"
            )
        if not silver_part.exists():
            raise GoldError(f"Silver partition not found for batch {batch_id!r}: {silver_part}")

        if manifest.is_run_completed(batch_id=batch_id, layer=RUN_LAYER_GOLD):
            logger.info("gold.skip.already_completed")
            click.echo(f"gold already computed for batch {batch_id}; nothing to do.")
            return

        # Clear any stale IN_PROGRESS / FAILED run rows so the new
        # ``insert_run`` does not collide on (batch_id, layer).
        manifest.delete_runs_for(batch_id=batch_id, layer=RUN_LAYER_GOLD)

        started_at = _iso_now()
        started_wall = time.monotonic()
        manifest.insert_run(
            run_id=run_id,
            batch_id=batch_id,
            layer=RUN_LAYER_GOLD,
            started_at=started_at,
        )
        logger.info("gold.run.opened")

        try:
            silver_lf = pl.scan_parquet(silver_part)
            rows_in = silver_lf.select(pl.len()).collect().item()
            classifier = _default_persona_classifier(settings)
            result = transform_gold(
                silver_lf,
                batch_id=batch_id,
                gold_root=gold_root,
                persona_classifier=classifier,
                settings=settings,
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
                "gold.failed",
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
                "gold.failed",
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
            )
            raise GoldError(f"unexpected {type(exc).__name__} during gold: {exc}") from exc

        duration_ms = int((time.monotonic() - started_wall) * 1000)
        manifest.mark_run_completed(
            run_id=run_id,
            finished_at=_iso_now(),
            duration_ms=duration_ms,
            rows_in=rows_in,
            rows_out=result.rows_out,
            output_path=str(gold_root),
        )

    logger.info(
        "gold.complete",
        rows_in=rows_in,
        rows_out=result.rows_out,
        gold_root=str(gold_root),
        duration_ms=duration_ms,
    )
    click.echo(
        f"gold wrote {result.rows_out} rows across 4 tables to {gold_root} "
        f"(run={run_id}, {duration_ms} ms)"
    )


def _iso_now() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
