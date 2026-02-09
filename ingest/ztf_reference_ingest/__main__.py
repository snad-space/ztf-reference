"""CLI entry point for ZTF reference PSF catalog ingestion."""

from __future__ import annotations

import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click
import httpx
import psycopg

from .db import ingest_catalog
from .discover import FileRef, generate_all_refs
from .download import download_if_changed
from .fits import parse_fits

logger = logging.getLogger(__name__)


def get_conninfo() -> str:
    host = os.environ.get("DB_HOST", "sql")
    dbname = os.environ.get("DB_NAME", "ztfref")
    user = os.environ.get("DB_USER", "ingest")
    return f"host={host} dbname={dbname} user={user}"


def process_one(ref: FileRef, conninfo: str, tmpdir: str) -> tuple[str, int]:
    """Download and ingest a single file. Returns (status, row_count)."""
    conn = psycopg.connect(conninfo, autocommit=False)
    client = httpx.Client()
    try:
        result = download_if_changed(client, conn, ref, tmpdir)
        if result is None:
            return ("skipped", 0)

        catalog = parse_fits(result.filepath)
        count = ingest_catalog(
            conn,
            catalog,
            ref,
            etag=result.etag,
            last_modified=result.last_modified,
            content_length=result.content_length,
        )
        result.filepath.unlink(missing_ok=True)
        return ("ingested", count)
    except Exception:
        logger.exception("Failed to process %s", ref.path)
        return ("failed", 0)
    finally:
        client.close()
        conn.close()


def ingest_local_file(filepath: Path, conninfo: str) -> int:
    """Ingest a single local FITS file into the database."""
    catalog = parse_fits(filepath)
    ref = FileRef(
        fieldid=catalog.fieldid,
        filter=catalog.filter,
        ccdid=catalog.ccdid,
        qid=catalog.qid,
    )
    conn = psycopg.connect(conninfo, autocommit=False)
    try:
        return ingest_catalog(conn, catalog, ref)
    finally:
        conn.close()


@click.command()
@click.option("--workers", default=10, help="Number of parallel download workers")
@click.option(
    "--fieldid", type=int, multiple=True, help="Only process specific field IDs"
)
@click.option(
    "--filter",
    "filters",
    type=click.Choice(["zg", "zr", "zi"]),
    multiple=True,
    help="Only process specific filters",
)
@click.option("--dry-run", is_flag=True, help="List files without downloading")
@click.option(
    "--from-file",
    "from_files",
    type=click.Path(exists=True, path_type=Path),
    multiple=True,
    help="Ingest local FITS file(s) instead of downloading from IRSA",
)
def main(
    workers: int,
    fieldid: tuple[int, ...],
    filters: tuple[str, ...],
    dry_run: bool,
    from_files: tuple[Path, ...],
):
    """Ingest ZTF reference PSF catalog files from IRSA."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    conninfo = get_conninfo()

    if from_files:
        total_rows = 0
        for filepath in from_files:
            logger.info("Ingesting local file: %s", filepath)
            count = ingest_local_file(filepath, conninfo)
            total_rows += count
        logger.info(
            "Done: ingested %d file(s), %d rows total", len(from_files), total_rows
        )
        return

    fieldids = list(fieldid) if fieldid else None
    filter_list = list(filters) if filters else None
    refs = generate_all_refs(fieldids=fieldids, filters=filter_list)
    logger.info("Total files to process: %d", len(refs))

    if dry_run:
        for ref in refs:
            click.echo(ref.url)
        return

    stats = {"ingested": 0, "skipped": 0, "failed": 0}
    total_rows = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_one, ref, conninfo, tmpdir): ref for ref in refs
            }
            for future in as_completed(futures):
                ref = futures[future]
                status, count = future.result()
                stats[status] += 1
                total_rows += count

    logger.info(
        "Done: %d ingested (%d rows), %d skipped, %d failed",
        stats["ingested"],
        total_rows,
        stats["skipped"],
        stats["failed"],
    )


if __name__ == "__main__":
    main()
