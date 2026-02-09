"""CLI entry point for ZTF reference PSF catalog ingestion."""

from __future__ import annotations

import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

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
def main(
    workers: int, fieldid: tuple[int, ...], filters: tuple[str, ...], dry_run: bool
):
    """Ingest ZTF reference PSF catalog files from IRSA."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    fieldids = list(fieldid) if fieldid else None
    filter_list = list(filters) if filters else None
    refs = generate_all_refs(fieldids=fieldids, filters=filter_list)
    logger.info("Total files to process: %d", len(refs))

    if dry_run:
        for ref in refs:
            click.echo(ref.url)
        return

    conninfo = get_conninfo()
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
