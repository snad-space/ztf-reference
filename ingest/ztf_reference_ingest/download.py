"""Download FITS files from IRSA with etag-based caching."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx
import psycopg

from .discover import FileRef

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    content: bytes
    etag: str | None
    last_modified: str | None
    content_length: int | None


def get_stored_metadata(conn: psycopg.Connection, ref: FileRef) -> dict | None:
    """Get stored etag/last_modified/content_length for a file."""
    row = conn.execute(
        """
        SELECT etag, last_modified, content_length
        FROM ingest_metadata
        WHERE fieldid = %s AND filter = %s AND ccdid = %s AND qid = %s
        """,
        (ref.fieldid, ref.filter, ref.ccdid, ref.qid),
    ).fetchone()
    if row is None:
        return None
    return {"etag": row[0], "last_modified": row[1], "content_length": row[2]}


def download_if_changed(
    client: httpx.Client,
    conn: psycopg.Connection,
    ref: FileRef,
) -> DownloadResult | None:
    """Download a FITS file if it has changed since last ingest.

    Returns DownloadResult with path and headers, or None if unchanged.
    """
    stored = get_stored_metadata(conn, ref)

    try:
        head_resp = client.head(ref.url, timeout=30, follow_redirects=True)
        head_resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.debug("File not found: %s", ref.url)
            return None
        raise
    except httpx.HTTPError:
        logger.warning("HEAD request failed for %s", ref.url)
        return None

    headers = head_resp.headers
    etag = headers.get("etag")
    last_modified = headers.get("last-modified")
    content_length_str = headers.get("content-length")
    content_length = int(content_length_str) if content_length_str else None

    if stored is not None:
        if etag and stored["etag"] == etag:
            logger.debug("Unchanged (etag): %s", ref.path)
            return None
        if last_modified and stored["last_modified"] == last_modified:
            if content_length and stored["content_length"] == content_length:
                logger.debug("Unchanged (last-modified+size): %s", ref.path)
                return None

    logger.info("Downloading %s", ref.path)
    try:
        resp = client.get(ref.url, timeout=120, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError:
        logger.warning("Download failed: %s", ref.url)
        return None

    return DownloadResult(
        content=resp.content,
        etag=etag,
        last_modified=last_modified,
        content_length=content_length,
    )
