"""Database operations for ingesting refpsfcat data."""

from __future__ import annotations

import logging

import psycopg

from .discover import FileRef
from .fits import ParsedCatalog

logger = logging.getLogger(__name__)

SOURCE_COLUMNS = (
    "fieldid", "filter", "ccdid", "qid", "sourceid",
    "xpos", "ypos", "ra", "dec", "coord",
    "flux", "sigflux", "mag", "sigmag", "snr", "chi", "sharp", "flags",
)


def ingest_catalog(
    conn: psycopg.Connection,
    catalog: ParsedCatalog,
    ref: FileRef,
    etag: str | None = None,
    last_modified: str | None = None,
    content_length: int | None = None,
) -> int:
    """Ingest a parsed catalog into the database within a single transaction.

    Returns the number of rows inserted.
    """
    with conn.transaction():
        # Upsert quadrant-level header data
        conn.execute(
            """
            INSERT INTO quadrant (fieldid, filter, ccdid, qid, magzp, magzp_rms, magzp_unc, infobits)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fieldid, filter, ccdid, qid)
            DO UPDATE SET magzp = EXCLUDED.magzp,
                          magzp_rms = EXCLUDED.magzp_rms,
                          magzp_unc = EXCLUDED.magzp_unc,
                          infobits = EXCLUDED.infobits
            """,
            (ref.fieldid, ref.filter, ref.ccdid, ref.qid,
             catalog.magzp, catalog.magzp_rms, catalog.magzp_unc, catalog.infobits),
        )

        # Replace source rows for this quadrant
        conn.execute(
            "DELETE FROM refpsfcat WHERE fieldid = %s AND filter = %s AND ccdid = %s AND qid = %s",
            (ref.fieldid, ref.filter, ref.ccdid, ref.qid),
        )

        with conn.cursor().copy(
            f"COPY refpsfcat ({', '.join(SOURCE_COLUMNS)}) FROM STDIN"
        ) as copy:
            for row in catalog.rows:
                copy.write_row(row)

        # Update ingest metadata
        conn.execute(
            """
            INSERT INTO ingest_metadata (fieldid, filter, ccdid, qid, etag, last_modified, content_length)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fieldid, filter, ccdid, qid)
            DO UPDATE SET etag = EXCLUDED.etag,
                          last_modified = EXCLUDED.last_modified,
                          content_length = EXCLUDED.content_length,
                          ingested_at = now()
            """,
            (ref.fieldid, ref.filter, ref.ccdid, ref.qid, etag, last_modified, content_length),
        )

    logger.info(
        "Ingested %d rows for field=%d filter=%s ccd=%d qid=%d",
        len(catalog.rows), ref.fieldid, ref.filter, ref.ccdid, ref.qid,
    )
    return len(catalog.rows)
