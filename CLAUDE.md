# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A web service that pre-ingests ZTF reference PSF catalog (`refpsfcat.fits`) files into PostgreSQL with pg_sphere, serving source lookups and cone searches via REST API. Replaces per-request FITS fetching from IRSA in the ztf.snad.space viewer.

## Build & Run

```bash
# Full stack (production)
docker compose up sql app

# Run ingest for a specific field
docker compose --profile ingest run --rm ingest --fieldid=202

# Monthly cron re-ingest (downloads only changed files via etag)
docker compose --profile ingest run --rm ingest

# Development stack (separate DB volume, dev.ref.ztf.snad.space host, ingests field 202/zg/ccd10 only)
docker compose -f docker-compose.yml -f docker-compose-dev.yml up sql app
docker compose -f docker-compose.yml -f docker-compose-dev.yml --profile ingest run --rm ingest
```

## Development Commands

Two separate Python packages with independent venvs managed by uv (Python 3.14):

```bash
# App (aiohttp web service) - run from app/
cd app && uv sync

# Ingest (batch CLI) - run from ingest/
cd ingest && uv sync

# Run ingest unit tests (from ingest/)
uv run --with pytest --with astropy pytest ../tests/test_ingest.py -v

# Run a single ingest test
uv run --with pytest --with astropy pytest ../tests/test_ingest.py::TestParseFits::test_parse_example -v

# Run app integration tests (requires pg_sphere database running)
cd app && uv run --with pytest --with pytest-aiohttp --with pytest-asyncio --with asyncpg pytest ../tests/test_routes.py -v

# Lint (from either app/ or ingest/)
uv run --with ruff ruff check .
uv run --with ruff ruff format --check .
```

## Architecture

**Three Docker services** (`sql`, `app`, `ingest`) on a shared `app` network:

- **`app/`** — aiohttp + asyncpg + gunicorn. Read-only `app` DB user. Queries the `refpsfcat_full` view which joins source data with quadrant-level header data. pg_sphere `spoint`/`scircle` types registered via text codecs in `pg_sphere.py`.
- **`ingest/`** — Synchronous batch job (psycopg3 + httpx + astropy). Read-write `ingest` DB user. Per-file transaction: upsert `quadrant`, delete+COPY `refpsfcat`, upsert `ingest_metadata`. Parallelized via ThreadPoolExecutor.
- **`sql/`** — PostgreSQL 17 + pg_sphere (Debian package). Schema init in `01-init-schema.sh`.

**Database schema** — Three tables + one view:
- `quadrant`: per-CCD-quadrant header data (magzp, magzp_rms, magzp_unc, infobits). PK: (fieldid, filter, ccdid, qid).
- `refpsfcat`: per-source data with FK to quadrant. PK: (fieldid, filter, ccdid, qid, sourceid). GIST index on `coord` (spoint).
- `ingest_metadata`: etag/last-modified for conditional re-downloads.
- `refpsfcat_full`: view joining refpsfcat + quadrant for API queries.

**API endpoints** (all under `/api/v1/`):
- `GET /source` — exact lookup by (fieldid, filter, ccdid, qid, sourceid)
- `GET /cone` — spatial search by (ra, dec, radius_arcsec), max 60", limit 1000
- `GET /health` — DB connectivity check

**Ingest pipeline** (discover → download → parse → insert):
- `discover.py`: crawls IRSA HTML listings or generates all (field, filter, ccd, qid) combos
- `download.py`: HEAD-based etag caching, skips unchanged files
- `fits.py`: astropy FITS parsing, maps FILTERID (1→zg, 2→zr, 3→zi)
- `db.py`: transactional per-quadrant upsert+COPY

## FITS File Structure

Header (HDU 0): FIELDID, CCDID, QID, FILTERID, MAGZP, MAGZPRMS, MAGZPUNC, INFOBITS.
Table (HDU 1): sourceid, xpos, ypos, ra, dec, flux, sigflux, mag, sigmag, snr, chi, sharp, flags.
Test fixture at `tests/fixtures/ztf_000202_zg_c10_q1_refpsfcat.fits`.

## Patterns from sibling projects

This follows the same patterns as `tns/` (aiohttp + asyncpg + pg_sphere codecs, gunicorn worker, wait_postgres polling, docker-compose with proxy network). The `proxy` network is external (nginx-proxy + letsencrypt).
