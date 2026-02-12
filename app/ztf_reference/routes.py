from __future__ import annotations

from aiohttp.web import (
    RouteTableDef,
    Request,
    Response,
    json_response,
    HTTPBadRequest,
    HTTPNotFound,
)

from .pg_sphere import SCircle, SPoint

routes = RouteTableDef()

MAX_RADIUS_ARCSEC = 60.0
MAX_CONE_RESULTS = 1000

FILTER_ID_TO_NAME = {"1": "zg", "2": "zr", "3": "zi"}
FILTER_NAME_TO_ID = {v: k for k, v in FILTER_ID_TO_NAME.items()}

API_DOCS_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ZTF Reference PSF Catalog API</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; max-width: 800px; margin: 2rem auto; padding: 0 1rem; line-height: 1.6; color: #333; }
  h1 { border-bottom: 2px solid #0066cc; padding-bottom: 0.5rem; }
  h2 { color: #0066cc; margin-top: 2rem; }
  code { background: #f4f4f4; padding: 0.15em 0.4em; border-radius: 3px; font-size: 0.9em; }
  pre { background: #f4f4f4; padding: 1rem; border-radius: 6px; overflow-x: auto; }
  pre code { padding: 0; background: none; }
  table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
  th, td { border: 1px solid #ddd; padding: 0.5rem 0.75rem; text-align: left; }
  th { background: #f4f4f4; }
  .endpoint { background: #e8f4fd; padding: 1rem; border-left: 4px solid #0066cc; margin: 1rem 0; border-radius: 0 6px 6px 0; }
  .method { font-weight: bold; color: #0066cc; }
  a { color: #0066cc; }
</style>
</head>
<body>
<h1>ZTF Reference PSF Catalog API</h1>
<p>
  REST API for querying pre-ingested ZTF reference PSF catalog
  (<code>refpsfcat.fits</code>) data. Part of the
  <a href="https://ztf.snad.space">ztf.snad.space</a> viewer infrastructure.
</p>

<h2>Endpoints</h2>

<div class="endpoint">
  <p><span class="method">GET</span> <code>/api/v1/source</code></p>
  <p>Exact lookup of a single source by its composite key.</p>
  <p><strong>Required parameters:</strong></p>
  <table>
    <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
    <tr><td><code>fieldid</code></td><td>int</td><td>ZTF field ID</td></tr>
    <tr><td><code>filter</code></td><td>string</td><td>Filter name: <code>zg</code>, <code>zr</code>, or <code>zi</code></td></tr>
    <tr><td><code>ccdid</code></td><td>int</td><td>CCD ID (1&ndash;16)</td></tr>
    <tr><td><code>qid</code></td><td>int</td><td>Quadrant ID (1&ndash;4)</td></tr>
    <tr><td><code>sourceid</code></td><td>int</td><td>Source ID within the quadrant</td></tr>
  </table>
  <p><strong>Example:</strong>
    <a href="/api/v1/source?fieldid=202&amp;filter=zg&amp;ccdid=10&amp;qid=1&amp;sourceid=5">/api/v1/source?fieldid=202&amp;filter=zg&amp;ccdid=10&amp;qid=1&amp;sourceid=5</a></p>
  <p>Returns a single JSON object with all source fields, or 404 if not found.</p>
</div>

<div class="endpoint">
  <p><span class="method">GET</span> <code>/api/v1/object</code></p>
  <p>Lookup a source by its ZTF DR object ID.</p>
  <p><strong>Required parameters:</strong></p>
  <table>
    <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
    <tr><td><code>oid</code></td><td>string</td><td>ZTF DR object ID (<code>{fieldid}{filter_id}{ccdid:02}{qid}{sourceid:08d}</code>)</td></tr>
  </table>
  <p><strong>Example:</strong>
    <a href="/api/v1/object?oid=2021101100000005">/api/v1/object?oid=2021101100000005</a></p>
  <p>Returns a single JSON object with all source fields, or 404 if not found.</p>
</div>

<div class="endpoint">
  <p><span class="method">GET</span> <code>/api/v1/cone</code></p>
  <p>Spatial cone search around a sky position.</p>
  <p><strong>Required parameters:</strong></p>
  <table>
    <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
    <tr><td><code>ra</code></td><td>float</td><td>Right ascension in degrees</td></tr>
    <tr><td><code>dec</code></td><td>float</td><td>Declination in degrees</td></tr>
    <tr><td><code>radius_arcsec</code></td><td>float</td><td>Search radius in arcseconds (0&ndash;60)</td></tr>
  </table>
  <p><strong>Optional parameters:</strong></p>
  <table>
    <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
    <tr><td><code>filter</code></td><td>string</td><td>Restrict to filter: <code>zg</code>, <code>zr</code>, or <code>zi</code></td></tr>
    <tr><td><code>fieldid</code></td><td>int</td><td>Restrict to a specific field ID</td></tr>
  </table>
  <p><strong>Example:</strong>
    <a href="/api/v1/cone?ra=100.0&amp;dec=40.0&amp;radius_arcsec=10&amp;filter=zr">/api/v1/cone?ra=100.0&amp;dec=40.0&amp;radius_arcsec=10&amp;filter=zr</a></p>
  <p>Returns a JSON array of matching sources (up to 1000), ordered by distance.</p>
</div>

<div class="endpoint">
  <p><span class="method">GET</span> <code>/api/v1/stats</code></p>
  <p>Approximate row counts: <a href="/api/v1/stats">/api/v1/stats</a>.
    Returns approximate number of sources and quadrants (based on PostgreSQL catalog statistics, updated by autovacuum).</p>
</div>

<div class="endpoint">
  <p><span class="method">GET</span> <code>/api/v1/health</code></p>
  <p>Health check: <a href="/api/v1/health">/api/v1/health</a>.
    Returns <code>{"status": "ok"}</code> if the database is reachable.</p>
</div>

<h2>Response fields</h2>
<table>
  <tr><th>Field</th><th>Type</th><th>Description</th></tr>
  <tr><td><code>oid</code></td><td>string</td><td>ZTF DR object ID</td></tr>
  <tr><td><code>fieldid</code></td><td>int</td><td>ZTF field ID</td></tr>
  <tr><td><code>filter</code></td><td>string</td><td>Filter name</td></tr>
  <tr><td><code>ccdid</code></td><td>int</td><td>CCD ID</td></tr>
  <tr><td><code>qid</code></td><td>int</td><td>Quadrant ID</td></tr>
  <tr><td><code>sourceid</code></td><td>int</td><td>Source ID</td></tr>
  <tr><td><code>xpos</code></td><td>float</td><td>X pixel position</td></tr>
  <tr><td><code>ypos</code></td><td>float</td><td>Y pixel position</td></tr>
  <tr><td><code>ra</code></td><td>float</td><td>Right ascension (deg)</td></tr>
  <tr><td><code>dec</code></td><td>float</td><td>Declination (deg)</td></tr>
  <tr><td><code>flux</code></td><td>float</td><td>PSF flux</td></tr>
  <tr><td><code>sigflux</code></td><td>float</td><td>Flux uncertainty</td></tr>
  <tr><td><code>mag</code></td><td>float</td><td>PSF magnitude</td></tr>
  <tr><td><code>sigmag</code></td><td>float</td><td>Magnitude uncertainty</td></tr>
  <tr><td><code>snr</code></td><td>float</td><td>Signal-to-noise ratio</td></tr>
  <tr><td><code>chi</code></td><td>float</td><td>PSF fit chi</td></tr>
  <tr><td><code>sharp</code></td><td>float</td><td>PSF fit sharpness</td></tr>
  <tr><td><code>flags</code></td><td>int</td><td>Source flags</td></tr>
  <tr><td><code>magzp</code></td><td>float</td><td>Magnitude zero point</td></tr>
  <tr><td><code>magzp_rms</code></td><td>float</td><td>Zero point RMS</td></tr>
  <tr><td><code>magzp_unc</code></td><td>float</td><td>Zero point uncertainty</td></tr>
  <tr><td><code>infobits</code></td><td>int</td><td>Info bit flags</td></tr>
</table>
<p>Float fields may be <code>null</code> when the original value is NaN.</p>

</body>
</html>
"""


@routes.get("/")
async def index(request: Request) -> Response:
    return Response(text=API_DOCS_HTML, content_type="text/html")


RESULT_COLUMNS = (
    "fieldid",
    "filter",
    "ccdid",
    "qid",
    "sourceid",
    "xpos",
    "ypos",
    "ra",
    "dec",
    "flux",
    "sigflux",
    "mag",
    "sigmag",
    "snr",
    "chi",
    "sharp",
    "flags",
    "magzp",
    "magzp_rms",
    "magzp_unc",
    "infobits",
)

SELECT_COLS = ", ".join(RESULT_COLUMNS)


def _row_to_dict(row) -> dict:
    result = {}
    for col in RESULT_COLUMNS:
        val = row[col]
        if isinstance(val, float) and val != val:
            val = None
        result[col] = val
    result["oid"] = _build_object_id(
        row["fieldid"], row["filter"], row["ccdid"], row["qid"], row["sourceid"]
    )
    return result


@routes.get("/api/v1/health")
async def health(request: Request) -> Response:
    async with request.app["pg_pool"].acquire() as con:
        await con.fetchval("SELECT 1")
    return json_response({"status": "ok"})


@routes.get("/api/v1/source")
async def source(request: Request) -> Response:
    try:
        fieldid = int(request.query["fieldid"])
        filt = request.query["filter"]
        ccdid = int(request.query["ccdid"])
        qid = int(request.query["qid"])
        sourceid = int(request.query["sourceid"])
    except KeyError as e:
        raise HTTPBadRequest(reason=f"Missing required parameter: {e}")
    except ValueError as e:
        raise HTTPBadRequest(reason=f"Invalid parameter value: {e}")

    if filt not in ("zg", "zr", "zi"):
        raise HTTPBadRequest(reason='filter must be one of "zg", "zr", "zi"')

    async with request.app["pg_pool"].acquire() as con:
        row = await con.fetchrow(
            f"""
            SELECT {SELECT_COLS}
            FROM refpsfcat_full
            WHERE fieldid = $1 AND filter = $2 AND ccdid = $3 AND qid = $4 AND sourceid = $5
            """,
            fieldid,
            filt,
            ccdid,
            qid,
            sourceid,
        )

    if row is None:
        raise HTTPNotFound(reason="Source not found")

    return json_response(_row_to_dict(row))


def _parse_object_id(oid: str) -> tuple[int, str, int, int, int]:
    """Parse ZTF DR object ID into (fieldid, filter, ccdid, qid, sourceid).

    Format: {fieldid}{filter_id}{ccdid:02}{qid}{sourceid:08d}
    The last 12 characters are fixed-width; fieldid is variable-length.
    """
    if len(oid) < 13 or not oid.isdigit():
        raise ValueError(f"Invalid object ID: {oid!r}")
    fieldid = int(oid[:-12])
    filter_id = oid[-12]
    if filter_id not in FILTER_ID_TO_NAME:
        raise ValueError(f"Invalid filter ID in object ID: {filter_id}")
    filt = FILTER_ID_TO_NAME[filter_id]
    ccdid = int(oid[-11:-9])
    qid = int(oid[-9])
    sourceid = int(oid[-8:])
    return fieldid, filt, ccdid, qid, sourceid


def _build_object_id(
    fieldid: int, filt: str, ccdid: int, qid: int, sourceid: int
) -> str:
    """Build ZTF DR object ID from components."""
    return f"{fieldid}{FILTER_NAME_TO_ID[filt]}{ccdid:02d}{qid}{sourceid:08d}"


@routes.get("/api/v1/object")
async def object_lookup(request: Request) -> Response:
    try:
        oid = request.query["oid"]
    except KeyError:
        raise HTTPBadRequest(reason='Missing required parameter: "oid"')

    try:
        fieldid, filt, ccdid, qid, sourceid = _parse_object_id(oid)
    except ValueError as e:
        raise HTTPBadRequest(reason=str(e))

    async with request.app["pg_pool"].acquire() as con:
        row = await con.fetchrow(
            f"""
            SELECT {SELECT_COLS}
            FROM refpsfcat_full
            WHERE fieldid = $1 AND filter = $2 AND ccdid = $3 AND qid = $4 AND sourceid = $5
            """,
            fieldid,
            filt,
            ccdid,
            qid,
            sourceid,
        )

    if row is None:
        raise HTTPNotFound(reason="Source not found")

    return json_response(_row_to_dict(row))


@routes.get("/api/v1/stats")
async def stats(request: Request) -> Response:
    async with request.app["pg_pool"].acquire() as con:
        rows = await con.fetch(
            """
            SELECT relname, reltuples::bigint AS approximate_row_count
            FROM pg_class
            WHERE relname IN ('refpsfcat', 'quadrant')
            """
        )
    counts = {row["relname"]: row["approximate_row_count"] for row in rows}
    return json_response(
        {
            "approximate_source_count": counts.get("refpsfcat", 0),
            "approximate_quadrant_count": counts.get("quadrant", 0),
        }
    )


@routes.get("/api/v1/cone")
async def cone(request: Request) -> Response:
    try:
        ra = float(request.query["ra"])
        dec = float(request.query["dec"])
        radius_arcsec = float(request.query["radius_arcsec"])
    except KeyError:
        raise HTTPBadRequest(
            reason='All of "ra", "dec" and "radius_arcsec" must be specified'
        )
    except ValueError:
        raise HTTPBadRequest(reason='"ra", "dec" and "radius_arcsec" must be floats')

    if radius_arcsec <= 0 or radius_arcsec > MAX_RADIUS_ARCSEC:
        raise HTTPBadRequest(
            reason=f'"radius_arcsec" must be positive and at most {MAX_RADIUS_ARCSEC}'
        )

    circle = SCircle(point=SPoint(ra=ra, dec=dec), radius_arcsec=radius_arcsec)

    params: list = [circle]
    where_extra = ""

    filt = request.query.get("filter")
    if filt is not None:
        if filt not in ("zg", "zr", "zi"):
            raise HTTPBadRequest(reason='filter must be one of "zg", "zr", "zi"')
        params.append(filt)
        where_extra += f" AND filter = ${len(params)}"

    fieldid = request.query.get("fieldid")
    if fieldid is not None:
        try:
            fieldid = int(fieldid)
        except ValueError:
            raise HTTPBadRequest(reason="fieldid must be an integer")
        params.append(fieldid)
        where_extra += f" AND fieldid = ${len(params)}"

    async with request.app["pg_pool"].acquire() as con:
        rows = await con.fetch(
            f"""
            SELECT {SELECT_COLS}
            FROM refpsfcat_full
            WHERE coord <@ $1::scircle{where_extra}
            ORDER BY coord <-> $1::scircle
            LIMIT {MAX_CONE_RESULTS}
            """,
            *params,
        )

    return json_response([_row_to_dict(row) for row in rows])
