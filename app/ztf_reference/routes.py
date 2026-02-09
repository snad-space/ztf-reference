from __future__ import annotations

from aiohttp.web import RouteTableDef, Request, Response, json_response, HTTPBadRequest, HTTPNotFound

from .pg_sphere import SCircle, SPoint

routes = RouteTableDef()

MAX_RADIUS_ARCSEC = 60.0
MAX_CONE_RESULTS = 1000

RESULT_COLUMNS = (
    "fieldid", "filter", "ccdid", "qid", "sourceid",
    "xpos", "ypos", "ra", "dec",
    "flux", "sigflux", "mag", "sigmag", "snr", "chi", "sharp", "flags",
    "magzp", "magzp_rms", "magzp_unc", "infobits",
)

SELECT_COLS = ", ".join(RESULT_COLUMNS)


def _row_to_dict(row) -> dict:
    result = {}
    for col in RESULT_COLUMNS:
        val = row[col]
        if isinstance(val, float) and val != val:
            val = None
        result[col] = val
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
            fieldid, filt, ccdid, qid, sourceid,
        )

    if row is None:
        raise HTTPNotFound(reason="Source not found")

    return json_response(_row_to_dict(row))


@routes.get("/api/v1/cone")
async def cone(request: Request) -> Response:
    try:
        ra = float(request.query["ra"])
        dec = float(request.query["dec"])
        radius_arcsec = float(request.query["radius_arcsec"])
    except KeyError:
        raise HTTPBadRequest(reason='All of "ra", "dec" and "radius_arcsec" must be specified')
    except ValueError:
        raise HTTPBadRequest(reason='"ra", "dec" and "radius_arcsec" must be floats')

    if radius_arcsec <= 0 or radius_arcsec > MAX_RADIUS_ARCSEC:
        raise HTTPBadRequest(reason=f'"radius_arcsec" must be positive and at most {MAX_RADIUS_ARCSEC}')

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
