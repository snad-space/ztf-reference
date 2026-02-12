import pytest


async def test_health(client):
    resp = await client.get("/api/v1/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ok"


async def test_stats(client):
    resp = await client.get("/api/v1/stats")
    assert resp.status == 200
    data = await resp.json()
    assert data["approximate_source_count"] == 3
    assert data["approximate_quadrant_count"] == 2


async def test_source_found(client):
    resp = await client.get(
        "/api/v1/source",
        params={"fieldid": 202, "filter": "zg", "ccdid": 10, "qid": 1, "sourceid": 0},
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["fieldid"] == 202
    assert data["filter"] == "zg"
    assert data["sourceid"] == 0
    assert data["ra"] == pytest.approx(24.9859705, abs=1e-5)
    assert data["magzp"] == pytest.approx(26.325, abs=0.001)
    assert data["infobits"] == 16
    assert data["oid"] == "202110100000000"


async def test_source_not_found(client):
    resp = await client.get(
        "/api/v1/source",
        params={"fieldid": 999, "filter": "zg", "ccdid": 1, "qid": 1, "sourceid": 0},
    )
    assert resp.status == 404


async def test_source_missing_param(client):
    resp = await client.get(
        "/api/v1/source",
        params={"fieldid": 202, "filter": "zg"},
    )
    assert resp.status == 400


async def test_object_found(client):
    resp = await client.get("/api/v1/object", params={"oid": "202110100000000"})
    assert resp.status == 200
    data = await resp.json()
    assert data["fieldid"] == 202
    assert data["filter"] == "zg"
    assert data["ccdid"] == 10
    assert data["qid"] == 1
    assert data["sourceid"] == 0
    assert data["oid"] == "202110100000000"


async def test_object_found_zr(client):
    resp = await client.get("/api/v1/object", params={"oid": "202210100000000"})
    assert resp.status == 200
    data = await resp.json()
    assert data["fieldid"] == 202
    assert data["filter"] == "zr"
    assert data["ccdid"] == 10
    assert data["qid"] == 1
    assert data["sourceid"] == 0
    assert data["oid"] == "202210100000000"
    assert data["magzp"] == pytest.approx(26.190, abs=0.001)
    assert data["infobits"] == 0


async def test_object_not_found(client):
    resp = await client.get("/api/v1/object", params={"oid": "9991101100000000"})
    assert resp.status == 404


async def test_object_invalid(client):
    resp = await client.get("/api/v1/object", params={"oid": "abc"})
    assert resp.status == 400


async def test_object_missing_param(client):
    resp = await client.get("/api/v1/object")
    assert resp.status == 400


async def test_cone_search(client):
    resp = await client.get(
        "/api/v1/cone",
        params={"ra": 24.986, "dec": -29.609, "radius_arcsec": 60},
    )
    assert resp.status == 200
    data = await resp.json()
    assert len(data) >= 1
    assert data[0]["fieldid"] == 202


async def test_cone_search_with_filter(client):
    resp = await client.get(
        "/api/v1/cone",
        params={"ra": 24.986, "dec": -29.609, "radius_arcsec": 60, "filter": "zi"},
    )
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 0


async def test_cone_invalid_radius(client):
    resp = await client.get(
        "/api/v1/cone",
        params={"ra": 24.986, "dec": -29.609, "radius_arcsec": 100},
    )
    assert resp.status == 400
