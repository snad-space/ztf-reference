import pytest


async def test_health(client):
    resp = await client.get("/api/v1/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ok"


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
    assert abs(data["ra"] - 24.99) < 0.001
    assert data["magzp"] == pytest.approx(26.325, abs=0.001)
    assert data["infobits"] == 16


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


async def test_cone_search(client):
    resp = await client.get(
        "/api/v1/cone",
        params={"ra": 24.99, "dec": -29.61, "radius_arcsec": 60},
    )
    assert resp.status == 200
    data = await resp.json()
    assert len(data) >= 1
    assert data[0]["fieldid"] == 202


async def test_cone_search_with_filter(client):
    resp = await client.get(
        "/api/v1/cone",
        params={"ra": 24.99, "dec": -29.61, "radius_arcsec": 60, "filter": "zr"},
    )
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 0


async def test_cone_invalid_radius(client):
    resp = await client.get(
        "/api/v1/cone",
        params={"ra": 24.99, "dec": -29.61, "radius_arcsec": 100},
    )
    assert resp.status == 400
