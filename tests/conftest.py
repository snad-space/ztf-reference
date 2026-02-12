import os

import pytest

# Only load app-related fixtures when asyncpg is available (app test environment)
try:
    import asyncpg
    import pytest_asyncio
    from ztf_reference.main import get_app
    from ztf_reference.pg_sphere import connection_setup

    @pytest.fixture
    def db_params():
        return {
            "host": os.environ.get("TEST_DB_HOST", "localhost"),
            "database": os.environ.get("TEST_DB_NAME", "ztfref"),
            "user": os.environ.get("TEST_DB_USER", "ztfref"),
        }

    @pytest_asyncio.fixture
    async def seed_db(db_params):
        """Insert test data into the database."""
        pool = await asyncpg.create_pool(**db_params, setup=connection_setup)
        async with pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO quadrant (fieldid, filter, ccdid, qid, magzp, magzp_rms, magzp_unc, infobits)
                VALUES
                    (202, 'zg', 10, 1, 26.325, 0.0873030188, 0.0004818736, 16),
                    (202, 'zr', 10, 1, 26.190, 0.0650000000, 0.0003500000, 0)
                ON CONFLICT DO NOTHING
                """
            )
            await con.execute(
                """
                INSERT INTO refpsfcat (fieldid, filter, ccdid, qid, sourceid, xpos, ypos, ra, dec, coord,
                                       flux, sigflux, mag, sigmag, snr, chi, sharp, flags)
                VALUES
                    (202, 'zg', 10, 1, 0, 119.791, 61.432, 24.9859705, -29.6089428,
                     spoint(radians(24.9859705), radians(-29.6089428)),
                     237.02818, 18.01066, -5.937, 0.083, 13.16, 1.009, -0.058, 0),
                    (202, 'zg', 10, 1, 1, 1354.238, 62.677, 25.3803179, -29.6047335,
                     spoint(radians(25.3803179), radians(-29.6047335)),
                     68.48572, 21.969954, -4.589, 0.348, 3.12, 1.459, -0.452, 0),
                    (202, 'zr', 10, 1, 0, 119.791, 61.432, 24.9859705, -29.6089428,
                     spoint(radians(24.9859705), radians(-29.6089428)),
                     310.50000, 15.20000, -6.230, 0.053, 20.43, 0.995, -0.041, 0)
                ON CONFLICT DO NOTHING
                """
            )
            await con.execute("ANALYZE quadrant")
            await con.execute("ANALYZE refpsfcat")
        await pool.close()

    @pytest_asyncio.fixture
    async def client(aiohttp_client, seed_db, db_params):
        os.environ["DB_HOST"] = db_params["host"]
        os.environ["DB_NAME"] = db_params["database"]
        os.environ["DB_USER"] = db_params["user"]
        app = await get_app()
        return await aiohttp_client(app)

except ImportError:
    pass
