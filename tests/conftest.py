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
                VALUES (202, 'zg', 10, 1, 26.325, 0.037, 0.015, 16)
                ON CONFLICT DO NOTHING
                """
            )
            await con.execute(
                """
                INSERT INTO refpsfcat (fieldid, filter, ccdid, qid, sourceid, xpos, ypos, ra, dec, coord,
                                       flux, sigflux, mag, sigmag, snr, chi, sharp, flags)
                VALUES
                    (202, 'zg', 10, 1, 0, 100.5, 200.3, 24.99, -29.61,
                     spoint(radians(24.99), radians(-29.61)),
                     1000.0, 10.0, 18.5, 0.01, 100.0, 1.0, 0.01, 0),
                    (202, 'zg', 10, 1, 1, 150.2, 300.1, 25.01, -29.60,
                     spoint(radians(25.01), radians(-29.60)),
                     800.0, 12.0, 18.8, 0.02, 66.7, 1.1, 0.02, 0)
                ON CONFLICT DO NOTHING
                """
            )
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
