from __future__ import annotations

import os

from aiohttp.web import Application
from asyncpg import create_pool

from .pg_sphere import connection_setup
from .routes import routes


async def on_startup(app: Application):
    app["pg_pool"] = await create_pool(
        host=os.environ.get("DB_HOST", "sql"),
        database=os.environ.get("DB_NAME", "ztfref"),
        user=os.environ.get("DB_USER", "app"),
        setup=connection_setup,
    )


async def on_cleanup(app: Application):
    await app["pg_pool"].close()


async def get_app() -> Application:
    app = Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.add_routes(routes)
    return app
