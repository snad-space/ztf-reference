#!/bin/bash
set -e
uv run python /app/wait_postgres.py
exec uv run gunicorn -w4 -b0.0.0.0:80 --worker-class=aiohttp.GunicornWebWorker ztf_reference.main:get_app
