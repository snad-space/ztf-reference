#!/bin/bash
set -e
uv run python /app/wait_postgres.py
exec uv run python -m ztf_reference.main
