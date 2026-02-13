#!/bin/bash
set -e

# Run ingest on startup, then repeat every 30 days
while true; do
    uv run python -m ztf_reference_ingest
    sleep 30d
done
