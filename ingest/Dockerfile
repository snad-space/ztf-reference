FROM python:3.14-slim-trixie
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /ingest
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY . .
RUN uv sync --frozen --no-dev --no-editable
COPY cron-entrypoint.sh /usr/local/bin/cron-entrypoint.sh
RUN chmod +x /usr/local/bin/cron-entrypoint.sh
ENTRYPOINT ["uv", "run", "python", "-m", "ztf_reference_ingest"]
