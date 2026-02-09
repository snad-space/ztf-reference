FROM python:3.14-slim-trixie
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /ingest
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY . .
ENTRYPOINT ["uv", "run", "python", "-m", "ztf_reference_ingest"]
