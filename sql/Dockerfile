FROM postgres:17-trixie
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-17-pgsphere && \
    rm -rf /var/lib/apt/lists/*
COPY docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/
ENV POSTGRES_HOST_AUTH_METHOD=trust
