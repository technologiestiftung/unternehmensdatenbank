# Postgres 15 with PostGIS and pgvector, the version and extensions the Berlin Data Hub runs
# (CIVITAS/CORE `pg_version: "15"`). Built from the official image instead of postgis/postgis
# because that one has no arm64 build; the Debian packages below come from the PGDG apt repo the
# official image already configures.
FROM postgres:15

RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-15-postgis-3 postgresql-15-pgvector \
 && rm -rf /var/lib/apt/lists/*
