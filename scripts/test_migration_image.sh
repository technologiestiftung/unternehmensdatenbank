#!/bin/sh
# Builds the migration image (db/Dockerfile) and runs it twice against an EMPTY Postgres 15 that
# was set up the way the hub sets one up: the init script creates the three logins, here under
# role names that are not ours, so a migration that hardcodes `writer` or `reader` fails.
#   1st run: V1 to the newest must all apply.   2nd run: must apply nothing.
# Then checks the grants: the writer may INSERT, the reader may not UPDATE.
# Needs Docker only. Leaves nothing behind.
set -eu

net=udb-migration-test
pg=udb-migration-test-pg
cleanup() { docker rm -f "$pg" >/dev/null 2>&1 || true; docker network rm "$net" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker build -q -t udb-migrations:test db >/dev/null
docker build -q -t udb-postgres:test postgres >/dev/null
docker network create "$net" >/dev/null

docker run -d --name "$pg" --network "$net" \
  -e POSTGRES_PASSWORD=pw \
  -e AIRFLOW_DB_USER=airflow -e AIRFLOW_DB_PASSWORD=pw -e AIRFLOW_DB_NAME=airflow \
  -e APP_DB_USER=hub_owner -e APP_DB_PASSWORD=pw \
  -e APP_DB_WRITER_USER=hub_writer -e APP_DB_WRITER_PASSWORD=pw \
  -e APP_DB_READER_USER=hub_reader -e APP_DB_READER_PASSWORD=pw \
  -e APP_DB_NAME=udb \
  -e KEYCLOAK_DB_USER=keycloak -e KEYCLOAK_DB_PASSWORD=pw -e KEYCLOAK_DB_NAME=keycloak \
  -e HASURA_DB_USER=hasura -e HASURA_DB_PASSWORD=pw -e HASURA_DB_NAME=hasura \
  -v "$PWD/postgres/init:/docker-entrypoint-initdb.d:ro" \
  udb-postgres:test >/dev/null

# pg_isready turns true during the init phase too; the init script's last database is the signal.
ready=
for _ in $(seq 60); do
  docker exec "$pg" psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'hasura'" 2>/dev/null | grep -q 1 \
    && docker exec "$pg" pg_isready -U postgres -h localhost >/dev/null 2>&1 && ready=1 && break
  sleep 2
done
[ -n "$ready" ] || { echo "Postgres not ready after 120 s"; docker logs "$pg"; exit 1; }

migrate() {
  docker run --rm --network "$net" \
    -e FLYWAY_URL="jdbc:postgresql://$pg:5432/udb" \
    -e FLYWAY_USER=hub_owner -e FLYWAY_PASSWORD=pw \
    -e FLYWAY_PLACEHOLDERS_WRITER_ROLE=hub_writer \
    -e FLYWAY_PLACEHOLDERS_READER_ROLE=hub_reader \
    udb-migrations:test
}

expect() {
  out=$(migrate 2>&1) || { echo "$out"; exit 1; }
  echo "$out" | grep -E "$1" || { echo "$out"; echo "expected: $1"; exit 1; }
}

expect "Successfully applied"
expect "No migration necessary"

grants=$(docker exec "$pg" psql -U postgres -d udb -tAc \
  "SELECT has_table_privilege('hub_writer', 'public.companies', 'INSERT')::text || ',' ||
          has_table_privilege('hub_reader', 'public.companies', 'UPDATE')::text")
[ "$grants" = "true,false" ] || { echo "unexpected grants (writer INSERT, reader UPDATE): $grants"; exit 1; }
echo "migration image OK"
