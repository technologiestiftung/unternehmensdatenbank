#!/bin/bash
set -euo pipefail

create_role() {
  local role="$1"
  local password="$2"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<EOSQL
DO
\$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${role}') THEN
    CREATE ROLE ${role} LOGIN PASSWORD '${password}';
  END IF;
END
\$\$;
EOSQL
}

create_db() {
  local db_name="$1"
  local owner="$2"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<EOSQL
SELECT format('CREATE DATABASE %I OWNER %I', '${db_name}', '${owner}')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db_name}')\gexec
EOSQL
}

create_role "$AIRFLOW_DB_USER" "$AIRFLOW_DB_PASSWORD"
create_db "$AIRFLOW_DB_NAME" "$AIRFLOW_DB_USER"
# Was applied by hand on 2026-09-07 and lost with every fresh volume; see the pool comment in
# docker-compose.yml for why the api-server needs it.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -c "ALTER ROLE ${AIRFLOW_DB_USER} SET idle_in_transaction_session_timeout = '120s'"

# Data Hub emulation: one owner (migrations), one writer (pipelines), one reader (app, Hasura),
# and the same three roles for a staging and a prod database. The DML grants live in Flyway
# (V21), because the hub creates the roles and we only own what happens inside our database.
create_role "$APP_DB_USER" "$APP_DB_PASSWORD"
create_role "$APP_DB_WRITER_USER" "$APP_DB_WRITER_PASSWORD"
create_role "$APP_DB_READER_USER" "$APP_DB_READER_PASSWORD"
for db in "$APP_DB_NAME" "$APP_DB_STAGING_NAME"; do
  create_db "$db" "$APP_DB_USER"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<EOSQL
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS vector;
EOSQL
done

# Platform services the hub runs for us, each in its own database.
create_role "$KEYCLOAK_DB_USER" "$KEYCLOAK_DB_PASSWORD"
create_db "$KEYCLOAK_DB_NAME" "$KEYCLOAK_DB_USER"
create_role "$HASURA_DB_USER" "$HASURA_DB_PASSWORD"
create_db "$HASURA_DB_NAME" "$HASURA_DB_USER"
