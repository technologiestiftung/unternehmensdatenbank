#!/bin/bash
set -euo pipefail

create_role_and_db() {
  local db_name="$1"
  local db_user="$2"
  local db_password="$3"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<EOSQL
DO
\$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${db_user}') THEN
    CREATE ROLE ${db_user} LOGIN PASSWORD '${db_password}';
  END IF;
END
\$\$;

SELECT format('CREATE DATABASE %I OWNER %I', '${db_name}', '${db_user}')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db_name}')\gexec
EOSQL
}

create_role_and_db "$AIRFLOW_DB_NAME" "$AIRFLOW_DB_USER" "$AIRFLOW_DB_PASSWORD"
create_role_and_db "$APP_DB_NAME" "$APP_DB_USER" "$APP_DB_PASSWORD"
