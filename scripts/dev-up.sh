#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

# Airflow's Fernet key (32 random bytes, URL-safe base64), one per developer and never in git.
cr=$'\r'
if grep -q "^AIRFLOW__CORE__FERNET_KEY=${cr}\{0,1\}$" .env; then
  key=$(openssl rand -base64 32 | tr '+/' '-_')
  sed -i.bak "s|^AIRFLOW__CORE__FERNET_KEY=\(${cr}\{0,1\}\)$|AIRFLOW__CORE__FERNET_KEY=${key}\1|" .env && rm .env.bak
  echo "Generated a new Airflow Fernet key in .env"
fi

echo "Starting Postgres..."
docker compose up -d postgres

echo "Running Flyway migrations..."
docker compose --profile tools run --rm flyway

echo "Building and starting Airflow..."
docker compose up -d --build

cat <<'EOF'

Stack started.

- Airflow UI: http://localhost:8082
- Username: airflow
- Password: airflow

Data Hub services (Keycloak, Hasura, S3): docker compose --profile datahub up -d

EOF
