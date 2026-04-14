#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

echo "Starting Postgres..."
docker compose up -d postgres

echo "Running Flyway migrations..."
docker compose --profile tools run --rm flyway

echo "Starting Airflow..."
docker compose up -d

cat <<'EOF'

Stack started.

- Airflow UI: http://localhost:8080
- Username: airflow
- Password: airflow

EOF
