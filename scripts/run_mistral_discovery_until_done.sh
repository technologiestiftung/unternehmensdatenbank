#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

env_value() {
  local key="$1"
  local value
  value="$(grep "^${key}=" .env | cut -d= -f2- || true)"
  printf '%s' "$value"
}

pending_count() {
  docker compose exec -T postgres psql -U postgres -d unternehmensdatenbank -Atqc "
    SELECT count(*)
    FROM public.companies c
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.company_source_records csr
      JOIN public.sources s ON s.id = csr.source_id
      WHERE csr.company_id = c.id
        AND s.key = 'mistral_search_discovery'
        AND COALESCE(csr.source_payload ->> 'status', '') <> 'planned'
    );
  "
}

checkpoint_value() {
  if [[ -f airflow/data/mistral_search_discovery_checkpoint.json ]]; then
    python3 - <<'PY'
import json
from pathlib import Path
p = Path("airflow/data/mistral_search_discovery_checkpoint.json")
try:
    data = json.loads(p.read_text())
    print(data.get("last_company_id") or "")
except Exception:
    print("")
PY
  else
    printf '%s' ""
  fi
}

run_once() {
  local mistral_api_key mistral_model mistral_search_tool mistral_discovery_max_results
  local mistral_request_delay_s mistral_http_timeout_s mistral_http_max_retries
  local mistral_retry_max_wait_s mistral_progress_log_every mistral_checkpoint_file
  local mistral_concurrency mistral_submit_delay_s

  mistral_api_key="$(env_value MISTRAL_API_KEY)"
  mistral_model="$(env_value MISTRAL_MODEL)"
  mistral_search_tool="$(env_value MISTRAL_SEARCH_TOOL)"
  mistral_discovery_max_results="$(env_value MISTRAL_DISCOVERY_MAX_RESULTS)"
  mistral_request_delay_s="$(env_value MISTRAL_REQUEST_DELAY_S)"
  mistral_http_timeout_s="$(env_value MISTRAL_HTTP_TIMEOUT_S)"
  mistral_http_max_retries="$(env_value MISTRAL_HTTP_MAX_RETRIES)"
  mistral_retry_max_wait_s="$(env_value MISTRAL_RETRY_MAX_WAIT_S)"
  mistral_progress_log_every="$(env_value MISTRAL_PROGRESS_LOG_EVERY)"
  mistral_checkpoint_file="$(env_value MISTRAL_DISCOVERY_CHECKPOINT_FILE)"
  mistral_concurrency="$(env_value MISTRAL_CONCURRENCY)"
  mistral_submit_delay_s="$(env_value MISTRAL_SUBMIT_DELAY_S)"

  docker compose exec \
    -e ENABLE_MISTRAL_SEARCH_DISCOVERY=true \
    -e MISTRAL_API_KEY="$mistral_api_key" \
    -e MISTRAL_MODEL="$mistral_model" \
    -e MISTRAL_SEARCH_TOOL="$mistral_search_tool" \
    -e MISTRAL_DISCOVERY_MAX_RESULTS="$mistral_discovery_max_results" \
    -e MISTRAL_REQUEST_DELAY_S="$mistral_request_delay_s" \
    -e MISTRAL_HTTP_TIMEOUT_S="$mistral_http_timeout_s" \
    -e MISTRAL_HTTP_MAX_RETRIES="$mistral_http_max_retries" \
    -e MISTRAL_RETRY_MAX_WAIT_S="$mistral_retry_max_wait_s" \
    -e MISTRAL_PROGRESS_LOG_EVERY="$mistral_progress_log_every" \
    -e MISTRAL_DISCOVERY_CHECKPOINT_FILE="$mistral_checkpoint_file" \
    -e MISTRAL_CONCURRENCY="$mistral_concurrency" \
    -e MISTRAL_SUBMIT_DELAY_S="$mistral_submit_delay_s" \
    -e SOURCE_ENRICHMENT_BATCH_SIZE="${MISTRAL_BATCH_SIZE:-1}" \
    airflow-scheduler \
    python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); import mistral_search_discovery.main as m; m.run_mistral_search_discovery()"
}

echo "Starting resumable Mistral discovery loop"
echo "Batch size: ${MISTRAL_BATCH_SIZE:-1}"

# How many consecutive no-progress runs to tolerate before giving up.
# Each no-progress run sleeps MISTRAL_RATE_LIMIT_WAIT_S before retrying.
max_stall_retries="${MISTRAL_STALL_RETRIES:-5}"
stall_count=0

while true; do
  before="$(pending_count)"
  checkpoint_before="$(checkpoint_value)"
  echo "Pending companies before run: $before"

  if [[ "$before" == "0" ]]; then
    echo "No companies left to process."
    break
  fi

  if ! run_once; then
    echo "One discovery run failed. Stopping loop so state can be inspected."
    exit 1
  fi

  after="$(pending_count)"
  checkpoint_after="$(checkpoint_value)"
  echo "Pending companies after run: $after"

  if [[ "$after" == "$before" && "$checkpoint_after" == "$checkpoint_before" ]]; then
    stall_count=$((stall_count + 1))
    if [[ "$stall_count" -ge "$max_stall_retries" ]]; then
      echo "No progress after $stall_count retries. Stopping loop."
      break
    fi
    wait_s="${MISTRAL_RATE_LIMIT_WAIT_S:-60}"
    echo "No progress (rate limit?). Waiting ${wait_s}s before retry ($stall_count/$max_stall_retries)."
    sleep "$wait_s"
    continue
  fi

  stall_count=0
  sleep "${MISTRAL_LOOP_SLEEP_S:-5}"
done
