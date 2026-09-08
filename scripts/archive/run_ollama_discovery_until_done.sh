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
        AND s.key = 'ollama_search_discovery'
        AND COALESCE(csr.source_payload ->> 'status', '') <> 'planned'
    );
  "
}

checkpoint_value() {
  if [[ -f airflow/data/ollama_search_discovery_checkpoint.json ]]; then
    python3 - <<'PY'
import json
from pathlib import Path
p = Path("airflow/data/ollama_search_discovery_checkpoint.json")
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
  local ollama_base_url ollama_model ollama_http_timeout_s ollama_http_max_retries
  local ollama_request_delay_s ollama_retry_wait_s ollama_progress_log_every
  local ollama_checkpoint_file searxng_base_url searxng_max_results
  local searxng_timeout_s searxng_http_max_retries

  ollama_base_url="$(env_value OLLAMA_BASE_URL)"
  ollama_model="$(env_value OLLAMA_MODEL)"
  ollama_http_timeout_s="$(env_value OLLAMA_HTTP_TIMEOUT_S)"
  ollama_http_max_retries="$(env_value OLLAMA_HTTP_MAX_RETRIES)"
  ollama_request_delay_s="$(env_value OLLAMA_REQUEST_DELAY_S)"
  ollama_retry_wait_s="$(env_value OLLAMA_RETRY_WAIT_S)"
  ollama_progress_log_every="$(env_value OLLAMA_PROGRESS_LOG_EVERY)"
  ollama_checkpoint_file="$(env_value OLLAMA_DISCOVERY_CHECKPOINT_FILE)"
  searxng_base_url="$(env_value SEARXNG_BASE_URL)"
  searxng_max_results="$(env_value SEARXNG_MAX_RESULTS)"
  searxng_timeout_s="$(env_value SEARXNG_TIMEOUT_S)"
  searxng_http_max_retries="$(env_value SEARXNG_HTTP_MAX_RETRIES)"

  docker compose exec \
    -e ENABLE_OLLAMA_SEARCH_DISCOVERY=true \
    -e OLLAMA_BASE_URL="$ollama_base_url" \
    -e OLLAMA_MODEL="$ollama_model" \
    -e OLLAMA_HTTP_TIMEOUT_S="$ollama_http_timeout_s" \
    -e OLLAMA_HTTP_MAX_RETRIES="$ollama_http_max_retries" \
    -e OLLAMA_REQUEST_DELAY_S="$ollama_request_delay_s" \
    -e OLLAMA_RETRY_WAIT_S="$ollama_retry_wait_s" \
    -e OLLAMA_PROGRESS_LOG_EVERY="$ollama_progress_log_every" \
    -e OLLAMA_DISCOVERY_CHECKPOINT_FILE="$ollama_checkpoint_file" \
    -e SEARXNG_BASE_URL="$searxng_base_url" \
    -e SEARXNG_MAX_RESULTS="$searxng_max_results" \
    -e SEARXNG_TIMEOUT_S="$searxng_timeout_s" \
    -e SEARXNG_HTTP_MAX_RETRIES="$searxng_http_max_retries" \
    -e SOURCE_ENRICHMENT_BATCH_SIZE="${OLLAMA_BATCH_SIZE:-10}" \
    airflow-scheduler \
    python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); import ollama_search_discovery.main as m; m.run_ollama_search_discovery()"
}

echo "Starting resumable Ollama discovery loop"
echo "Batch size: ${OLLAMA_BATCH_SIZE:-10}"

max_stall_retries="${OLLAMA_STALL_RETRIES:-5}"
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
    wait_s="${OLLAMA_STALL_WAIT_S:-30}"
    echo "No progress (service unavailable?). Waiting ${wait_s}s before retry ($stall_count/$max_stall_retries)."
    sleep "$wait_s"
    continue
  fi

  stall_count=0
  sleep "${OLLAMA_LOOP_SLEEP_S:-2}"
done
