# ollama_search_discovery

Self-hosted company discovery using SearXNG (meta-search) + Ollama (local LLM). No cloud API keys required. Unlimited queries.

## What it does

For each company without an `ollama_search_discovery` source record:

1. Runs 4 SearXNG queries (general, website, kununu, linkedin)
2. Deduplicates results and feeds snippets to the local Ollama model
3. Extracts: `website`, `ceo_name_or_contact`, `tech_field`, `cluster`, `kununu_url`, `linkedin_url`, `candidate_urls`
4. Writes observations and a source record to the DB

## Prerequisites

### 1. Start SearXNG and Ollama

```bash
docker compose --profile llm up -d searxng ollama
```

### 2. Pull the model

```bash
docker compose --profile llm exec ollama ollama pull qwen2.5:7b
```

For better quality at the cost of more VRAM:

```bash
docker compose --profile llm exec ollama ollama pull llama3.1:8b
# or
docker compose --profile llm exec ollama ollama pull qwen2.5:14b
```

Set `OLLAMA_MODEL` in `.env` to match whichever model you pulled.

### 3. Enable the DAG

```
ENABLE_OLLAMA_SEARCH_DISCOVERY=true
```

## Key env vars

| Variable | Default | Notes |
|---|---|---|
| `ENABLE_OLLAMA_SEARCH_DISCOVERY` | `false` | Must be `true` to run |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | Ollama service URL |
| `OLLAMA_MODEL` | `qwen2.5:7b` | Must be pulled first |
| `OLLAMA_HTTP_TIMEOUT_S` | `120` | Increase for large models |
| `SEARXNG_BASE_URL` | `http://searxng:8080` | SearXNG service URL |
| `SEARXNG_MAX_RESULTS` | `5` | Results per query |

## Running

```bash
OLLAMA_BATCH_SIZE=10 ./scripts/archive/run_ollama_discovery_until_done.sh
```

The script loops until all companies are processed, with stall-retry logic if Ollama or SearXNG becomes temporarily unavailable.

## GPU support

GPU passthrough for Ollama is commented out in `docker-compose.yml`. Uncomment the `deploy` block under the `ollama` service to enable NVIDIA GPU acceleration.
