from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

import requests


log = logging.getLogger(__name__)

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
OLLAMA_HTTP_TIMEOUT_S = float(os.getenv("OLLAMA_HTTP_TIMEOUT_S", "120"))
OLLAMA_HTTP_MAX_RETRIES = int(os.getenv("OLLAMA_HTTP_MAX_RETRIES", "3"))
OLLAMA_REQUEST_DELAY_S = float(os.getenv("OLLAMA_REQUEST_DELAY_S", "0.0"))
OLLAMA_RETRY_WAIT_S = float(os.getenv("OLLAMA_RETRY_WAIT_S", "5.0"))


class OllamaUnavailableError(RuntimeError):
    pass


def require_ollama() -> None:
    url = f"{OLLAMA_BASE_URL.rstrip('/')}/api/tags"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.ConnectionError as exc:
        raise OllamaUnavailableError(
            f"Ollama not reachable at {OLLAMA_BASE_URL}: {exc}"
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise OllamaUnavailableError(
            f"Ollama availability check failed: {exc}"
        ) from exc

    model_base = OLLAMA_MODEL.split(":")[0]
    available = [
        m.get("name", "").split(":")[0]
        for m in (data.get("models") or [])
    ]
    if model_base not in available:
        raise OllamaUnavailableError(
            f"Model '{OLLAMA_MODEL}' not found in Ollama. "
            f"Available: {available or ['(none)']}. "
            f"Pull it with: docker compose --profile llm exec ollama ollama pull {OLLAMA_MODEL}"
        )


def extract_json_block(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("No JSON object found in Ollama response.")
    return json.loads(match.group(0))


def run_chat_prompt(prompt: str) -> dict[str, Any]:
    url = f"{OLLAMA_BASE_URL.rstrip('/')}/api/chat"
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.1,
            "num_predict": 1024,
        },
    }

    last_error: Exception | None = None
    for attempt in range(1, OLLAMA_HTTP_MAX_RETRIES + 1):
        try:
            if OLLAMA_REQUEST_DELAY_S > 0:
                time.sleep(OLLAMA_REQUEST_DELAY_S)
            resp = requests.post(url, json=payload, timeout=OLLAMA_HTTP_TIMEOUT_S)
            if resp.status_code in (500, 503):
                raise OllamaUnavailableError(
                    f"Ollama returned {resp.status_code}: {resp.text[:200]}"
                )
            resp.raise_for_status()
            body = resp.json()
            raw_text = (body.get("message") or {}).get("content") or ""
            if not raw_text:
                raise ValueError("Ollama returned empty content.")
            return {"raw_text": raw_text, "parsed": extract_json_block(raw_text)}
        except OllamaUnavailableError:
            raise
        except requests.exceptions.ConnectionError as exc:
            raise OllamaUnavailableError(
                f"Ollama not reachable at {OLLAMA_BASE_URL}: {exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < OLLAMA_HTTP_MAX_RETRIES:
                time.sleep(OLLAMA_RETRY_WAIT_S * attempt)
                continue
            break

    raise RuntimeError(
        f"Ollama chat request failed after {OLLAMA_HTTP_MAX_RETRIES} attempts: {last_error}"
    ) from last_error
