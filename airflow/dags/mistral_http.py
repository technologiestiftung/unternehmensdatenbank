from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import requests


MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-medium-latest")
MISTRAL_SEARCH_TOOL = os.getenv("MISTRAL_SEARCH_TOOL", "web_search")
MISTRAL_API_BASE_URL = os.getenv("MISTRAL_API_BASE_URL", "https://api.mistral.ai/v1")
MISTRAL_HTTP_TIMEOUT_S = float(os.getenv("MISTRAL_HTTP_TIMEOUT_S", "45"))
MISTRAL_HTTP_MAX_RETRIES = int(os.getenv("MISTRAL_HTTP_MAX_RETRIES", "3"))
MISTRAL_REQUEST_DELAY_S = float(os.getenv("MISTRAL_REQUEST_DELAY_S", "3.0"))
MISTRAL_RETRY_MAX_WAIT_S = float(os.getenv("MISTRAL_RETRY_MAX_WAIT_S", "60"))


class MistralRateLimitError(RuntimeError):
    pass


def require_mistral_api_key() -> None:
    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY is not configured.")


def _extract_text_from_response(response_json: dict[str, Any]) -> str:
    outputs = response_json.get("outputs") or []
    chunks: list[str] = []

    for output in outputs:
        content = output.get("content") if isinstance(output, dict) else None
        if isinstance(content, str):
            chunks.append(content)
            continue
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get("text"):
                    chunks.append(item["text"])

    return "\n".join(chunk.strip() for chunk in chunks if chunk and chunk.strip())


def extract_json_block(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("No JSON object found in Mistral response.")
    return json.loads(match.group(0))


def run_websearch_prompt(prompt: str) -> dict[str, Any]:
    require_mistral_api_key()

    payload = {
        "model": MISTRAL_MODEL,
        "inputs": [{"role": "user", "content": prompt}],
        "tools": [{"type": MISTRAL_SEARCH_TOOL}],
        "store": False,
    }
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }
    url = f"{MISTRAL_API_BASE_URL.rstrip('/')}/conversations"

    last_error: Exception | None = None
    for attempt in range(1, MISTRAL_HTTP_MAX_RETRIES + 1):
        try:
            if MISTRAL_REQUEST_DELAY_S > 0:
                time.sleep(MISTRAL_REQUEST_DELAY_S)
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=MISTRAL_HTTP_TIMEOUT_S,
            )
            if response.status_code == 429 and attempt < MISTRAL_HTTP_MAX_RETRIES:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_s = float(retry_after)
                    except ValueError:
                        wait_s = min(2**attempt, MISTRAL_RETRY_MAX_WAIT_S)
                else:
                    wait_s = min(2**attempt * 5, MISTRAL_RETRY_MAX_WAIT_S)
                time.sleep(wait_s)
                continue
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                detail = f"429 Too Many Requests for {url}"
                if retry_after:
                    detail += f" (Retry-After={retry_after}s)"
                raise MistralRateLimitError(detail)
            response.raise_for_status()
            body = response.json()
            text = _extract_text_from_response(body)
            if not text:
                raise ValueError("Mistral returned no text output.")
            return {"raw_response": body, "raw_text": text, "parsed": extract_json_block(text)}
        except MistralRateLimitError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < MISTRAL_HTTP_MAX_RETRIES:
                time.sleep(min(2**attempt, MISTRAL_RETRY_MAX_WAIT_S))
                continue
            break

    raise RuntimeError(f"Mistral websearch request failed: {last_error}") from last_error
