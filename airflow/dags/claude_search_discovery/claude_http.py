from __future__ import annotations

import json
import os
import time
from typing import Any

import requests


ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")
CLAUDE_WEB_SEARCH_TOOL_TYPE = os.getenv("CLAUDE_WEB_SEARCH_TOOL_TYPE", "web_search_20250305")
CLAUDE_API_BASE_URL = os.getenv("CLAUDE_API_BASE_URL", "https://api.anthropic.com/v1")
CLAUDE_HTTP_TIMEOUT_S = float(os.getenv("CLAUDE_HTTP_TIMEOUT_S", "45"))
CLAUDE_HTTP_MAX_RETRIES = int(os.getenv("CLAUDE_HTTP_MAX_RETRIES", "5"))
CLAUDE_REQUEST_DELAY_S = float(os.getenv("CLAUDE_REQUEST_DELAY_S", "3.0"))
CLAUDE_RETRY_MAX_WAIT_S = float(os.getenv("CLAUDE_RETRY_MAX_WAIT_S", "60"))
CLAUDE_MAX_TOKENS = int(os.getenv("CLAUDE_MAX_TOKENS", "1200"))
CLAUDE_WEB_SEARCH_MAX_USES = int(os.getenv("CLAUDE_WEB_SEARCH_MAX_USES", "5"))
CLAUDE_MAX_PAUSE_TURNS = int(os.getenv("CLAUDE_MAX_PAUSE_TURNS", "3"))


class ClaudeRateLimitError(RuntimeError):
    pass


def require_anthropic_api_key() -> None:
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured.")


def _extract_text_from_content(content: list[dict[str, Any]]) -> str:
    chunks: list[str] = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text" and block.get("text"):
            chunks.append(block["text"])
    return "\n".join(chunk.strip() for chunk in chunks if chunk and chunk.strip())


def extract_json_block(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in Claude response.")
    return json.loads(text[start : end + 1])


def _post_messages(payload: dict[str, Any]) -> dict[str, Any]:
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    url = f"{CLAUDE_API_BASE_URL.rstrip('/')}/messages"

    last_error: Exception | None = None
    for attempt in range(1, CLAUDE_HTTP_MAX_RETRIES + 1):
        try:
            if CLAUDE_REQUEST_DELAY_S > 0:
                time.sleep(CLAUDE_REQUEST_DELAY_S)
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=CLAUDE_HTTP_TIMEOUT_S,
            )
            if response.status_code == 429 and attempt < CLAUDE_HTTP_MAX_RETRIES:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_s = float(retry_after)
                    except ValueError:
                        wait_s = min(2**attempt * 5, CLAUDE_RETRY_MAX_WAIT_S)
                else:
                    wait_s = min(2**attempt * 5, CLAUDE_RETRY_MAX_WAIT_S)
                time.sleep(wait_s)
                continue
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                detail = f"429 Too Many Requests for {url}"
                if retry_after:
                    detail += f" (Retry-After={retry_after}s)"
                raise ClaudeRateLimitError(detail)
            response.raise_for_status()
            return response.json()
        except ClaudeRateLimitError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < CLAUDE_HTTP_MAX_RETRIES:
                time.sleep(min(2**attempt, CLAUDE_RETRY_MAX_WAIT_S))
                continue
            break

    raise RuntimeError(f"Claude web search request failed: {last_error}") from last_error


def run_websearch_prompt(prompt: str) -> dict[str, Any]:
    require_anthropic_api_key()

    tools = [
        {
            "type": CLAUDE_WEB_SEARCH_TOOL_TYPE,
            "name": "web_search",
            "max_uses": CLAUDE_WEB_SEARCH_MAX_USES,
        }
    ]
    messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
    raw_responses: list[dict[str, Any]] = []

    for _ in range(CLAUDE_MAX_PAUSE_TURNS + 1):
        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": CLAUDE_MAX_TOKENS,
            "messages": messages,
            "tools": tools,
        }
        body = _post_messages(payload)
        raw_responses.append(body)

        stop_reason = body.get("stop_reason")
        content = body.get("content") or []
        if stop_reason == "pause_turn":
            messages.append({"role": "assistant", "content": content})
            continue

        text = _extract_text_from_content(content)
        if not text:
            raise ValueError("Claude returned no text output.")
        return {
            "raw_responses": raw_responses,
            "raw_text": text,
            "parsed": extract_json_block(text),
        }

    raise RuntimeError("Claude web search exceeded maximum pause_turn continuations.")
