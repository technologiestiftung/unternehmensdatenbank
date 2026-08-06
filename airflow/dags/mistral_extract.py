"""Mistral chat-completions extraction: text in, strict JSON out.

The other half of every search provider. Exa, Brave and Parallel return pages; something
still has to read them and produce fields. This is that step, deliberately separate so the
search provider can be swapped without touching extraction — which is what makes
"which provider is best" a fair comparison: same model, same prompt, same scoring.

Mistral because it is EU-hosted and the fetched Impressum text carries managing-director
names. The API is OpenAI-shaped, so `usage.prompt_tokens` / `usage.completion_tokens` come
back in the familiar place and token cost per business is directly measurable.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import requests


log = logging.getLogger(__name__)

EXTRACT_API_KEY = os.getenv("MISTRAL_API_KEY", "")
EXTRACT_URL = os.getenv("MISTRAL_API_BASE_URL", "https://api.mistral.ai/v1") + "/chat/completions"
# NOT MISTRAL_MODEL: the discovery DAGs set that to magistral-small-2509, a reasoning model
# that would burn output tokens deliberating over a mechanical extraction. Measured on a
# German Impressum sample, ministral-8b matched mistral-small field-for-field at identical
# token count, so take the cheaper one.
EXTRACT_MODEL = os.getenv("EXTRACT_MODEL", "ministral-8b-latest")
# 8000 was chosen when the input was a ~28k blob of mostly-irrelevant search results. Now
# that hits are filtered to this company first, what survives is dense and on-topic, so
# truncating it discards signal: measured on the same 5 companies, raising the cap recovered
# a description that 8000 chars had cut off. Costs input tokens — lower it if spend matters
# more than recall.
EXTRACT_MAX_CHARS = int(os.getenv("EXTRACT_MAX_CHARS", "20000"))
EXTRACT_HTTP_TIMEOUT_S = float(os.getenv("EXTRACT_HTTP_TIMEOUT_S", "90"))
EXTRACT_HTTP_MAX_RETRIES = int(os.getenv("EXTRACT_HTTP_MAX_RETRIES", "3"))
EXTRACT_RETRY_MAX_WAIT_S = float(os.getenv("EXTRACT_RETRY_MAX_WAIT_S", "30"))


class ExtractRateLimitError(RuntimeError):
    pass


def require_extract_api_key() -> None:
    if not EXTRACT_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY is not configured (needed for extraction).")


#: JSON mode returns the *string* "null" often enough that a plain truthiness test lets it
#: through. Measured: `website`, `cluster` and `ceo_name_or_contact` were all stored as the
#: literal text "null" at confidence 0.8 — above the resolver's 0.35 website threshold, so it
#: would have become the canonical website. ollama_search_discovery._str has the same gap.
NULLISH_VALUES = frozenset(
    {"", "null", "none", "nil", "n/a", "na", "-", "--", "unknown", "keine angabe", "unbekannt"}
)


def as_str(value: Any) -> str | None:
    """Coerce an LLM field to a clean string, or None.

    Two guards, both learned the hard way. Non-strings: models return dicts and lists where a
    string was asked for, and handing one to psycopg2 raises "can't adapt type 'dict'" deep
    inside the insert rather than at the boundary (this part is ollama's `_str`). Nullish text:
    see NULLISH_VALUES above.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    return None if text.lower() in NULLISH_VALUES else text


def as_confidence(value: Any) -> float | None:
    """Clamp a model's self-reported confidence into [0, 1], or None.

    `company_field_observations.confidence` is NUMERIC(5,4) with a
    `confidence IS NULL OR (confidence >= 0 AND confidence <= 1)` check, so an out-of-range
    number raises on insert. ollama_search_discovery passes the model's value straight to
    insert_text_observation with no validation — don't copy that part.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if number != number:  # NaN
        return None
    return max(0.0, min(1.0, number))


def extract_json(
    prompt: str,
    *,
    model: str = "",
    max_chars: int = 0,
) -> tuple[dict[str, Any], int, int]:
    """One extraction call. Returns (parsed_json, tokens_in, tokens_out).

    Raises rather than returning None on a hard failure: a silent empty result is
    indistinguishable from "the page said nothing", which is how a broken provider contract
    stays invisible. Callers that want to tolerate failures should catch explicitly.
    """
    require_extract_api_key()
    payload = {
        "model": model or EXTRACT_MODEL,
        "messages": [{"role": "user", "content": prompt[: max_chars or EXTRACT_MAX_CHARS]}],
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "stream": False,
    }
    headers = {
        "Authorization": f"Bearer {EXTRACT_API_KEY}",
        "Content-Type": "application/json",
    }

    last_error: Exception | None = None
    for attempt in range(1, EXTRACT_HTTP_MAX_RETRIES + 1):
        try:
            response = requests.post(
                EXTRACT_URL, headers=headers, json=payload, timeout=EXTRACT_HTTP_TIMEOUT_S
            )
            if response.status_code == 429:
                if attempt < EXTRACT_HTTP_MAX_RETRIES:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        wait_s = (
                            min(float(retry_after), EXTRACT_RETRY_MAX_WAIT_S)
                            if retry_after
                            else 2.0 * attempt
                        )
                    except ValueError:
                        wait_s = 2.0 * attempt
                    time.sleep(wait_s)
                    continue
                raise ExtractRateLimitError(f"429 Too Many Requests for {EXTRACT_URL}")
            # 401 bad key, 402 no balance — terminal, and both must be loud. A DeepSeek
            # account at $0.00 returned 402 on every call while the run still looked like
            # "extraction found nothing".
            if response.status_code in (401, 402, 403):
                raise RuntimeError(
                    f"extraction HTTP {response.status_code}: {response.text[:200]}"
                )
            response.raise_for_status()
            body = response.json()
            usage = body.get("usage") or {}
            content = body["choices"][0]["message"]["content"]
            return (
                json.loads(content),
                int(usage.get("prompt_tokens") or 0),
                int(usage.get("completion_tokens") or 0),
            )
        except (ExtractRateLimitError, RuntimeError):
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < EXTRACT_HTTP_MAX_RETRIES:
                time.sleep(min(2**attempt, EXTRACT_RETRY_MAX_WAIT_S))
                continue
            break

    raise RuntimeError(f"extraction request failed: {last_error}") from last_error
