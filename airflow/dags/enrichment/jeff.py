"""jeff: TSB's self-hosted drop-in for TypeSafe's jev API, the one place it is called (#194).

[jeff](https://github.com/technologiestiftung/jeff) runs GLiFormer (`knowledgator/gliformer-large-v1`, 400M
parameters) behind jev's `POST /v1/systemone`. A `choice` question gets the text (`state`) and the labels
(`criteria`) and answers one label with a confidence, `(p_max - 1/n) / (1 - 1/n)`, and every label's
probability. No training needed, like `enrichment.decide`.

**Hosted by TSB** (`JEFF_BASE_URL`, today on the IONOS dev cluster, CPU and ONNX int8). The developer decided on
2026-11-11 that text goes to it unredacted, because we host it. That covers this self-hosted jeff only: jev
itself, or jeff on a third-party host, needs it-compliance first.

**At most `MAX_LABELS` labels per question** (the server's `JEFF_MAX_LABELS`, 64 by default; more is a 422).
Berlin's 77 Technologiefelder and the 87 WZ divisions do not fit, so `choose` asks them in parts and puts each
part's best `FINALISTS` into a final question, whose answer and confidence count.
"""
from __future__ import annotations

import os
import time
from typing import Any, Mapping, Sequence

import requests

BASE_URL = os.getenv("JEFF_BASE_URL", "https://jeff.ionos-dev.ds-apps.tsb-berlin.de").rstrip("/")
MAX_LABELS = int(os.getenv("JEFF_MAX_LABELS", "64"))
#: Labels from each part that go into the final question.
FINALISTS = 8
TIMEOUT = 120
#: 429 (rate limit) and 529 (queue full) are waited out; anything else fails the task.
RETRIES = 5


def session() -> requests.Session:
    key = os.getenv("JEFF_API_KEY", "")
    if not key:
        raise RuntimeError("JEFF_API_KEY is not set: add it as an Airflow variable (Admin → Variables).")
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    return s


def model(http: requests.Session) -> str:
    """The name of the model jeff serves now (`GET /v1/models`, first entry; the rest are aliases)."""
    response = http.get(f"{BASE_URL}/v1/models", timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()["models"][0]["name"]


def ask(http: requests.Session, state: str, instructions: str, labels: Sequence[str]) -> dict[str, Any]:
    """One `choice` question: {"choice", "confidence", "probabilities"}."""
    body = {"state": state, "model": "jev-latest", "questions": {"q": {
        "type": "choice", "instructions": instructions, "criteria": {label: None for label in labels}}}}
    for attempt in range(RETRIES):
        response = http.post(f"{BASE_URL}/v1/systemone", json=body, timeout=TIMEOUT)
        if response.status_code in (429, 529) and attempt < RETRIES - 1:
            time.sleep(float(response.headers.get("retry-after-ms") or 1000 * 2 ** attempt) / 1000)
            continue
        response.raise_for_status()
        return response.json()["answers"]["q"]
    raise AssertionError("unreachable")


def parts(labels: Sequence[str], size: int = MAX_LABELS) -> list[list[str]]:
    """`labels` in as few even parts as fit `size` each."""
    n = -(-len(labels) // size)
    step = -(-len(labels) // n)
    return [list(labels[i:i + step]) for i in range(0, len(labels), step)]


def finalists(answer: Mapping[str, Any], k: int = FINALISTS) -> list[str]:
    """A part's `k` most probable labels, best first."""
    probabilities = answer.get("probabilities") or {}
    return sorted(probabilities, key=lambda label: -probabilities[label])[:k]


def choose(http: requests.Session, state: str, instructions: str, labels: Sequence[str]) -> tuple[str | None, float]:
    """(label, jeff's confidence); in parts and a final question when there are more than `MAX_LABELS`."""
    if len(labels) > MAX_LABELS:
        labels = [label for part in parts(labels) for label in finalists(ask(http, state, instructions, part))]
    answer = ask(http, state, instructions, labels)
    return answer.get("choice"), float(answer.get("confidence") or 0.0)
