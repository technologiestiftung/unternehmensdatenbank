"""jeff: TSB's self-hosted drop-in for TypeSafe's jev API, the one place it is called (#194).

[jeff](https://github.com/technologiestiftung/jeff) runs GLiFormer (`knowledgator/gliformer-large-v1`, 400M
parameters) behind jev's `POST /v1/systemone`. A `choice` question gets the text (`state`) and the labels
(`criteria`) and answers one label with a confidence, `(p_max - 1/n) / (1 - 1/n)`, and every label's
probability. No training needed, like `enrichment.decide`.

**Hosted by TSB** (`JEFF_BASE_URL`, today on the IONOS dev cluster, CPU and ONNX int8). The developer decided on
2026-11-11 that text goes to it unredacted, because we host it. That covers this self-hosted jeff only: jev
itself, or jeff on a third-party host, needs it-compliance first.

**At most `MAX_LABELS` labels per question** (the server's `JEFF_MAX_LABELS`; more is a 422). jeff's default is
64; TSB's server was raised to 128 on 2026-11-19, so Berlin's 77 Technologiefelder fit one question. A list
longer than the limit is asked in parts, each part's best `FINALISTS` going into a final question, whose answer
and confidence count. Set `JEFF_MAX_LABELS` to the server's value when it changes.
"""
from __future__ import annotations

import os
import time
from typing import Any, Mapping, Sequence

import requests

BASE_URL = os.getenv("JEFF_BASE_URL", "https://jeff.ionos-dev.ds-apps.tsb-berlin.de").rstrip("/")
MAX_LABELS = int(os.getenv("JEFF_MAX_LABELS", "128"))
#: Labels from each part that go into the final question.
FINALISTS = 8
TIMEOUT = 120
#: 429 (rate limit), 529 (queue full) and a gateway without jeff behind it (502-504: a restart or a deploy,
#: one ended a run on play on 2026-11-19) are waited out; anything else fails the task.
WAIT_STATUSES = (429, 502, 503, 504, 529)
RETRIES = 5


def session() -> requests.Session:
    key = os.getenv("JEFF_API_KEY", "")
    if not key:
        raise RuntimeError("JEFF_API_KEY is not set: add it as an Airflow variable (Admin → Variables).")
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    return s


def model(http: requests.Session) -> str:
    """The model jeff serves now (`GET /v1/models`, first entry; the rest are aliases): its name and its
    description, which names the weights' path, because jeff sets the name apart from the weights it loads."""
    response = http.get(f"{BASE_URL}/v1/models", timeout=TIMEOUT)
    response.raise_for_status()
    served = response.json()["models"][0]
    return f"{served['name']} ({served['description']})"


def ask(http: requests.Session, state: str, instructions: str, labels: Sequence[str]) -> dict[str, Any]:
    """One `choice` question: {"choice", "confidence", "probabilities"}."""
    body = {"state": state, "model": "jev-latest", "questions": {"q": {
        "type": "choice", "instructions": instructions, "criteria": {label: None for label in labels}}}}
    for attempt in range(RETRIES):
        response = http.post(f"{BASE_URL}/v1/systemone", json=body, timeout=TIMEOUT)
        if response.status_code in WAIT_STATUSES and attempt < RETRIES - 1:
            time.sleep(float(response.headers.get("retry-after-ms") or 1000 * 2 ** attempt) / 1000)
            continue
        response.raise_for_status()
        return response.json()["answers"]["q"]
    raise AssertionError("unreachable")


def parts(labels: Sequence[str], size: int | None = None) -> list[list[str]]:
    """`labels` in as few even parts as fit `size` each (`MAX_LABELS`, read when called)."""
    size = size or MAX_LABELS
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
