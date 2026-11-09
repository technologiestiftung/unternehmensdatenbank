"""The local classifier: GLiNER2.5-multi-Decide, the one place it is loaded (#183).

A 287M-parameter multilingual classifier (Fastino, Apache-2.0). It is given the labels at call time and
picks one with a confidence, so it needs no training to answer; fine-tuning on Claude's labels and people's
verdicts is the next step (#182).

**Local only.** The package `gliner2` without `[local]` is a client for Fastino's hosted API, which would send
the text away; the requirements install `gliner2[local]`, the images bake the weights in (`MODEL_ID` at
`MODEL_REVISION`), and `load` sets `HF_HUB_OFFLINE`, so nothing is downloaded or sent when a task runs.

**Measured** (#182, zero-shot, 183 companies the keywords left open, against Claude's answer, not hand-checked):
the same Technologiefeld 41 %, the same WZ division 34 %; its confidence carries signal (at 0.7 and above
41 % and 57 %, below 0.5 11 % and 25 %). Hence `CONFIDENCE_SCALE`: every answer it gives sits below the
keywords, the layers and Claude.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Iterable, Sequence

MODEL_ID = "fastino/GLiNER2.5-multi-Decide"
#: Pinned: a new revision is a new model, and the cache in each company's record knows it by this name.
MODEL_REVISION = "36e46e4c94b7307e8bd59b23b6dfa36519a23269"
MODEL_VERSION = f"{MODEL_ID}@{MODEL_REVISION[:12]}"
#: The checkpoint names this encoder; its configuration and tokenizer files are baked in next to it.
ENCODER_ID = "microsoft/mdeberta-v3-base"
#: The model's own confidence times this is the stored confidence: at most 0.50, below Claude (0.55),
#: the Wirtschaftsatlas layers (0.56) and the keywords (0.60). Unmeasured as a probability (#182).
CONFIDENCE_SCALE = 0.5
#: One text at a time: memory grows steeply with batch and length. Measured on a laptop (87 WZ labels): 2,500
#: characters 2.3 s and 2.1 GB (1.2 GB of it the model), 6,000 characters 8.7 s; four of those at once swapped
#: to 9 GB and took 307 s.
BATCH_SIZE = int(os.getenv("DECIDE_BATCH_SIZE", "1"))


@lru_cache(maxsize=1)
def load() -> Any:
    """The model, loaded once per process from the baked-in weights. Never downloads."""
    os.environ["HF_HUB_OFFLINE"] = "1"
    from gliner2 import AutoExtractor
    from huggingface_hub import snapshot_download

    # The pinned snapshot's own folder: gliner2 asks for its `encoder_config` at "main", not at the
    # revision, which an offline cache holding only the pinned snapshot cannot answer.
    return AutoExtractor.from_pretrained(snapshot_download(MODEL_ID, revision=MODEL_REVISION, local_files_only=True))


def best(result: Any, question: str) -> tuple[str | None, float]:
    """(label, confidence) out of one `classify_text` result; (None, 0) when the model gave none."""
    value = result.get(question) if isinstance(result, dict) else None
    items = value if isinstance(value, list) else [value] if value else []
    scored = [(item.get("label"), float(item.get("confidence") or 0)) if isinstance(item, dict) else (item, 0.0)
              for item in items]
    return max(scored, key=lambda pair: pair[1], default=(None, 0.0))


def classify(texts: Sequence[str], labels: Iterable[str], question: str = "label") -> list[tuple[str | None, float]]:
    """The best label and the model's own confidence for each text, in batches of `BATCH_SIZE`."""
    if not texts:
        return []
    results = load().batch_classify_text(list(texts), {question: list(labels)}, batch_size=BATCH_SIZE,
                                         include_confidence=True)
    return [best(result, question) for result in results]


def stored_confidence(model_confidence: float) -> float:
    """What the observation stores: scaled below every other source, never 0 (0 means superseded)."""
    return max(0.01, round(model_confidence * CONFIDENCE_SCALE, 4))


def bake() -> None:
    """Download the weights into `HF_HOME` and load them once offline: run while an image is built, so a
    missing file fails the build, never a task. The encoder contributes only its configuration and tokenizer
    files (the checkpoint holds all weights)."""
    import subprocess
    import sys

    from huggingface_hub import snapshot_download

    snapshot_download(MODEL_ID, revision=MODEL_REVISION)
    snapshot_download(ENCODER_ID, allow_patterns=["*.json", "*.model", "*.txt"])
    # A fresh process: huggingface_hub reads HF_HUB_OFFLINE when imported, so this one cannot test it.
    subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import decide; decide.load()",
                    os.path.dirname(os.path.abspath(__file__))], env={**os.environ, "HF_HUB_OFFLINE": "1"}, check=True)


if __name__ == "__main__":
    bake()
