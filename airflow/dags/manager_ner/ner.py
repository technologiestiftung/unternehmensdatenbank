"""Manager names from Impressum pages with spaCy, and the training data the app's votes make.

No database, no network, no Airflow here: `main.py` does the I/O. Everything below takes text and
votes and returns names, labels, examples or scores, so it is unit-tested with a blank spaCy model.

WHAT THE MODEL READS

Not the whole Impressum. On a whole page spaCy finds a median of 12 person names (web designers,
photographers, the editor), measured over 222 cached Impressum pages on 2026-10-27. It reads the
lines after a manager label ("Geschäftsführer", "Vertreten durch", ...), the same lines the regex
extractor reads (`url_discovery.imprint_extractor.ceo_label_windows`).

HOW VOTES BECOME TRAINING DATA

Per company, from `extraction_feedback` rows for `management`:
- a value most voters marked correct is a person (PER),
- every correction is a person,
- a value most voters marked wrong is NOT a person (O),
- everything nobody voted on is MISSING ("-"), not "not a person". Marking it O would teach the
  model that every unchecked name is wrong; spaCy skips missing tokens in the loss instead.
A tie between correct and wrong votes is left missing too.
"""
from __future__ import annotations

import random
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable

from url_discovery.imprint_extractor import ceo_label_windows

LABEL = "PER"

#: A name has at least two words. Of the 185 extra names spaCy found in the measurement, 137 were a
#: single word (a first name alone, a role, a town), so one-word spans are not reported.
MIN_NAME_TOKENS = 2

_TITLE = re.compile(r"^(dr\.?|prof\.?|dipl\.[-\w.]*)\s+", re.IGNORECASE)


def normalize(name: str) -> str:
    """The comparison key for a name: lower case, no academic title, single spaces."""
    name = re.sub(r"\s+", " ", name.strip(" ,.;:").lower())
    while _TITLE.match(name):
        name = _TITLE.sub("", name)
    return name


def window_text(page: str) -> str:
    """The text the model reads: every manager-label window of an Impressum, one per paragraph.

    `page` is the page as plain text (`imprint_extractor.page_text`). The label itself is not part
    of a window, so the regex extractor, which needs the label, is run on `page`, not on this.
    """
    return "\n\n".join("\n".join(lines) for lines in ceo_label_windows(page))


def extract_names(nlp: Any, text: str) -> list[str]:
    """Person names spaCy finds in `text`, at least two words long, first spelling kept."""
    if not text.strip():
        return []
    seen: dict[str, str] = {}
    for ent in nlp(text).ents:
        if ent.label_ == LABEL and len(ent) >= MIN_NAME_TOKENS:
            seen.setdefault(normalize(ent.text), ent.text.strip(" ,.;:"))
    return list(seen.values())


@dataclass
class Labels:
    positives: set[str] = field(default_factory=set)
    negatives: set[str] = field(default_factory=set)

    def __bool__(self) -> bool:
        return bool(self.positives or self.negatives)


def labels_from_votes(rows: Iterable[dict[str, Any]]) -> Labels:
    """Majority per value; a tie stays unlabelled. Every correction counts as a person."""
    tally: dict[str, Counter] = {}
    labels = Labels()
    for row in rows:
        tally.setdefault(row["value"], Counter())[row["verdict"]] += 1
        if row.get("correction"):
            labels.positives.add(row["correction"].strip())
    for value, votes in tally.items():
        if votes["correct"] > votes["wrong"]:
            labels.positives.add(value)
        elif votes["wrong"] > votes["correct"]:
            labels.negatives.add(value)
    labels.negatives -= labels.positives
    return labels


def find_spans(text: str, value: str) -> list[tuple[int, int]]:
    """Every case-insensitive occurrence of `value` in `text`, as character offsets."""
    if not value.strip():
        return []
    return [(m.start(), m.end()) for m in re.finditer(re.escape(value.strip()), text, re.IGNORECASE)]


def entity_tags(doc: Any, labels: Labels) -> list[str] | None:
    """BILUO tags with partial annotation, or None when no labelled value occurs in the text.

    Tokens of a positive span get B/I/L/U-PER, tokens of a negative span get O, every other token
    is "-" (missing). Spans that do not line up with token boundaries are skipped.
    """
    tags = ["-"] * len(doc)
    found = False
    for value, positive in [(v, True) for v in labels.positives] + [(v, False) for v in labels.negatives]:
        for start, end in find_spans(doc.text, value):
            span = doc.char_span(start, end, alignment_mode="strict")
            if span is None or any(tags[i] != "-" for i in range(span.start, span.end)):
                continue
            found = True
            if not positive:
                for i in range(span.start, span.end):
                    tags[i] = "O"
            elif len(span) == 1:
                tags[span.start] = f"U-{LABEL}"
            else:
                tags[span.start] = f"B-{LABEL}"
                for i in range(span.start + 1, span.end - 1):
                    tags[i] = f"I-{LABEL}"
                tags[span.end - 1] = f"L-{LABEL}"
    return tags if found else None


@dataclass
class Score:
    """Counted only against voted values: a predicted name nobody voted on is neither right nor wrong."""

    tp: int = 0
    fp: int = 0
    fn: int = 0

    def add(self, predicted: Iterable[str], labels: Labels) -> None:
        got = {normalize(n) for n in predicted}
        positives = {normalize(n) for n in labels.positives}
        negatives = {normalize(n) for n in labels.negatives}
        self.tp += len(got & positives)
        self.fp += len(got & negatives)
        self.fn += len(positives - got)

    @property
    def precision(self) -> float:
        return self.tp / (self.tp + self.fp) if self.tp + self.fp else 0.0

    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if self.tp + self.fn else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if p + r else 0.0

    def as_dict(self) -> dict[str, float | int]:
        return {"tp": self.tp, "fp": self.fp, "fn": self.fn, "precision": round(self.precision, 3),
                "recall": round(self.recall, 3), "f1": round(self.f1, 3)}


def is_holdout(company_id: str, share: float = 0.2) -> bool:
    """Stable split: a company is always on the same side, so a score is comparable between runs."""
    return (int(re.sub(r"[^0-9a-f]", "", company_id.lower())[:8] or "0", 16) % 100) < share * 100


def train(nlp: Any, examples: list[Any], *, epochs: int, seed: int = 0) -> list[float]:
    """Continue training the NER component on `examples`; the other components stay frozen.

    Returns the loss per epoch. Small data, so few epochs and dropout: the aim is to correct the
    model on Impressum layouts, not to replace what it learned about German names.
    """
    from spacy.util import minibatch

    rng = random.Random(seed)
    losses_per_epoch = []
    with nlp.select_pipes(enable=["ner"]):
        # A trained pipeline continues; a blank one (the unit tests) is initialised from the examples.
        # The test is "has labels", not a model dimension: de_core_news_md reports its output width as
        # unset, and initialize() on it would throw away everything it learned.
        trained = bool(nlp.get_pipe("ner").labels)
        optimizer = nlp.resume_training() if trained else nlp.initialize(lambda: examples)
        for _ in range(epochs):
            rng.shuffle(examples)
            losses: dict[str, float] = {}
            for batch in minibatch(examples, size=8):
                nlp.update(batch, sgd=optimizer, drop=0.2, losses=losses)
            # float(): spaCy returns numpy floats, which json.dumps (metrics.json) refuses.
            losses_per_epoch.append(round(float(losses.get("ner", 0.0)), 3))
    return losses_per_epoch
