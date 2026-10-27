"""A text classifier for the Technologiefeld, seeded by the keywords and corrected by the app's votes.

WHY

The keywords give a field to 403 of the 1,113 companies with a fetched website (measured over the
Osnabrück example data). Most of the rest are `no_keyword`: we read their pages and no listed word
fired. A classifier reads the whole text and can recognise a field without the exact word, so it is
used for exactly those gaps, and nowhere else (see "WHERE IT DECIDES").

WHERE THE LABELS COME FROM

1. Seed: every company the keywords assigned (`outcome == "assigned"`, `decided_by == "keywords"`).
   The classifier starts by learning to imitate the keywords.
2. App votes on `technologiefeld` (`extraction_feedback`, V23/V24) override the seed per company:
   a correction naming one of the 77 fields replaces the label, a majority "correct" confirms it,
   and a majority "wrong" without a correction REMOVES the example, so a known mistake is never
   learned. Corrections that are not one of the 77 names are ignored.

HOW IT IS JUDGED

Only against people. A fixed fifth of the companies with a human label is held out, and the
classifier's accuracy there is compared with the keywords' accuracy on the same companies. Scoring
it against keyword labels would grade the student against its own teacher.

WHERE IT DECIDES

Only where the keywords found nothing (`no_keyword`), the company has a verified website, and the
classifier is at least `TEXTCAT_MIN_SCORE` sure. Such rows say `decided_by == "textcat"` and are
stored at their own confidence (`branch_observations._CONFIDENCE`). Where a keyword fired, the
keywords still decide: "keywords decide" stays the rule until the votes show otherwise.

No database, no network, no files here; `scripts/train_branch_textcat.py` and the `classify`
command do the I/O.
"""
from __future__ import annotations

import os
import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

#: Column 1 only. Deliberately not in `purpose_branch.ALL_DECIDED_BY`, which is column 3's vocabulary.
DECIDED_BY_TEXTCAT = "textcat"

#: The classifier fills a gap only above this probability for its top field.
TEXTCAT_MIN_SCORE = float(os.getenv("BRANCH_TEXTCAT_MIN_SCORE", "0.6"))

#: Characters of page text the classifier reads. Long pages are mostly navigation and footers.
MAX_CHARS = int(os.getenv("BRANCH_TEXTCAT_MAX_CHARS", "5000"))


def classifier_text(texts: Mapping[str, str]) -> str:
    """The text the classifier reads, and the text `classify` hands the tie-break model: the services
    pages, then the homepage. One definition, so training and prediction read the same thing."""
    return " ".join(texts.get(source, "") for source in ("services", "homepage")).strip()


@dataclass
class TrainingSet:
    #: company_id -> (text, label) for every company with a label.
    labelled: dict[str, tuple[str, str]] = field(default_factory=dict)
    #: company_ids whose label came from a person, not from the keywords.
    human: set[str] = field(default_factory=set)
    #: What happened to the seed, for the run's log and metrics.
    counts: Counter = field(default_factory=Counter)


def human_label(votes: Iterable[Mapping[str, Any]], fields: set[str]) -> tuple[str | None, set[str]]:
    """(label people gave this company or None, values most people called wrong)."""
    corrections = Counter(v["correction"].strip() for v in votes if v.get("correction") and v["correction"].strip() in fields)
    if corrections:
        return corrections.most_common(1)[0][0], set()
    tally: dict[str, Counter] = {}
    for v in votes:
        tally.setdefault(v["value"], Counter())[v["verdict"]] += 1
    confirmed = [value for value, t in tally.items() if t["correct"] > t["wrong"] and value in fields]
    rejected = {value for value, t in tally.items() if t["wrong"] > t["correct"]}
    return (confirmed[0] if len(confirmed) == 1 else None), rejected


def training_set(
    branch_rows: Iterable[Mapping[str, Any]],
    texts: Mapping[str, str],
    votes: Mapping[str, list[Mapping[str, Any]]],
    fields: set[str],
) -> TrainingSet:
    """Seed from the keywords, overridden by people. `texts` maps company_id -> classifier text."""
    result = TrainingSet()
    for row in branch_rows:
        company_id = row.get("company_id")
        text = texts.get(company_id or "", "")
        if not company_id or not text:
            continue
        seed = row.get("branch") if row.get("outcome") == "assigned" and row.get("decided_by") == "keywords" else None
        label, rejected = human_label(votes.get(company_id, []), fields)
        if label:
            result.labelled[company_id] = (text, label)
            result.human.add(company_id)
            result.counts["seed_confirmed" if label == seed else "human_label"] += 1
        elif seed and seed in rejected:
            result.counts["seed_removed_by_vote"] += 1
        elif seed in fields:
            result.labelled[company_id] = (text, seed)
            result.counts["seed"] += 1
    return result


def train(examples: list[tuple[str, str]], fields: list[str], *, epochs: int, seed: int = 0) -> tuple[Any, list[float]]:
    """A fresh German pipeline with one exclusive text classifier over `fields`."""
    import spacy
    from spacy.training import Example
    from spacy.util import minibatch

    nlp = spacy.blank("de")
    textcat = nlp.add_pipe("textcat")
    for name in fields:
        textcat.add_label(name)
    data = [
        Example.from_dict(nlp.make_doc(text[:MAX_CHARS]), {"cats": {name: float(name == label) for name in fields}})
        for text, label in examples
    ]
    optimizer = nlp.initialize(lambda: data)
    rng = random.Random(seed)
    losses_per_epoch = []
    for _ in range(epochs):
        rng.shuffle(data)
        losses: dict[str, float] = {}
        for batch in minibatch(data, size=16):
            nlp.update(batch, sgd=optimizer, drop=0.2, losses=losses)
        losses_per_epoch.append(round(float(losses.get("textcat", 0.0)), 4))
    return nlp, losses_per_epoch


def predict(nlp: Any, text: str) -> tuple[str | None, float]:
    """The most likely field and its probability; (None, 0.0) for an empty text."""
    if not text.strip():
        return None, 0.0
    cats = nlp(text[:MAX_CHARS]).cats
    label = max(cats, key=cats.get)
    return label, float(cats[label])


def accuracy(pairs: Iterable[tuple[str | None, str]]) -> tuple[float, int]:
    """(share of predictions equal to the human label, number compared)."""
    pairs = list(pairs)
    return (sum(1 for got, want in pairs if got == want) / len(pairs) if pairs else 0.0), len(pairs)
