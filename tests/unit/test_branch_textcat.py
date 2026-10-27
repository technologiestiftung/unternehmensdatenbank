"""`combined_discovery.branch_textcat`: keyword seeds, corrected by votes, train a text classifier."""
from __future__ import annotations

import json

from combined_discovery.branch_observations import confidence_for
from combined_discovery.branch_textcat import (
    DECIDED_BY_TEXTCAT,
    accuracy,
    classifier_text,
    human_label,
    predict,
    train,
    training_set,
)
from pipeline_coordinator.resolver import _FIELD_MIN_CONF

FIELDS = {"Energietechnik", "Gesundheitswirtschaft", "Software"}


def row(cid: str, branch: str | None, outcome: str = "assigned", decided_by: str = "keywords") -> dict:
    return {"company_id": cid, "branch": branch, "outcome": outcome, "decided_by": decided_by}


def vote(value: str, verdict: str, correction: str | None = None) -> dict:
    return {"value": value, "verdict": verdict, "correction": correction}


def test_classifier_text_reads_services_then_homepage() -> None:
    assert classifier_text({"homepage": "Start", "services": "Leistungen", "imprint": "x"}) == "Leistungen Start"


def test_human_label_prefers_a_valid_correction_and_ignores_free_text() -> None:
    assert human_label([vote("Software", "wrong", "Energietechnik")], FIELDS) == ("Energietechnik", set())
    assert human_label([vote("Software", "wrong", "irgendwas")], FIELDS) == (None, {"Software"})
    assert human_label([vote("Software", "correct"), vote("Software", "correct"), vote("Software", "wrong")], FIELDS) == ("Software", set())


def test_seed_is_overridden_removed_or_kept() -> None:
    rows = [
        row("a", "Software"),                       # kept as seed
        row("b", "Software"),                       # corrected by a person
        row("c", "Software"),                       # voted wrong, no correction: removed
        row("d", "Software"),                       # confirmed by a person
        row("e", None, outcome="no_keyword", decided_by=""),  # no seed, no vote: nothing
        row("f", "Software", decided_by="model"),   # not a keyword decision: not a seed
    ]
    texts = {cid: f"Text {cid}" for cid in "abcdef"}
    votes = {
        "b": [vote("Software", "wrong", "Gesundheitswirtschaft")],
        "c": [vote("Software", "wrong")],
        "d": [vote("Software", "correct")],
    }
    data = training_set(rows, texts, votes, FIELDS)
    assert data.labelled == {
        "a": ("Text a", "Software"),
        "b": ("Text b", "Gesundheitswirtschaft"),
        "d": ("Text d", "Software"),
    }
    assert data.human == {"b", "d"}
    assert data.counts == {"seed": 1, "human_label": 1, "seed_removed_by_vote": 1, "seed_confirmed": 1}


def test_companies_without_text_are_skipped() -> None:
    assert training_set([row("a", "Software")], {}, {}, FIELDS).labelled == {}


def test_textcat_confidence_is_publishable_and_below_the_keywords() -> None:
    assert _FIELD_MIN_CONF < confidence_for(DECIDED_BY_TEXTCAT) < confidence_for("keywords")


def test_training_learns_to_separate_fields() -> None:
    examples = [
        ("Wir planen Photovoltaik und Windkraft Anlagen", "Energietechnik"),
        ("Solarstrom Speicher und Netze für Energie", "Energietechnik"),
        ("Praxis für Physiotherapie und Pflege", "Gesundheitswirtschaft"),
        ("Klinik Medizin Patienten Therapie", "Gesundheitswirtschaft"),
        ("Softwareentwicklung Apps Cloud Programmierung", "Software"),
        ("Programmierung von Web Anwendungen und Software", "Software"),
    ] * 3
    nlp, losses = train(examples, sorted(FIELDS), epochs=15)
    json.dumps(losses)  # goes into metrics.json
    assert losses[-1] < losses[0]
    assert predict(nlp, "Windkraft und Photovoltaik")[0] == "Energietechnik"
    assert predict(nlp, "") == (None, 0.0)
    assert accuracy([(predict(nlp, t)[0], label) for t, label in examples])[0] > 0.8
