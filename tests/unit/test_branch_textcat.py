"""`combined_discovery.branch_textcat`: keyword seeds, corrected by votes, train a text classifier."""
from __future__ import annotations

import json

import pytest

from enrichment.branch_classifier import load_taxonomy
from enrichment.branch_observations import confidence_for, planned_writes
from enrichment.branch_textcat import (
    DECIDED_BY_TEXTCAT,
    accuracy,
    classifier_text,
    gap_accuracy,
    gap_companies,
    human_label,
    predict,
    train,
    training_set,
)
from pipeline_coordinator.resolver import _FIELD_MIN_CONF
from enrichment.purpose_branch import ASSIGNED_KEYWORDS, DECIDED_BY_KEYWORDS

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


def test_gap_companies_are_the_rows_the_keywords_did_not_decide() -> None:
    rows = [
        row("kw", "Software"),
        row("tie", "Software", decided_by="model"),
        row("gap", None, outcome="no_keyword", decided_by=""),
        row("filled", "Software", decided_by=DECIDED_BY_TEXTCAT),
        row("site", None, outcome="no_website", decided_by=""),
        row("", None, outcome="no_keyword", decided_by=""),
    ]
    assert gap_companies(rows) == {"gap", "filled"}


def test_gap_accuracy_counts_labels_and_rejections_of_the_prediction() -> None:
    pairs = [
        ("Software", [vote("Software", "correct")]),                           # confirmed: hit
        ("Software", [vote("Software", "wrong", "Energietechnik")]),           # corrected: miss
        ("Energietechnik", [vote("Software", "wrong", "Energietechnik")]),     # corrected to it: hit
        ("Software", [vote("Software", "wrong")]),                             # rejected prediction: miss
        ("Software", [vote("Energietechnik", "wrong")]),                       # other value rejected: ignored
        (None, [vote("Software", "wrong")]),                                   # no prediction: ignored
    ]
    assert gap_accuracy(pairs, FIELDS) == (0.5, 4)
    assert gap_accuracy([], FIELDS) == (0.0, 0)


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


def test_planned_writes_store_the_english_names() -> None:
    branch_row = {"outcome": "assigned", "technology_field": "Architekturmarkt",
                  "withheld_branch": "Additive Fertigung", "decided_by": DECIDED_BY_KEYWORDS}
    purpose_row = {"outcome": ASSIGNED_KEYWORDS, "purpose_field": "5G- bzw. 6G Anwendungen",
                   "withheld_by_floor": "Architekturmarkt", "decided_by": DECIDED_BY_KEYWORDS}

    written = {field: value for field, value, _, _ in planned_writes(branch_row, purpose_row)}

    assert written == {
        "technology_field": "Architecture Market",
        "technology_field_withheld": "Additive Manufacturing",
        "purpose_field": "5G/6G Applications",
        "purpose_field_withheld": "Architecture Market",
    }


def test_load_taxonomy_refuses_a_branch_without_an_english_name(tmp_path) -> None:
    path = tmp_path / "taxonomy.yaml"
    path.write_text('branches:\n  "Software":\n    keywords: ["Software"]\n', encoding="utf-8")
    with pytest.raises(ValueError, match="name_en"):
        load_taxonomy(path)
