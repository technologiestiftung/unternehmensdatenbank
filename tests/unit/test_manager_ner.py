"""`manager_ner.ner`: votes become partially annotated training data, and training uses them.

A blank German spaCy pipeline keeps these offline and fast; no model download is needed.
"""
from __future__ import annotations

import json

import spacy
from spacy.training import Example

from manager_ner.ner import (
    Labels,
    Score,
    entity_tags,
    is_holdout,
    labels_from_votes,
    normalize,
    train,
    window_text,
)


def vote(value: str, verdict: str, correction: str | None = None) -> dict:
    return {"value": value, "verdict": verdict, "correction": correction}


def test_votes_majority_and_corrections() -> None:
    labels = labels_from_votes([
        vote("Erika Muster", "correct"), vote("Erika Muster", "correct"), vote("Erika Muster", "wrong"),
        vote("Hamburg Hafen", "wrong", correction="Max Beispiel"),
        vote("Tie Fall", "correct"), vote("Tie Fall", "wrong"),
    ])
    assert labels.positives == {"Erika Muster", "Max Beispiel"}
    assert labels.negatives == {"Hamburg Hafen"}


def test_a_corrected_value_is_never_also_a_negative() -> None:
    labels = labels_from_votes([vote("Max Beispiel", "wrong", correction="Max Beispiel")])
    assert labels.positives == {"Max Beispiel"} and labels.negatives == set()


def test_tags_mark_people_negatives_and_leave_the_rest_missing() -> None:
    doc = spacy.blank("de").make_doc("Erika Muster und Hamburg Hafen sowie Otto Normal")
    tags = entity_tags(doc, Labels(positives={"erika muster"}, negatives={"Hamburg Hafen"}))
    assert tags == ["B-PER", "L-PER", "-", "O", "O", "-", "-", "-"]


def test_no_tags_when_no_voted_value_is_on_the_page() -> None:
    doc = spacy.blank("de").make_doc("Impressum ohne Namen")
    assert entity_tags(doc, Labels(positives={"Erika Muster"})) is None


def test_window_text_keeps_only_the_lines_after_a_manager_label() -> None:
    page = "Impressum\nMusterweg 1\nGeschäftsführer:\nErika Muster\nUSt-ID DE123"
    assert window_text(page) == "Erika Muster\nUSt-ID DE123"


def test_normalize_drops_titles_case_and_spacing() -> None:
    assert normalize(" Dr. Prof.  Erika   Muster, ") == "erika muster"


def test_score_counts_only_voted_values() -> None:
    score = Score()
    score.add(["Erika Muster", "Hamburg Hafen", "Unbekannt Person"], Labels({"Erika Muster", "Max Beispiel"}, {"Hamburg Hafen"}))
    assert (score.tp, score.fp, score.fn) == (1, 1, 1)
    assert score.precision == 0.5 and score.recall == 0.5


def test_holdout_split_is_stable() -> None:
    company = "50e01909-0000-4000-8000-000000000000"
    assert is_holdout(company) == is_holdout(company)


def test_training_learns_from_partial_annotation(tmp_path) -> None:
    nlp = spacy.blank("de")
    nlp.add_pipe("ner")
    texts = [f"{first} {last}" for first, last in [("Erika", "Muster"), ("Max", "Beispiel"), ("Otto", "Normal")]]
    examples = []
    for text in texts * 4:
        doc = nlp.make_doc(f"{text} Hafen")
        examples.append(Example.from_dict(doc, {"entities": entity_tags(doc, Labels(positives={text}, negatives={"Hafen"}))}))

    losses = train(nlp, examples, epochs=15)

    assert losses[-1] < losses[0]
    json.dumps(losses)  # written to metrics.json; numpy floats would fail here
    nlp.to_disk(tmp_path / "model")
    assert "ner" in spacy.load(tmp_path / "model").pipe_names


def test_training_continues_a_pretrained_model_instead_of_resetting_it() -> None:
    """Regression: a wrong 'is it trained?' check once called initialize() on de_core_news_md."""
    import pytest

    pytest.importorskip("de_core_news_md")
    nlp = spacy.load("de_core_news_md")
    doc = nlp.make_doc("Erika Muster\nUSt-ID DE123")
    train(nlp, [Example.from_dict(doc, {"entities": entity_tags(doc, Labels(positives={"Erika Muster"}))})], epochs=1)
    assert {"PER", "LOC", "ORG", "MISC"} <= set(nlp.get_pipe("ner").labels)
