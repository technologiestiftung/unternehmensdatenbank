"""`local_classifier`, `enrichment.decide` and Claude's guesses (#183): what is asked, what is stored, how sure."""
from __future__ import annotations

import pytest

import local_classifier.main as lc
from enrichment import decide
from enrichment.branch_textcat import DECIDED_BY_TEXTCAT
from enrichment.branch_observations import confidence_for
from llm_labels.main import CONFIDENCE as CLAUDE_CONFIDENCE, observations_for
from pipeline_coordinator.resolver import _FIELD_MIN_CONF, _GUESS_MIN_CONF, GUESS_FIELDS


def test_only_missing_or_stale_answers_are_asked_never_asked_first() -> None:
    websites = {"a": "https://a.de", "b": "https://b.de", "c": "https://c.de", "d": "https://d.de"}
    cached = {
        "a": {"model": lc.ASKED_WITH, "website": "https://a.de"},  # current: not asked
        "b": {"model": "old-model", "website": "https://b.de"},           # another model
        "c": {"model": lc.ASKED_WITH, "website": "https://old.de"},  # another website
    }
    assert lc.to_ask(websites, cached, 10) == ["d", "b", "c"]
    assert lc.to_ask(websites, cached, 1) == ["d"]


def test_each_answer_is_stored_as_its_value_with_the_models_confidence(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_classify(texts, labels, question):  # noqa: ANN001, ANN202
        labels = list(labels)
        return [(labels[0], 0.8), ("a label nobody offered", 0.9)][: len(texts)]

    monkeypatch.setattr(decide, "classify", fake_classify)
    asked = {"deeptech_class": lc.DEEPTECH, "is_ai": lc.IS_AI}
    answers = lc.answer({"a": "Wir entwickeln Software.", "b": "Bäckerei"}, asked)
    assert answers["a"] == {"deeptech_class": ("non-tech", 0.8), "is_ai": ("true", 0.8)}
    assert answers["b"] == {}  # an answer outside the labels is dropped, never stored


def test_deeptech_has_exactly_the_four_classes() -> None:
    assert sorted(lc.DEEPTECH.values()) == ["deep tech", "hybrid", "non-tech", "tech"]
    assert sorted(lc.IS_AI.values()) == ["false", "true"]


def test_the_model_answer_is_read_in_every_shape_it_comes_in() -> None:
    assert decide.best({"q": {"label": "x", "confidence": 0.7}}, "q") == ("x", 0.7)
    assert decide.best({"q": [{"label": "x", "confidence": 0.2}, {"label": "y", "confidence": 0.6}]}, "q") == ("y", 0.6)
    assert decide.best({"q": "x"}, "q") == ("x", 0.0)
    assert decide.best({}, "q") == (None, 0.0)


def test_a_local_guess_ranks_below_every_other_source_and_above_zero() -> None:
    assert decide.stored_confidence(1.0) == 0.5
    assert decide.stored_confidence(0.0) == 0.01  # 0 means superseded: never written for a live guess
    # keywords > Claude > our spaCy model > the local classifier
    assert decide.stored_confidence(1.0) < confidence_for(DECIDED_BY_TEXTCAT) < CLAUDE_CONFIDENCE < confidence_for("keywords")
    assert 0 < _GUESS_MIN_CONF < decide.stored_confidence(0.0) + 0.001 <= _FIELD_MIN_CONF


def test_claude_none_writes_nothing_and_never_the_official_wz_code() -> None:
    assert observations_for("Artificial Intelligence", "62") == [("technology_field", "Artificial Intelligence"),
                                                                 ("wz_division_estimated", "62")]
    assert observations_for("NONE", "NONE") == []
    assert observations_for(None, "62") == [("wz_division_estimated", "62")]
    assert all(field != "wz_code" for field, _ in observations_for("X", "62"))


def test_the_guess_fields_are_the_four_the_app_colours() -> None:
    assert set(GUESS_FIELDS) == {"technology_field", "wz_division_estimated", "deeptech_class", "is_ai"}
