"""`enrichment.jeff` and the `jeff` DAG: more labels than one question takes, retries, what is stored (#194)."""
from __future__ import annotations

from typing import Any

import pytest

from enrichment import jeff
from jeff import main


class _Response:
    def __init__(self, status: int, body: dict[str, Any] | None = None) -> None:
        self.status_code, self.body, self.headers = status, body or {}, {"retry-after-ms": "0"}

    def json(self) -> dict[str, Any]:
        return self.body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class _Jeff:
    """Answers a question that fits one part with its first label at 0.7, every label's probability falling
    with its position; a final round (16 labels) with its last label at 0.9."""

    def __init__(self, busy: int = 0, weights: str = "models/gliformer-large-v1") -> None:
        self.questions: list[list[str]] = []
        self.busy, self.weights = busy, weights

    def post(self, url: str, *, json: dict[str, Any], timeout: int) -> _Response:
        if self.busy:
            self.busy -= 1
            return _Response(529)
        labels = list(json["questions"]["q"]["criteria"])
        assert len(labels) <= jeff.MAX_LABELS, "jeff answers 422 above its label limit"
        self.questions.append(labels)
        probabilities = {label: 1 / (i + 2) for i, label in enumerate(labels)}
        choice, confidence = (labels[-1], 0.9) if len(labels) == 2 * jeff.FINALISTS else (labels[0], 0.7)
        return _Response(200, {"answers": {"q": {"choice": choice, "confidence": confidence,
                                                 "probabilities": probabilities}}})

    def get(self, url: str, *, timeout: int) -> _Response:
        assert url.endswith("/v1/models")
        return _Response(200, {"models": [
            {"name": "gliformer-large-v1", "description": f"GLiFormer served by jeff ({self.weights})",
             "release_date": "2026-09-16"},
            {"name": "jev-latest", "description": "Alias of gliformer-large-v1", "release_date": "2026-09-16"}]})


def test_77_fields_go_in_two_parts_and_a_final_round_of_their_best() -> None:
    fields = [f"Feld {i}" for i in range(77)]
    assert [len(p) for p in jeff.parts(fields)] == [39, 38]
    assert [len(p) for p in jeff.parts([str(i) for i in range(87)])] == [44, 43]
    http = _Jeff()
    first, second = jeff.parts(fields)
    assert jeff.choose(http, "Text", "Welches Feld?", fields) == (second[jeff.FINALISTS - 1], 0.9)
    assert http.questions == [first, second, first[:jeff.FINALISTS] + second[:jeff.FINALISTS]]


def test_the_model_is_the_one_jeff_serves_not_an_alias_and_new_weights_change_it() -> None:
    assert jeff.model(_Jeff()) == "gliformer-large-v1 (GLiFormer served by jeff (models/gliformer-large-v1))"
    assert jeff.model(_Jeff(weights="models/finetuned")) != jeff.model(_Jeff())


def test_labels_that_fit_are_asked_once() -> None:
    http = _Jeff()
    assert jeff.choose(http, "Text", "Welche?", ["a", "b"]) == ("a", 0.7)
    assert len(http.questions) == 1


def test_a_full_queue_is_waited_out_and_a_missing_key_fails_loudly(monkeypatch) -> None:
    assert jeff.choose(_Jeff(busy=2), "Text", "Welche?", ["a"]) == ("a", 0.7)
    with pytest.raises(RuntimeError):
        jeff.choose(_Jeff(busy=jeff.RETRIES), "Text", "Welche?", ["a"])
    monkeypatch.delenv("JEFF_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="JEFF_API_KEY"):
        jeff.session()


def test_answers_are_stored_values_and_an_unknown_label_is_left_out() -> None:
    asked = {"technology_field": {"Künstliche Intelligenz": "Artificial Intelligence"},
             "wz_division_estimated": {"62 Erbringung von Dienstleistungen der Informationstechnologie": "62"}}
    assert main.answer_one(_Jeff(), "Text", asked) == {"technology_field": ("Artificial Intelligence", 0.7),
                                                      "wz_division_estimated": ("62", 0.7)}

    class _Lost(_Jeff):
        def post(self, url: str, *, json: dict[str, Any], timeout: int) -> _Response:
            return _Response(200, {"answers": {"q": {"choice": "erfunden", "confidence": 0.9, "probabilities": {}}}})

    assert main.answer_one(_Lost(), "Text", asked) == {}


def test_jeff_never_writes_the_official_wz_code() -> None:
    assert "wz_code" not in main.FIELDS and set(main.FIELDS) <= set(main.INSTRUCTIONS)


TREE = ({"A Land- und Forstwirtschaft, Fischerei": "A", "B Bergbau": "B"},
        {"A": {"01 Landwirtschaft": "01", "02 Forstwirtschaft": "02"}, "B": {"05 Kohlenbergbau": "05"}})


def test_the_wz_is_asked_section_first_then_its_divisions_and_both_must_be_right() -> None:
    http = _Jeff()
    assert main.choose_wz(http, "Text", TREE) == ("01 Landwirtschaft", pytest.approx(0.7 * 0.7))
    assert http.questions == [list(TREE[0]), ["01 Landwirtschaft", "02 Forstwirtschaft"]]


def test_a_section_with_one_division_needs_no_second_question() -> None:
    class _B(_Jeff):
        def post(self, url: str, *, json: dict[str, Any], timeout: int) -> _Response:
            self.questions.append(list(json["questions"]["q"]["criteria"]))
            return _Response(200, {"answers": {"q": {"choice": "B Bergbau", "confidence": 0.6, "probabilities": {}}}})

    http = _B()
    assert main.choose_wz(http, "Text", TREE) == ("05 Kohlenbergbau", 0.6)
    assert len(http.questions) == 1


def test_with_the_wz_tree_the_stored_value_is_still_the_two_digit_division() -> None:
    asked = {"wz_division_estimated": {"01 Landwirtschaft": "01", "02 Forstwirtschaft": "02", "05 Kohlenbergbau": "05"}}
    assert main.answer_one(_Jeff(), "Text", asked, TREE) == {"wz_division_estimated": ("01", pytest.approx(0.49))}


@pytest.mark.parametrize(("value", "want"), [(None, "next"), ("", "next"), ("claude", "claude"), ("anything", "next")])
def test_a_scheduled_or_unknown_run_asks_the_next_companies(monkeypatch, value, want) -> None:  # noqa: ANN001
    if value is None:
        monkeypatch.delenv("JEFF_COMPANIES", raising=False)
    else:
        monkeypatch.setenv("JEFF_COMPANIES", value)
    assert main.companies() == want
