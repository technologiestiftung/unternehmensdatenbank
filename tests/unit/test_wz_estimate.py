"""`wz_estimate`: labels from Claude and people, judged by Claude until enough people judged (#158)."""
from __future__ import annotations

from wz_estimate import main
from wz_estimate.main import judge, labels, predictions, should_promote

DIVISIONS = {"62", "56", "41"}


def votes(*rows: tuple[str, str, str | None]) -> list[dict]:
    return [{"value": value, "verdict": verdict, "correction": correction} for value, verdict, correction in rows]


def test_a_persons_label_replaces_claudes_and_a_rejected_one_is_dropped() -> None:
    llm = {"a": "62", "b": "56", "c": "NONE", "d": "41"}
    people = {"a": votes(("62", "wrong", "56")), "b": votes(("56", "wrong", None)), "e": votes(("41", "correct", None))}
    label_of, human = labels(llm, people, DIVISIONS)
    assert label_of == {"a": "56", "d": "41", "e": "41"}  # b rejected, c is no division
    assert human == {"a", "e"}


def test_claude_judges_until_enough_people_labelled_held_out_companies(monkeypatch) -> None:
    monkeypatch.setattr(main, "MIN_HOLDOUT", 2)
    predict = {"a": "62", "b": "56", "c": "41"}.get
    label_of = {"a": "62", "b": "41", "c": "41"}
    by_claude = judge(predict, ["a", "b", "c"], label_of, {}, set(), DIVISIONS)
    assert by_claude == {"accuracy": 2 / 3, "judged": 3, "judged_by": "llm"}

    people = {"a": votes(("62", "correct", None)), "b": votes(("41", "wrong", "56"))}
    by_people = judge(predict, ["a", "b", "c"], label_of, people, {"a", "b"}, DIVISIONS)
    assert by_people == {"accuracy": 1.0, "judged": 2, "judged_by": "people"}


def test_promoted_only_when_judged_on_enough_companies_well_enough_and_better(monkeypatch) -> None:
    monkeypatch.setattr(main, "MIN_HOLDOUT", 30)
    monkeypatch.setattr(main, "MIN_ACCURACY", 0.7)
    assert should_promote({"accuracy": 0.7, "judged": 30}, None)
    assert not should_promote({"accuracy": 1.0, "judged": 29}, None)
    assert not should_promote({"accuracy": 0.69, "judged": 30}, None)
    assert not should_promote({"accuracy": 0.8, "judged": 30}, {"accuracy": 0.8, "judged": 30})


def test_a_cached_prediction_is_reused_and_kept_without_text() -> None:
    cached = {"a": {"model_version": "v2", "division": "62", "probability": 0.9},
              "b": {"model_version": "v1", "division": "56", "probability": 0.8},
              "c": {"model_version": "v1", "division": "41", "probability": 0.7}}
    texts = {"site-b": "Restaurant", "site-d": "Hochbau"}
    found, new = predictions({"a": "site-a", "b": "site-b", "c": "site-c", "d": "site-d"}, cached, "v2",
                             lambda text: ("41", 0.66666) if text == "Hochbau" else ("56", 0.9), lambda w: texts.get(w, ""))
    assert found["a"] is cached["a"] and found["c"] is cached["c"]
    assert found["b"] == {"model_version": "v2", "division": "56", "probability": 0.9}
    assert found["d"]["probability"] == 0.667
    assert new == {"b", "d"}
