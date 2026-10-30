"""`technology_field.main`: which text a keyword fired in, and weighing over every company read."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from enrichment import branch_classifier as bc
from enrichment import branch_observations
from technology_field import main, train
from technology_field.main import classify_all, keywords_by_source

YAML = """
branches:
  Bahnindustrie:
    name_en: Rail industry
    keywords: [Schienenfahrzeuge, Stellwerk]
  Photonik:
    name_en: Photonics
    keywords: [Lasertechnik]
"""


def _taxonomy(tmp_path: Path) -> bc.Taxonomy:
    path = tmp_path / "taxonomy.yaml"
    path.write_text(YAML, encoding="utf-8")
    return bc.load_taxonomy(path)


def test_a_keyword_counts_for_the_first_text_it_fired_in(tmp_path: Path) -> None:
    taxonomy = _taxonomy(tmp_path)
    texts = {"services": "Wir bauen Schienenfahrzeuge.", "homepage": "Schienenfahrzeuge und Lasertechnik"}
    assert keywords_by_source(texts, taxonomy) == {"schienenfahrzeuge": "services", "lasertechnik": "homepage"}


def test_never_inside_a_compound(tmp_path: Path) -> None:
    assert keywords_by_source({"homepage": "Stellwerkstechnik"}, _taxonomy(tmp_path)) == {}


def test_weighed_over_all_companies_and_deleted_keywords_dropped(tmp_path: Path) -> None:
    taxonomy = _taxonomy(tmp_path)
    reads = {f"other-{i}": ({}, True) for i in range(50)}
    reads["rail"] = ({"schienenfahrzeuge": "services", "stellwerk": "services", "removed": "homepage"}, True)
    reads["silent"] = ({}, True)
    reads["empty"] = ({}, False)
    answers = classify_all(reads, taxonomy)
    assert answers["rail"].branch == "Bahnindustrie"
    assert "removed" not in answers["rail"].keywords
    assert answers["silent"].outcome == "no_keyword"
    assert answers["empty"].outcome == "no_text"


class _Cursor:
    """Just enough of a psycopg2 cursor for `run_classify`: the read records, the values shown."""

    def __init__(self, records: list[tuple], shown: list[tuple]) -> None:
        self.records, self.shown, self.rows, self.drops, self.cached = records, shown, [], [], []

    def execute(self, sql: str, params: tuple = ()) -> None:
        if "'textcat'" in sql:
            self.rows = []
        elif "FROM public.company_source_records" in sql:
            self.rows = self.records
        elif "FROM public.company_field_observations" in sql:
            self.rows = self.shown
        elif "UPDATE public.company_field_observations" in sql:
            self.drops.append(params)
        elif "UPDATE public.company_source_records" in sql:
            self.cached.append(params)

    def fetchall(self) -> list:
        return self.rows


class _Connection:
    def __init__(self, cur: _Cursor) -> None:
        self.cur = cur

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> nullcontext:
        return nullcontext(self.cur)

    def close(self) -> None:
        return None


READ_AT = datetime(2026, 3, 1, tzinfo=timezone.utc)


def _run_classify(
    monkeypatch, tmp_path: Path, records: list[tuple], shown: list[tuple], model: object = None
) -> tuple[_Cursor, list]:
    taxonomy = _taxonomy(tmp_path)
    monkeypatch.setattr(train, "load_current", lambda: ("v1", model) if model else (None, None))
    monkeypatch.setattr(train, "stored_text", lambda website: f"Text von {website}")
    monkeypatch.setattr(train, "require_store", lambda: None)
    cur, inserts = _Cursor(records, shown), []
    monkeypatch.setattr(main.bc, "load_taxonomy", lambda: taxonomy)
    # The write path translates to English through its own cached copy of the real YAML.
    monkeypatch.setattr(branch_observations, "_taxonomy", lambda: taxonomy)
    monkeypatch.setattr(main, "MIN_COMPANIES", 10)
    monkeypatch.setattr(main, "get_connection", lambda: _Connection(cur))
    monkeypatch.setattr(main, "ensure_source", lambda *a, **k: "source")
    monkeypatch.setattr(main, "start_sync_run", lambda *a, **k: "run")
    monkeypatch.setattr(main, "finish_sync_run", lambda *a, **k: None)
    monkeypatch.setattr(main, "insert_text_observation", lambda cur, **k: inserts.append(k))
    main.run_classify()
    return cur, inserts


def _records() -> list[tuple]:
    rail = ("rail", "record-rail", READ_AT, {"schienenfahrzeuge": "services", "stellwerk": "services"}, True, True,
            "https://rail.de")
    return [rail] + [(f"other-{i}", f"record-{i}", READ_AT, {}, True, True, f"https://other-{i}.de") for i in range(50)]


def test_a_value_is_dated_by_the_page_read_and_old_values_drop_to_zero(monkeypatch, tmp_path: Path) -> None:
    shown = [("rail", "technology_field", "Photonics"), ("gone", "technology_field", "Photonics")]
    cur, inserts = _run_classify(monkeypatch, tmp_path, _records(), shown)
    assert [(k["company_id"], k["value_text"], k["source_updated_at"]) for k in inserts] == [
        ("rail", "Rail industry", READ_AT)]
    # Neither the company's old value nor one of a company no longer read stays shown; nothing deleted.
    assert sorted(p[0] for p in cur.drops) == ["gone", "rail"]


def test_nothing_is_written_before_enough_companies_are_read(monkeypatch, tmp_path: Path) -> None:
    cur, inserts = _run_classify(monkeypatch, tmp_path, _records()[:5], [("gone", "technology_field", "Photonics")])
    assert inserts == [] and cur.drops == []


class _Model:
    """A text classifier that is sure only about other-0's website."""

    def __call__(self, text: str) -> SimpleNamespace:
        sure = "other-0." in text
        return SimpleNamespace(cats={"Photonik": 0.9 if sure else 0.5, "Bahnindustrie": 0.1 if sure else 0.5})


def test_a_promoted_model_fills_only_where_no_keyword_fired(monkeypatch, tmp_path: Path) -> None:
    cur, inserts = _run_classify(monkeypatch, tmp_path, _records(), [], model=_Model())
    by_company = {k["company_id"]: k for k in inserts}
    # The keywords keep rail; the model fills other-0 and stays out where it is not sure enough.
    assert sorted(by_company) == ["other-0", "rail"]
    assert by_company["rail"]["metadata"]["decided_by"] == "keywords"
    filled = by_company["other-0"]
    assert (filled["value_text"], filled["confidence"]) == ("Photonics", 0.53)
    assert filled["metadata"]["decided_by"] == "textcat" and filled["metadata"]["model_name"] == "branch_textcat:v1"
    # Every gap company's prediction is cached with the model version, merged into its payload.
    assert len(cur.cached) == 50 and cur.drops == []


class _NoModel:
    def __call__(self, text: str) -> None:
        raise AssertionError("the model was called")


def test_a_cached_prediction_of_the_model_in_use_is_reused(monkeypatch) -> None:
    monkeypatch.setattr(train, "stored_text", lambda website: "Text")
    cached = {"model_version": "v1", "label": "Photonik", "probability": 0.9}
    predictions, new = main.gap_predictions({"a": ("https://a.de", cached)}, "v1", _NoModel())
    assert predictions == {"a": cached} and new == {}


def test_a_new_model_predicts_again_but_never_on_no_text(monkeypatch) -> None:
    monkeypatch.setattr(train, "stored_text", lambda website: "" if "gone" in website else "other-0.")
    old = {"model_version": "v0", "label": "Bahnindustrie", "probability": 0.7}
    predictions, new = main.gap_predictions(
        {"a": ("https://other-0.de", old), "gone": ("https://gone.de", old), "none": ("https://gone.de/x", None)},
        "v1", _Model())
    assert new == {"a": {"model_version": "v1", "label": "Photonik", "probability": 0.9}}
    assert predictions == {**new, "gone": old}


def test_a_store_failure_stops_the_run_before_anything_is_written(monkeypatch) -> None:
    monkeypatch.setattr(train.corpus, "configured", lambda: True)
    monkeypatch.setattr(train.corpus, "enabled", lambda: False)
    with pytest.raises(RuntimeError):
        train.require_store()
