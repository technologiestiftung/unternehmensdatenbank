"""`feedback_observations.main`: people's corrections become observations once enough people agree."""
from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

from feedback_observations import main
from feedback_observations.main import clean, confidence, planned_observations

COMPANY = "00000000-0000-0000-0000-000000000001"
FIELDS_77 = frozenset({"Künstliche Intelligenz", "Gesundheitswirtschaft"})


def vote(author: str, field: str = "name", verdict: str = "wrong", correction: str | None = "Muster GmbH",
         company: str = COMPANY, value: str = "Alte GmbH") -> dict:
    return {"company_id": company, "field_name": field, "value": value, "verdict": verdict,
            "correction": correction, "author_id": author}


def test_one_person_stays_below_every_bar_and_two_people_are_shown() -> None:
    assert confidence(1, 0) < 0.35  # the website bar, the lowest
    assert confidence(2, 0) >= 0.50
    assert confidence(5, 0) == confidence(3, 0) == 0.75


def test_people_who_call_the_shown_value_correct_count_against_a_correction() -> None:
    assert confidence(2, 1) == confidence(1, 0)
    assert confidence(1, 1) == confidence(1, 3) == 0.0


def test_the_same_correction_by_two_people_is_one_claim_ignoring_case_and_spaces() -> None:
    planned, skipped = planned_observations([vote("a"), vote("b", correction="  muster   gmbh ")])
    assert skipped == {}
    assert len(planned) == 1
    row = planned[0]
    assert (row["field_name"], row["value"], row["agree"], row["confidence"]) == ("name", "Muster GmbH", 2, 0.60)


def test_one_person_counts_once_however_often_they_correct() -> None:
    planned, _ = planned_observations([vote("a"), vote("a", correction="MUSTER GMBH")])
    assert planned[0]["agree"] == 1


def test_confirming_the_old_value_counts_against_a_correction() -> None:
    planned, _ = planned_observations([vote("a"), vote("b"), vote("c", verdict="correct", correction=None)])
    assert (planned[0]["agree"], planned[0]["disagree"], planned[0]["confidence"]) == (2, 1, 0.30)


def test_confirming_the_correction_counts_for_it() -> None:
    planned, _ = planned_observations([vote("a"), vote("b"),
                                       vote("c", verdict="correct", correction=None, value="muster gmbh")])
    assert (planned[0]["agree"], planned[0]["disagree"], planned[0]["confidence"]) == (3, 0, 0.75)


def test_calling_the_correction_itself_wrong_counts_against_it() -> None:
    planned, _ = planned_observations([vote("a"), vote("b"),
                                       vote("c", correction=None, value="Muster GmbH")])
    assert (planned[0]["agree"], planned[0]["disagree"]) == (2, 1)


def test_only_a_persons_latest_vote_counts() -> None:
    planned, _ = planned_observations([vote("a"), vote("b"), vote("a", verdict="correct", correction=None)])
    assert (planned[0]["agree"], planned[0]["disagree"]) == (1, 1)


def test_a_changed_mind_moves_the_person_to_the_new_correction() -> None:
    planned, _ = planned_observations([vote("a", correction="B GmbH"), vote("b", correction="B GmbH"),
                                       vote("a", value="B GmbH", correction="C GmbH")])
    counts = {row["value"]: (row["agree"], row["disagree"], row["confidence"]) for row in planned}
    assert counts == {"B GmbH": (1, 0, 0.30), "C GmbH": (1, 0, 0.30)}


def test_the_app_key_becomes_the_coordinators_field_name() -> None:
    planned, _ = planned_observations([vote("a", field="management", correction="Erika Muster"),
                                       vote("a", field="deeptech", correction="hardware")])
    assert {row["field_name"] for row in planned} == {"ceo_name_or_contact", "deeptech_class"}


def test_invalid_and_uncorrectable_corrections_are_counted_and_skipped() -> None:
    planned, skipped = planned_observations([
        vote("b", field="technologiefeld", correction="Raketenbau"),
        vote("a", field="fundings", correction="01AB2345"),
        vote("a", field="technologiefeld", correction="Künstliche Intelligenz"),
    ], FIELDS_77)
    assert skipped == {"invalid technologiefeld": 1, "not correctable: fundings": 1}
    assert [row["value"] for row in planned] == ["Künstliche Intelligenz"]


def test_a_wrong_vote_without_a_correction_writes_nothing() -> None:
    assert planned_observations([vote("a", correction=None)]) == ([], {})


def test_values_are_cleaned_to_what_the_coordinator_and_the_profile_read() -> None:
    assert clean("url", "www.Muster.de/impressum") == "https://www.muster.de"
    assert clean("url", "keine ahnung") is None
    assert clean("url", "user@evil.de") is None
    assert clean("url", "javascript:alert(1)") is None
    assert clean("url", "ftp://muster.de") is None
    assert clean("url", "muster.de:8080") is None
    assert clean("url", "http://Shop.Muster-Firma.de/") == "http://shop.muster-firma.de"
    assert clean("url", "bäckerei-müller.de") == "https://xn--bckerei-mller-bfb28a.de"
    assert clean("date", "01.02.2003") == clean("date", "2003-02-01") == "2003-02-01"
    assert clean("date", "31.02.2003") is None
    assert clean("integer", "1.200") == 1200.0
    assert clean("integer", "12,5") is None
    assert clean("number", "1.200.000,50") == 1200000.5
    assert clean("number", "-3") is None
    assert clean("boolean", "Ja") == "true" and clean("boolean", "vielleicht") is None
    assert clean("register_status", "aktiv") == "aktiv" and clean("register_status", "pleite") is None
    assert clean("wz_code", "62.01") == "62.01" and clean("wz_code", "Software") is None


def test_one_person_fills_an_empty_field_but_not_one_a_source_fills() -> None:
    assert confidence(1, 0, filled=False) >= 0.50
    assert confidence(1, 0, filled=True) < 0.35
    assert confidence(1, 1, filled=False) == 0.0
    votes = [vote("a", field="employee_count", correction="12"), vote("a", field="website", correction="muster.de")]
    planned, _ = planned_observations(votes, filled={(COMPANY, "website")})
    shown = {row["field_name"]: row["confidence"] for row in planned}
    assert shown == {"employee_count": 0.60, "website": 0.30}


class _Cursor:
    """Just enough of a psycopg2 cursor for `run_publish`: votes, no other source, one stored observation."""

    def __init__(self, votes: list[dict], stored: list[tuple]) -> None:
        self.votes, self.stored, self.inserts, self.rowcount = votes, stored, [], 0
        self.description, self.rows = None, []

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.rowcount = 0
        if "FROM public.extraction_feedback" in sql:
            self.description = [SimpleNamespace(name=n) for n in self.votes[0]]
            self.rows = [tuple(v.values()) for v in self.votes]
        elif "value_text, value_number" in sql:
            self.rows = self.stored
        elif sql.lstrip().startswith("INSERT"):
            self.inserts.append(params)
            self.rows = [("new",)]
        else:
            self.rows = []

    def fetchall(self) -> list:
        return self.rows

    def fetchone(self) -> tuple:
        return self.rows[0]


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


def test_a_different_earliest_spelling_updates_the_stored_claim_instead_of_adding_one(monkeypatch) -> None:
    cur = _Cursor([vote("b", correction="muster gmbh"), vote("a")],
                  [(COMPANY, "name", "Muster GmbH", None, "obs-1", 0.60)])
    monkeypatch.setattr(main, "get_connection", lambda: _Connection(cur))
    monkeypatch.setattr(main, "ensure_source", lambda *a, **k: "source")
    monkeypatch.setattr(main, "start_sync_run", lambda *a, **k: "run")
    monkeypatch.setattr(main, "finish_sync_run", lambda *a, **k: None)
    monkeypatch.setattr(main, "upsert_source_record", lambda *a, **k: ("record", False))
    monkeypatch.setattr(main, "_technologiefelder", frozenset)
    assert main.run_publish() == 0
    assert cur.inserts == []

