"""`feedback_observations.main`: people's corrections become observations once enough people agree."""
from __future__ import annotations

from feedback_observations.main import clean, confidence, planned_observations

COMPANY = "00000000-0000-0000-0000-000000000001"
FIELDS_77 = frozenset({"Künstliche Intelligenz", "Gesundheitswirtschaft"})


def vote(author: str, field: str = "name", verdict: str = "wrong", correction: str | None = "Muster GmbH",
         company: str = COMPANY) -> dict:
    return {"company_id": company, "field_name": field, "verdict": verdict, "correction": correction,
            "author_id": author}


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


def test_a_correct_vote_by_someone_else_lowers_the_confidence() -> None:
    planned, _ = planned_observations([vote("a"), vote("b"), vote("c", verdict="correct", correction=None)])
    assert (planned[0]["agree"], planned[0]["disagree"], planned[0]["confidence"]) == (2, 1, 0.30)


def test_the_app_key_becomes_the_coordinators_field_name() -> None:
    planned, _ = planned_observations([vote("a", field="management", correction="Erika Muster"),
                                       vote("a", field="deeptech", correction="hardware")])
    assert {row["field_name"] for row in planned} == {"ceo_name_or_contact", "deeptech_class"}


def test_invalid_and_uncorrectable_corrections_are_counted_and_skipped() -> None:
    planned, skipped = planned_observations([
        vote("a", field="technologiefeld", correction="Raketenbau"),
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
    assert clean("date", "01.02.2003") == clean("date", "2003-02-01") == "2003-02-01"
    assert clean("date", "31.02.2003") is None
    assert clean("integer", "1.200") == 1200.0
    assert clean("integer", "12,5") is None
    assert clean("number", "1.200.000,50") == 1200000.5
    assert clean("number", "-3") is None
    assert clean("boolean", "Ja") == "true" and clean("boolean", "vielleicht") is None
    assert clean("register_status", "aktiv") == "aktiv" and clean("register_status", "pleite") is None
    assert clean("wz_code", "62.01") == "62.01" and clean("wz_code", "Software") is None
