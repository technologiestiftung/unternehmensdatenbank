"""`cordis.main`: Berlin company participations out of CORDIS's CSV rows (#104)."""
from __future__ import annotations

from datetime import date

from cordis.main import funding_rows, is_berlin_company


def org(**values: str) -> dict[str, str]:
    base = {"projectID": "101069359", "organisationID": "999", "name": "GREENDELTA GMBH", "country": "DE",
            "city": "Berlin", "activityType": "PRC", "ecContribution": "299250,5"}
    return base | values


PROJECTS = {"101069359": {"acronym": "SolDAC", "title": "Full spectrum SOLar Direct Air Capture",
                          "startDate": "2022-09-01", "endDate": "2025-08-31"}}


def test_a_berlin_company_participation_becomes_one_grant_row() -> None:
    (row,) = funding_rows([org()], PROJECTS, "Horizon Europe")
    assert row == ("cordis-101069359-999", "GREENDELTA GMBH", "Berlin", 299250.5, date(2022, 9, 1),
                   date(2025, 8, 31), "SolDAC: Full spectrum SOLar Direct Air Capture", "Europäische Kommission",
                   "Horizon Europe", "cordis")


def test_only_private_companies_in_berlin() -> None:
    assert is_berlin_company(org(city="BERLIN-ADLERSHOF"))
    assert not is_berlin_company(org(activityType="REC"))  # a research institute
    assert not is_berlin_company(org(city="Potsdam"))
    assert not is_berlin_company(org(city="Berlingerode"))  # Thuringia
    assert not is_berlin_company(org(city="Berlinchen"))  # Brandenburg
    assert not is_berlin_company(org(country="AT", city="Berlin"))


def test_missing_project_and_amount_leave_the_fields_empty() -> None:
    (row,) = funding_rows([org(ecContribution="", projectAcronym="X")], {}, "Horizon 2020")
    assert row[3] is None and row[4] is None and row[6] == "X"


def test_an_organisation_listed_twice_in_a_project_is_one_row_with_both_amounts() -> None:
    (row,) = funding_rows([org(ecContribution="100"), org(ecContribution="50")], PROJECTS, "Horizon Europe")
    assert row[3] == 150.0
