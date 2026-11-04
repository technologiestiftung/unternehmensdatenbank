"""`structured_data.main`: which founding values a page may give, and which node may give them."""
from __future__ import annotations

from datetime import date, datetime

import pytest

from enrichment.structured_data import organizations
from feedback_observations.main import clean
from source_enrichment_common import normalize_company_name_for_search
import structured_data.main as structured_data
from structured_data.main import decide, founding, names_company, read_company

TODAY = date(2026, 11, 4)


def test_a_full_date_is_a_date_and_a_year_alone_stays_a_year() -> None:
    assert founding("2010-05-01", TODAY) == ("established_at", "2010-05-01")
    assert founding("2010-05-01T00:00:00+02:00", TODAY) == ("established_at", "2010-05-01")
    assert founding("01.05.2010", TODAY) == ("established_at", "2010-05-01")
    # Never 1 January of that year: a precision the page did not claim.
    assert founding("2010", TODAY) == ("founding_year", "2010")
    assert founding("2010-05", TODAY) == ("founding_year", "2010")


def test_anything_else_is_nothing() -> None:
    for raw in (None, "", "seit 1990", "2010-13-01", "1650", "2030", "2026-12-01", "ca. 2010"):
        assert founding(raw, TODAY) is None, raw


def test_only_a_node_that_names_the_company_counts() -> None:
    key = normalize_company_name_for_search("Beispiel Technologies GmbH")
    assert names_company({"name": "Beispiel Technologies"}, key)
    assert names_company({"name": "Startseite", "legal_name": "Beispiel Technologies GmbH"}, key)
    assert not names_company({"name": "Kunde AG"}, key)  # a client on the same page
    assert not names_company({"name": "Beispiel Technologies"}, "ab")  # too short to compare


def test_a_partner_named_by_part_of_the_company_name_does_not_count() -> None:
    key = normalize_company_name_for_search("Siemens Partner Berlin GmbH")
    assert not names_company({"name": "Siemens"}, key)
    assert names_company({"name": "Siemens Partner Berlin"}, key)
    assert not names_company({"name": "Digital Partner"}, normalize_company_name_for_search("IT Partner GmbH"))


def test_pages_that_disagree_on_the_year_give_nothing() -> None:
    assert decide([("founding_year", "2010"), ("founding_year", "2010")]) == [("founding_year", "2010")]
    assert decide([("founding_year", "2010"), ("founding_year", "2012")]) == []
    assert decide([("established_at", "2010-05-01"), ("established_at", "2012-05-01")]) == []
    assert decide([]) == []


def test_a_full_date_also_gives_its_year_so_a_year_correction_can_win() -> None:
    assert decide([("founding_year", "2010"), ("established_at", "2010-05-01")]) == [
        ("founding_year", "2010"), ("established_at", "2010-05-01")]
    # Two dates in the same year: the year stands, the date does not.
    assert decide([("established_at", "2010-05-01"), ("established_at", "2010-06-01")]) == [("founding_year", "2010")]


def test_each_value_is_dated_by_the_pages_that_state_it(monkeypatch: pytest.MonkeyPatch) -> None:
    def page(stated: str, name: str = "Beispiel Technologies") -> str:
        return f'<script type="application/ld+json">{{"@type": "Organization", "name": "{name}", "foundingDate": "{stated}"}}</script>'
    html = {"a": page("2010"), "b": page("2010-05-01"), "c": page("2010"), "d": page("1990", "Kunde AG")}
    monkeypatch.setattr(structured_data, "_html", html.__getitem__)
    pages = [("https://beispiel.de/a", "a", datetime(2026, 3, 1)), ("https://beispiel.de/b", "b", datetime(2026, 1, 1)),
             ("https://beispiel.de/c", "c", datetime(2026, 2, 1)), ("https://beispiel.de/d", "d", datetime(2026, 9, 1))]
    values, payload = read_company("Beispiel Technologies GmbH", pages)
    assert values == [("founding_year", "2010", datetime(2026, 3, 1)),
                      ("established_at", "2010-05-01", datetime(2026, 1, 1))]
    assert [e["url"] for e in payload["evidence"]] == ["https://beispiel.de/a", "https://beispiel.de/b", "https://beispiel.de/c"]


def test_the_founding_date_comes_out_of_json_ld_as_the_page_wrote_it() -> None:
    html = ('<script type="application/ld+json">{"@type": "Organization", "name": "Beispiel Technologies GmbH",'
            ' "foundingDate": "2010"}</script>')
    assert organizations(html)[0]["founding_date"] == "2010"


def test_a_correction_in_the_app_keeps_the_year_only() -> None:
    assert clean("year", "2010") == "2010"
    assert clean("year", "01.05.2010") == "2010"
    assert clean("year", "2010-05-01") == "2010"
    assert clean("year", "1650") is None
    assert clean("year", "zweitausendzehn") is None
