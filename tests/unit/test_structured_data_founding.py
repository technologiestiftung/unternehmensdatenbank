"""`structured_data.main`: which founding values a page may give, and which node may give them."""
from __future__ import annotations

from datetime import date

from enrichment.structured_data import organizations
from feedback_observations.main import clean
from source_enrichment_common import normalize_company_name_for_search
from structured_data.main import decide, founding, names_company

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


def test_pages_that_disagree_on_the_year_give_nothing() -> None:
    assert decide([("founding_year", "2010"), ("founding_year", "2010")]) == ("founding_year", "2010")
    assert decide([("founding_year", "2010"), ("established_at", "2010-05-01")]) == ("established_at", "2010-05-01")
    assert decide([("founding_year", "2010"), ("founding_year", "2012")]) is None
    assert decide([("established_at", "2010-05-01"), ("established_at", "2010-06-01")]) is None
    assert decide([]) is None


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
