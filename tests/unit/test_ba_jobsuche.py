"""`ba_jobsuche`: ads grouped by employer, only Berlin postcodes kept, nothing but the employer."""
from __future__ import annotations

from ba_jobsuche.main import employers


def _ad(name: str, employer: str | None, *postcodes: str) -> dict:
    return {"firma": name, "arbeitgeberKundennummerHash": employer,
            "stellenlokationen": [{"adresse": {"plz": p, "ort": "Berlin"}} for p in postcodes]}


def test_ads_are_grouped_by_employer_with_berlin_postcodes_and_a_count() -> None:
    got = employers([_ad("EMC Adam GmbH", "h1", "13583"), _ad("EMC Adam GmbH", "h1", "13583", "14476"),
                     _ad("EMC ADAM GmbH", "h1", "10115")])
    assert got == {"h1": {"name": "EMC Adam GmbH", "postcodes": ["10115", "13583"], "ads": 3}}


def test_an_ad_without_an_employer_id_is_grouped_by_name_and_one_without_a_name_is_skipped() -> None:
    got = employers([_ad("Beyond Imaging Health GmbH", None, "10789"), _ad("", "h2", "10115")])
    assert list(got) == ["Beyond Imaging Health GmbH"]
