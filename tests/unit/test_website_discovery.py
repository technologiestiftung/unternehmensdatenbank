"""`website_discovery.main`: the re-check rule for stored websites, and officers as comparison keys."""
from __future__ import annotations

from enrichment import confidence as conf
from enrichment.officers import officer_keys
from website_discovery.main import demotion


def test_a_website_the_page_never_identified_drops_below_the_bar() -> None:
    assert demotion({"evidence": ["company_name_on_page", "imprint_found"]}, 0.63) == conf.UNVERIFIED_CAP
    assert conf.UNVERIFIED_CAP < conf.WEBSITE_MIN_CONFIDENCE


def test_a_proven_website_stays() -> None:
    assert demotion({"evidence": ["register_number_match"]}, 0.9) is None
    assert demotion({"evidence": ["officer_name_on_imprint"]}, 0.8) is None
    assert demotion({"evidence": ["company_name_on_page", "postcode_match"]}, 0.6) is None


def test_already_below_the_bar_or_unscored_is_left_alone() -> None:
    assert demotion({"evidence": []}, conf.UNVERIFIED_CAP) is None
    assert demotion({}, None) is None


def test_officers_become_comparison_keys_people_only() -> None:
    keys = officer_keys([
        {"firstname": "Erika", "lastname": "Muster", "type": "person"},
        {"name": "Dr. Max Beispiel"},
        {"name": "Beispiel Holding GmbH", "type": "company"},
        {"name": "  "},
    ])
    assert keys == frozenset({"erika muster", "max beispiel"})
    assert officer_keys(None) == frozenset()
