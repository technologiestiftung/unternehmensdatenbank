"""`enrichment.wikidata`: SPARQL rows become one item per Wikidata id; only commercial register digits.
`wikidata.main`: which companies lose what an item proved for them, and which URLs drop to 0."""
from __future__ import annotations

from enrichment.confidence import METHOD_WIKIDATA, UNVERIFIED_CAP, WEBSITE_MIN_CONFIDENCE, score
from enrichment.wikidata import parse, register_digits
from enrichment.proposals import unproven
from wikidata.main import moved_from


def row(qid: str, website: str, label: str | None = None, register: str | None = None) -> dict:
    out = {"item": {"value": f"http://www.wikidata.org/entity/{qid}"}, "website": {"value": website}}
    if label:
        out["label"] = {"value": label}
    if register:
        out["register"] = {"value": register}
    return out


def test_two_websites_of_one_item_are_one_item() -> None:
    items = parse([row("Q1", "https://b.de", "Beispiel GmbH", "165662"), row("Q1", "https://a.de")])
    assert len(items) == 1
    assert items[0].websites == ("https://a.de", "https://b.de")
    assert (items[0].label, items[0].register_digits) == ("Beispiel GmbH", "165662")


def test_only_commercial_register_digits_are_kept() -> None:
    assert register_digits("165662") == "165662"
    assert register_digits("HRB 165662 B") == "165662"
    assert register_digits("VR 36562 B") is None  # association register: we have no such numbers
    assert register_digits(None) is None


def test_a_wikidata_website_nothing_on_the_page_confirms_stays_below_the_bar() -> None:
    # Wikidata only proposes: its base alone must never publish a website.
    verdict = score(methods=[METHOD_WIKIDATA], evidence=set(), name_uniqueness=1.0)
    assert verdict.value <= UNVERIFIED_CAP < WEBSITE_MIN_CONFIDENCE


def test_a_wikidata_website_with_the_register_number_on_the_page_is_published() -> None:
    verdict = score(methods=[METHOD_WIKIDATA], evidence={"register_number_match"}, name_uniqueness=0.5)
    assert verdict.value >= WEBSITE_MIN_CONFIDENCE
    assert verdict.verified_by == "register_number_match"


def test_only_an_item_that_now_matches_another_company_takes_its_websites_away() -> None:
    previous = {"Q1": "a", "Q2": "b", "Q3": "c"}
    # Q1 moved to company d; Q2 matches b again; Q3 stopped matching and keeps what its page proved.
    assert moved_from(previous, {"Q1": "d", "Q2": "b", "Q4": "e"}) == {"a"}


def test_a_url_the_page_no_longer_proves_drops_unless_another_record_proves_it() -> None:
    observations = [
        ("old-website", "c", "https://a.de"),   # proven 90 days ago, now only a candidate
        ("new-candidate", "c", "https://a.de"),  # just written below the bar: keeps its confidence
        ("down", "c", "https://down.de"),        # unreachable now, nobody proves it
        ("other", "c", "https://b.de"),          # another item of company c still proves it
    ]
    proven = {("c", "https://b.de"), ("d", "https://a.de")}
    assert unproven(observations, proven, frozenset({"new-candidate"})) == ["old-website", "down"]
