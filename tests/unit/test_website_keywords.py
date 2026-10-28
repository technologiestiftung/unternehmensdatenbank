"""`enrichment.activity_keywords` and `website_keywords.main`: keywords the site states, generic ones measured out."""
from __future__ import annotations

from enrichment import activity_keywords as ak
from source_enrichment_common import failed_read_payload
from website_keywords.main import CONFIDENCE, planned_write

HOMEPAGE = '<html><head><meta name="Keywords" content="Kältetechnik, Klimaanlagen; Wartung"></head></html>'
SERVICES = """
<h1>Unsere Leistungen</h1>
<h2>Kältetechnik</h2><h2>Wärmepumpen</h2>
<h3>Dr. Erika Muster</h3>
<h3>Wir planen, bauen und warten Ihre Anlage seit 1990 in ganz Berlin</h3>
<h3>info@firma.de</h3><h3>2024</h3><h3>Kälte Muster</h3>
<h3>Rufen Sie uns an:</h3><h3>Was ist ein Kältemittel?</h3><h3>Susanne Beispiel</h3><h3>Frau Beispiel</h3>
"""


def test_candidates_come_from_meta_keywords_and_offer_headings() -> None:
    found = ak.candidates(HOMEPAGE, [SERVICES], company_name="Kälte Muster GmbH")
    assert found == ["Kältetechnik", "Klimaanlagen", "Wartung", "Wärmepumpen"]


def test_names_and_sentences_are_never_keywords() -> None:
    for heading in ("Susanne Beispiel", "Dr. Erika Muster", "Frau Beispiel", "Rufen Sie uns an:",
                    "Was ist ein Kältemittel?", "Erika Muster – Inhaberin", "Erika Muster, Geschäftsführerin",
                    "Erika Muster (Inhaberin)", "Kontakt: Erika Muster", "ERIKA MUSTER", "Erika Muster Inhaberin",
                    "Inhaberin Erika Muster", "Ansprechpartnerin Erika Muster", "Erika Muster-Inhaberin"):
        assert not ak.is_candidate(heading), heading
    # The price of that rule, kept visible: a capitalised two-word phrase reads as a name too.
    assert not ak.is_candidate("Erneuerbare Energien") and not ak.is_candidate("Heizung / Sanitär")
    assert ak.is_candidate("Kältetechnik") and ak.is_candidate("erneuerbare Energien")
    assert ak.is_candidate("KÄLTETECHNIK") and ak.is_candidate("Heizung und Sanitär")


def test_no_meta_tag_and_no_headings_give_nothing() -> None:
    assert ak.candidates("<html></html>", []) == []


def test_generic_terms_are_measured_over_companies() -> None:
    per_company = [["Unsere Leistungen", f"Thema {i}"] for i in range(ak.MIN_CORPUS)]
    per_company[0].append("unsere leistungen")  # one company repeating a term counts once
    generic = ak.generic_terms(per_company)
    assert generic == {"unsere leistungen"}


def test_nothing_is_generic_before_there_is_enough_to_measure() -> None:
    assert ak.generic_terms([["Unsere Leistungen"]] * (ak.MIN_CORPUS - 1)) == set()


def test_keywords_drop_generic_terms_and_are_capped() -> None:
    terms = ["Unsere Leistungen"] + [f"Thema {i}" for i in range(20)]
    found = ak.keywords(terms, {"unsere leistungen"})
    assert found[0] == "Thema 0" and len(found) == ak.MAX_KEYWORDS


def test_publish_inserts_updates_and_drops() -> None:
    assert planned_write(["Kältetechnik"], set(), None) == ("insert", ["Kältetechnik"])
    assert planned_write(["Kältetechnik"], set(), ("Kältetechnik", CONFIDENCE)) is None
    assert planned_write(["Kältetechnik", "Wartung"], {"wartung"}, ("Kältetechnik; Wartung", CONFIDENCE)) == (
        "update", ["Kältetechnik"])
    assert planned_write(["Wartung"], {"wartung"}, ("Wartung", CONFIDENCE)) == ("drop", [])
    assert planned_write(["Wartung"], {"wartung"}, ("Wartung", 0.0)) is None
    assert planned_write([], set(), None) is None


def test_a_failed_read_is_marked_and_counted() -> None:
    first = failed_read_payload({"candidates": [], "pages": []}, 0)
    assert first == {"candidates": [], "pages": [], "failed": True, "attempts": 1}
    assert failed_read_payload(first, first["attempts"])["attempts"] == 2
