"""#111: a site run from abroad is refused unless it proves the company; "HR B 61871" is a register entry."""
from __future__ import annotations

import pytest

from enrichment import confidence as conf
from enrichment.imprint_extractor import extract_register_numbers, is_abroad

PRIVACY_TAIL = " Datenschutz " + "x" * 200 + " Google LLC, 1600 Amphitheatre Parkway, Mountain View, CA 94043, USA"


@pytest.mark.parametrize("text", [
    "Ignite Sales, 14755 Preston Rd, Suite 400 Dallas, TX 75254, USA",   # house number like a postcode
    "Hollenzen 105/6, 6290 Mayrhofen, UID-Nummer ATU 70793037",
    "DanConnect · Brennerpasset 71 · DK-6000 Kolding · Tel.: +45 2810 4828",
    "Berlin Heals Holding AG, Handelsregister Nr.: CH-020.3.040.130-8",
])
def test_a_business_abroad_is_recognised(text: str) -> None:
    assert is_abroad(text)


@pytest.mark.parametrize("text", [
    "EXTRA BAU, Magdeburger Allee 163, 99086 Erfurt" + PRIVACY_TAIL,     # US address only in the privacy text
    "Kolonnenstr. 8, 10827 Berlin, Büro London +44 20 7946 0000",       # a German postcode wins over a phone
    "NobleProg Limited, Telefon +49 30 7543 6104",                       # +49 means a German presence
    "Preis: +1 Monat gratis",                                            # not a phone number
    "Über +100 zufriedene Kunden und +750 Projekte, +1.000 Kunden",      # counts, not phone numbers
    "Hotline +4930 1234567, London +44 20 7946 0000",                    # unspaced +49 is German
    "Fax 004930 1234567, Austria",                                       # unspaced 0049 is German
    "Musterstr. 1, DE 10115 Berlin",                                     # DE prefix, not a US state
])
def test_a_german_page_is_not_abroad(text: str) -> None:
    assert not is_abroad(text)


def test_a_register_entry_with_a_space_is_read() -> None:
    assert extract_register_numbers("Registergericht Hannover; HR B 61871") == ["HRB 61871"]
    assert extract_register_numbers("die HR Abteilung 2024") == []


def test_abroad_disqualifies_unless_the_page_proves_the_company() -> None:
    base = {conf.NAME_ON_PAGE, conf.HOST_EXACT, conf.IMPRINT_FOUND}
    assert conf.score(methods=["url_pattern"], evidence=base | {conf.IMPRINT_ABROAD}).value == 0.0
    for proof in (conf.REGISTER_MATCH, conf.OFFICER_MATCH):
        assert conf.score(methods=["url_pattern"], evidence=base | {conf.IMPRINT_ABROAD, proof}).value > 0.35
