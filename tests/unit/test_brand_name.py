"""A brand before an embedded legal form, and a register number printed with a space (issue #30).

"3pc GmbH Neue Kommunikation" has the website 3pc.de and prints its entry as "HRB 794 28".
"""
from __future__ import annotations

from enrichment.imprint_extractor import extract_register_numbers
from enrichment.own_site import own_site_verdict
from enrichment.url_patterns import brand_stem, generate_candidates

NAME = "3pc GmbH Neue Kommunikation"


def test_the_words_before_an_embedded_legal_form_are_the_brand() -> None:
    assert brand_stem(NAME) == "3pc"


def test_a_name_that_ends_with_its_legal_form_has_no_brand() -> None:
    assert brand_stem("Erika Muster GmbH") == ""
    assert brand_stem("Muster GmbH & Co. KG") == ""


def test_a_brand_too_short_generic_or_a_given_name_is_refused() -> None:
    assert brand_stem("ab GmbH Handel") == ""
    assert brand_stem("Bau GmbH Berlin") == ""
    assert brand_stem("Karl GmbH Gastronomie") == ""
    assert brand_stem("Verwaltungs GmbH Muster") == ""


def test_the_brand_is_the_first_guess() -> None:
    assert generate_candidates(NAME)[0].host == "3pc.de"
    assert generate_candidates(NAME)[0].pattern == "brand"


def test_the_brand_counts_as_the_name_only_as_the_whole_host() -> None:
    assert own_site_verdict("https://3pc.de", NAME) == "own_site"
    assert own_site_verdict("https://www.3pc.de/impressum", NAME) == "own_site"
    assert own_site_verdict("https://my3pc.de", NAME) == "host_name_mismatch"
    assert own_site_verdict("https://3pcx.de", NAME) == "host_name_mismatch"


def test_a_register_number_with_a_space_is_read_both_ways() -> None:
    assert extract_register_numbers("Amtsgericht Berlin HRB 794 28 USt-IdNr.") == ["HRB 794", "HRB 79428"]
    assert extract_register_numbers("HRB 794 28.") == ["HRB 794", "HRB 79428"]


def test_a_postcode_or_a_price_after_the_entry_is_never_glued_on() -> None:
    assert extract_register_numbers("HRB 12345 10115 Berlin") == ["HRB 12345"]
    assert extract_register_numbers("HRB 123 4,50 EUR") == ["HRB 123"]
    assert extract_register_numbers("HRB 222 442 B") == ["HRB 222442"]
