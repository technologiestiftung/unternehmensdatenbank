"""#111: an Impressum outside Berlin, and not at the company's own address, refuses the site unless proven."""
from __future__ import annotations

from types import SimpleNamespace

from enrichment import confidence as conf
from enrichment import pipeline
from enrichment.imprint_extractor import extract_from_html, is_berlin_postcode

URL = "https://extra-bau.de"


def _company(postcode: str = "10115") -> SimpleNamespace:
    return SimpleNamespace(company_name="Extra-Bau GmbH", address=f"Musterstr. 1, {postcode} Berlin",
                           city="Berlin", postcode=postcode,
                           registration_number="Amtsgericht Berlin (Charlottenburg) HRB 34944")


def _evidence(imprint: str, company: SimpleNamespace) -> set[str]:
    html = f"<html><body><h1>Impressum</h1><p>{imprint}</p></body></html>"
    evidence, _ = pipeline._page_evidence(URL, company, html=html, extracted=extract_from_html(html))
    return evidence


def test_berlin_postcodes() -> None:
    assert is_berlin_postcode("10115") and is_berlin_postcode("14199")
    assert not is_berlin_postcode("99086") and not is_berlin_postcode(None)


def test_an_impressum_elsewhere_in_germany_is_flagged() -> None:
    assert conf.IMPRINT_OUTSIDE_BERLIN in _evidence("EXTRA BAU, Magdeburger Allee 163, 99086 Erfurt", _company())


def test_a_berlin_address_or_the_company_s_own_address_is_not() -> None:
    assert conf.IMPRINT_OUTSIDE_BERLIN not in _evidence("Extra-Bau GmbH, Musterstr. 1, 10997 Berlin", _company())
    # A Charlottenburg entry can sit outside Berlin: its own postcode on the page is a match, not a refusal.
    moved = _evidence("Extra-Bau GmbH, Hoher Weg 18, 14548 Schwielowsee", _company("14548"))
    assert conf.IMPRINT_OUTSIDE_BERLIN not in moved and conf.POSTCODE_MATCH in moved


def test_proof_lifts_it() -> None:
    base = {conf.NAME_ON_PAGE, conf.HOST_EXACT, conf.IMPRINT_FOUND, conf.IMPRINT_OUTSIDE_BERLIN}
    assert conf.score(methods=["url_pattern"], evidence=base).value == 0.0
    assert conf.score(methods=["url_pattern"], evidence=base | {conf.REGISTER_MATCH}).value > 0.35
