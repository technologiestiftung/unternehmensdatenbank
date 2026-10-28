"""`website_contacts.main`: only the company's own, non-personal address and its phone are kept."""
from __future__ import annotations

from enrichment.imprint_extractor import extract_email, extract_emails
from website_contacts.main import company_email, contacts_from_impressum, looks_personal, same_domain


def test_every_address_is_read_and_placeholders_are_not() -> None:
    text = "Kontakt: Info@Firma.de, noreply@firma.de, erika.muster@firma.de, info@firma.de"
    assert extract_emails(text) == ["info@firma.de", "erika.muster@firma.de"]
    assert extract_email(text) == "info@firma.de"
    assert extract_email("noreply@firma.de, info@firma.de") is None  # unchanged: first match only


def test_domain_must_be_the_websites() -> None:
    assert same_domain("firma.de", "firma.de")
    assert same_domain("firma.de", "shop.firma.de") and same_domain("berlin.firma.de", "firma.de")
    assert not same_domain("webagentur.de", "firma.de")
    assert not same_domain("notfirma.de", "firma.de")
    assert not same_domain("", "firma.de")


def test_person_shaped_mailboxes_are_left_out() -> None:
    assert looks_personal("erika.muster", [])
    assert looks_personal("e.muster", [])
    assert looks_personal("emuster", ["Erika Muster"])
    assert not looks_personal("info", ["Erika Muster"])
    assert not looks_personal("kontakt", [])
    assert not looks_personal("office2", [])


def test_first_own_role_address_wins() -> None:
    emails = ["hallo@webagentur.de", "erika.muster@firma.de", "info@firma.de", "presse@firma.de"]
    assert company_email(emails, "https://www.firma.de/", []) == "info@firma.de"
    assert company_email(["erika.muster@firma.de"], "https://firma.de", []) is None


def test_contacts_from_an_impressum() -> None:
    text = "Impressum\nFirma GmbH\nGeschäftsführer: Erika Muster\nTel.: 030 123456\nE-Mail: emuster@firma.de\ninfo@firma.de"
    assert contacts_from_impressum(text, "https://firma.de") == {"email": "info@firma.de", "phone": "030 123456"}
    assert contacts_from_impressum("Impressum ohne Kontakt", "https://firma.de") == {}
