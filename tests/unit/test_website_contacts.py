"""`website_contacts.main`: every address is kept, with its kind and that kind's confidence."""
from __future__ import annotations

from enrichment.imprint_extractor import extract_email, extract_emails
from website_contacts.main import EMAIL_CONFIDENCE, contacts_from_impressum, email_kind, looks_personal, same_domain


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


def test_person_shaped_mailboxes() -> None:
    assert looks_personal("erika.muster", [])
    assert looks_personal("e.muster", [])
    assert looks_personal("emuster", ["Erika Muster"])
    assert not looks_personal("info", ["Erika Muster"])
    assert not looks_personal("kontakt", [])
    assert not looks_personal("office2", [])


def test_every_address_gets_a_kind() -> None:
    site = "https://www.firma.de/"
    assert email_kind("info@firma.de", site, []) == "role"
    assert email_kind("info@berlin.firma.de", site, []) == "role"
    assert email_kind("erika.muster@firma.de", site, []) == "person"
    assert email_kind("emuster@firma.de", site, ["Erika Muster"]) == "person"
    assert email_kind("hallo@webagentur.de", site, []) == "other_domain"
    assert EMAIL_CONFIDENCE["role"] > EMAIL_CONFIDENCE["person"] > EMAIL_CONFIDENCE["other_domain"]


def test_contacts_from_an_impressum() -> None:
    text = (
        "Impressum\nFirma GmbH\nGeschäftsführer: Erika Muster\nTel.: 030 123456\n"
        "E-Mail: emuster@firma.de\ninfo@firma.de\nDatenschutz: dsb@webagentur.de"
    )
    assert contacts_from_impressum(text, "https://firma.de") == [
        ("email", "emuster@firma.de", "person", EMAIL_CONFIDENCE["person"]),
        ("email", "info@firma.de", "role", EMAIL_CONFIDENCE["role"]),
        ("email", "dsb@webagentur.de", "other_domain", EMAIL_CONFIDENCE["other_domain"]),
        ("phone", "030 123456", "impressum", 0.6),
    ]
    assert contacts_from_impressum("Impressum ohne Kontakt", "https://firma.de") == []
