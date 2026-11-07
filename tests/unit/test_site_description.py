"""The homepage's own description as `activity_description` (#152)."""
from __future__ import annotations

from enrichment import site_description as sd
from website_keywords.main import CONFIDENCE, planned_change

NO_NAMES = lambda text, company: []  # noqa: E731


def test_meta_description_first_then_og() -> None:
    both = ('<meta property="og:description" content="Von og">'
            '<meta name="Description" content="  Gerüstbau in   Berlin und Brandenburg ">')
    assert sd.meta_description(both) == "Gerüstbau in Berlin und Brandenburg"
    assert sd.meta_description('<meta property="og:description" content="Nur og">') == "Nur og"
    assert sd.meta_description("<p>keine</p>") == ""


def test_short_texts_and_contact_data_are_dropped() -> None:
    assert sd.rejection("Willkommen", names=NO_NAMES) == "too_short"
    for text in ("Ihre Apotheke. Neuköllner Str. 261, 12357 Berlin, schnell erreichbar",
                 "Büro für Sprache und Strategie – Telefon: 030 1234567 – jederzeit",
                 "Schreiben Sie uns an info@example.de für ein Angebot zur Dachsanierung",
                 "Rufen Sie an: +49 30 999 für Ihre Dachsanierung in ganz Berlin"):
        assert sd.rejection(text, names=NO_NAMES) == "contact", text
    assert sd.rejection("Seit 1988 schulen wir Betriebsräte und Personalräte in Berlin.", names=NO_NAMES) is None


def test_a_person_name_drops_the_text() -> None:
    text = "Eine Filmproduktion in Berlin, geführt von Erika Muster, für den internationalen Markt."
    assert sd.rejection(text, names=lambda t, c: ["Erika Muster"]) == "person_name"


def test_long_texts_are_cut_at_a_word() -> None:
    text = "Wort " * 100
    short = sd.shorten(text.strip())
    assert len(short) <= sd.MAX_CHARS and short.endswith("Wort…")
    assert sd.shorten("kurz genug") == "kurz genug"


def test_a_text_on_several_homepages_is_generic() -> None:
    assert sd.generic(["homepage, dokument, webpage", "homepage, dokument, webpage", "Eigene Beschreibung", None]) == {
        "homepage, dokument, webpage"}


def test_read_says_why_there_is_no_description() -> None:
    assert sd.read("<html></html>", "X GmbH") == {"description": None, "description_dropped": "none"}


def test_publish_inserts_updates_and_drops_one_value() -> None:
    assert planned_change("Gerüstbau in Berlin", None) == "insert"
    assert planned_change("Gerüstbau in Berlin", ("Gerüstbau in Berlin", CONFIDENCE)) is None
    assert planned_change("Gerüstbau in Brandenburg", ("Gerüstbau in Berlin", CONFIDENCE)) == "update"
    assert planned_change("", ("Gerüstbau in Berlin", CONFIDENCE)) == "drop"
    assert planned_change("", ("Gerüstbau in Berlin", 0.0)) is None
    assert planned_change("", None) is None
