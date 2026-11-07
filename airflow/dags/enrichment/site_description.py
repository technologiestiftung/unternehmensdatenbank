"""What a company says it does, in one or two sentences of its own: the homepage's description (#152).

Target column 13, "Tätigkeitsbeschreibung". The text is the homepage's `<meta name="description">`,
else its `og:description`: the company's own words, so nothing is made up. It goes into its own field,
`activity_description` ("Tätigkeit laut Website"), never into `business_purpose`, which is the
register's Unternehmensgegenstand: a different question (docs/purpose-classification.md).

Measured on the local copy of play (2026-11-07, 482 homepages in the corpus): 340 (71%) have one of the
two tags. A hand check of 50 of them: 46 say what the company does; the 4 others were a CMS's default
text, two too vague to say anything, and an address with a phone number.

A text is dropped when:
- it is shorter than `MIN_CHARS`;
- it holds contact data: an email, a phone number, a postcode with a city;
- spaCy's German model finds a person's name in it that is not part of the company's name. On the
  same 340 texts that dropped 23: about 10 real names ("run by Sol Bondy and Fred Burle"), the rest
  words it mistook for one. `imprint_extractor.looks_like_person_name` is no use here: it flagged 284
  of the 340, as it answers yes to any two capitalised words;
- the same text is on the homepage of `GENERIC_MIN_COMPANIES` or more companies (measured over every
  company read, never a list of bad phrases): a CMS default or a group's shared text.

Longer texts are cut at a word before `MAX_CHARS`: a short quote of the company's own words, not a copy
of the page.
"""
from __future__ import annotations

import re
from collections import Counter
from functools import cache
from typing import Any, Iterable

from bs4 import BeautifulSoup

MIN_CHARS = 25
MAX_CHARS = 300
#: A text on this many homepages says nothing about one company.
GENERIC_MIN_COMPANIES = 2
SPACY_MODEL = "de_core_news_md"

_CONTACT = re.compile(
    r"@|\b(?:tel|telefon|fon|fax|phone)\b\.?:?|\+\d|\d[\d /()-]{6,}\d|\b\d{5}\s+[A-ZÄÖÜ]", re.IGNORECASE
)


def meta_description(html: str) -> str:
    """The homepage's `<meta name="description">`, else its `og:description`, whitespace collapsed."""
    soup = BeautifulSoup(html or "", "html.parser")
    for attrs in ({"name": re.compile(r"^description$", re.I)}, {"property": re.compile(r"^og:description$", re.I)}):
        tag = soup.find("meta", attrs=attrs)
        text = " ".join((tag.get("content") or "").split()) if tag else ""
        if text:
            return text
    return ""


@cache
def _nlp() -> Any:
    import spacy

    return spacy.load(SPACY_MODEL, disable=["parser", "lemmatizer"])


def person_names(text: str, company_name: str = "") -> list[str]:
    """The person names spaCy finds in the text, except those inside the company's own name
    ("Marcus Mahlke Immobilien" names its owner; it is still the company's name)."""
    name = company_name.lower()
    return [e.text for e in _nlp()(text).ents if e.label_ == "PER" and e.text.lower() not in name]


def shorten(text: str) -> str:
    """At most `MAX_CHARS`, cut at a word, with "…" when cut."""
    if len(text) <= MAX_CHARS:
        return text
    return text[:MAX_CHARS - 1].rsplit(" ", 1)[0].rstrip(" ,;:-–") + "…"


def rejection(text: str, company_name: str = "", names: Any = person_names) -> str | None:
    """Why this text cannot be published for one company ("too_short", "contact", "person_name"),
    or None. `names` is the person-name finder, replaceable in tests."""
    if len(text) < MIN_CHARS:
        return "too_short"
    if _CONTACT.search(text):
        return "contact"
    if names(text, company_name):
        return "person_name"
    return None


def read(html: str, company_name: str) -> dict[str, str | None]:
    """What the read step stores per company: the description, or None and why."""
    text = meta_description(html)
    if not text:
        return {"description": None, "description_dropped": "none"}
    reason = rejection(text, company_name)
    return {"description": None if reason else shorten(text), "description_dropped": reason}


def generic(per_company: Iterable[str | None]) -> set[str]:
    """Texts on the homepages of `GENERIC_MIN_COMPANIES` or more companies."""
    counts = Counter(text for text in per_company if text)
    return {text for text, n in counts.items() if n >= GENERIC_MIN_COMPANIES}
