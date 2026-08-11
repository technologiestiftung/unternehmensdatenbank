from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup

HRB_RE = re.compile(r"\b(HR[AB])\s*[.:\s]?\s*(\d+)\b", re.IGNORECASE)
VAT_RE = re.compile(r"\bDE\s*(\d{9})\b")
POSTCODE_RE = re.compile(r"\b(\d{5})\b")

PHONE_RE = re.compile(
    r"(?:Tel(?:efon)?\.?|Phone|Fon|Ruf|Telephone|Call)\s*[.:\s]\s*([\+\d][\d\s\(\)\/\-\.]{6,25})",
    re.IGNORECASE,
)
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Matches the management label then captures up to 70 chars (name or comma-separated names).
# Covers both German (Geschäftsführer, Inhaber, …) and English (Managing Director, CEO, …).
_CEO_LABELS = (
    # German
    r"Geschäftsführer(?:in)?(?:\s+und\s+\w+)?|"
    r"Vertreten\s+durch|"
    r"Vertretungsberechtigte[rs]?|"
    r"Vorstand(?:svorsitzende[rs]?)?|"
    r"Inhaber(?:in)?|"
    r"Einzelkaufmann(?:frau)?|"
    # English
    r"Managing\s+Director[s]?|"
    r"Chief\s+Executive\s+Officer|"
    r"(?<!\w)CEO|"
    r"General\s+Manager|"
    r"Executive\s+Director[s]?|"
    r"Director[s]?\s+General|"
    r"Represented\s+by"
)
CEO_RE = re.compile(
    rf"(?:{_CEO_LABELS})\s*[:\-]?\s*([^\n\r;|<]{{4,70}})",
    re.IGNORECASE,
)
#: Zero-width: finds every label without consuming what follows it. CEO_RE swallows up to 70
#: characters, and on a page reading "Vertreten durch\nGeschäftsführer: Max Mustermann" that one
#: match eats the second label too, so re.finditer never offers it. Kept separate from CEO_RE
#: because CEO_RE is part of this module's public surface.
_CEO_LABEL_RE = re.compile(rf"(?:{_CEO_LABELS})", re.IGNORECASE)

# Noise that often follows the name on the same line
_CEO_NOISE = re.compile(r"\s*\(.*?\)\s*|,\s*\d.*$|,\s*[A-Z][a-z]+ \d.*$")

#: Dropped when they start a candidate, so "Herr Max Mustermann" yields "Max Mustermann".
_HONORIFICS = frozenset({"herr", "herrn", "frau", "familie", "hr", "fr"})

#: A token that can be part of a person's name: starts with a capital, no digits. Covers
#: "Müller-Lüdenscheidt", "O'Brien", "Dr." and "von" is handled separately below.
_NAME_TOKEN_RE = re.compile(r"^[A-ZÄÖÜ][A-Za-zÄÖÜäöüß'’.\-]*$")

#: Lowercase particles that legitimately sit inside a German name.
_NAME_PARTICLES = frozenset({"von", "van", "de", "der", "den", "zu", "zur", "am", "vom"})

#: A candidate containing any of these is a company, an address or a form label, not a person.
#: Compared token by token, lowercased and stripped of punctuation.
_NOT_A_PERSON = frozenset({
    "gmbh", "ag", "kg", "ohg", "ug", "mbh", "gbr", "se", "kgaa", "ev", "co", "ltd", "inc",
    "amtsgericht", "registergericht", "handelsregister", "register", "sitz", "impressum",
    "telefon", "telefax", "fax", "email", "mail", "adresse", "anschrift", "postfach",
    "geschäftsführer", "geschäftsführerin", "geschäftsführung", "vorstand", "inhaber",
    "inhaberin", "vertreten", "vertretungsberechtigt", "vertretungsberechtigte",
    "umsatzsteuer", "umsatzsteueridentifikationsnummer", "steuernummer", "ust", "ustidnr",
    "datenschutz", "kontakt", "deutschland", "germany", "verantwortlich", "redaktion",
    "aufsichtsbehörde", "berufsbezeichnung", "kammer", "tel", "web", "internet", "www",
})

#: A token ending in one of these is part of a street or place name.
_PLACE_SUFFIXES = ("straße", "strasse", "str", "weg", "platz", "allee", "ring", "gasse", "ufer")

#: German Impressums put a qualification next to the name: "Erika Mustermann Diplom-Kauffrau".
#: It is capitalised, so nothing else here stops it joining the name. Allowed only BEFORE the
#: name, where it is a title that reads as part of it ("Dipl.-Ing. Max Mustermann"); after two
#: name words it ends the name instead. Matched on the whole token or on a hyphen/dot piece of
#: it, so "Dipl.-Ing." and "Diplom-Kauffrau" are both caught.
_QUALIFICATION_WORDS = frozenset({
    "diplom", "dipl", "ing", "kfm", "kffr", "msc", "bsc", "mba", "llm", "mag", "rer", "oec",
    "betriebswirt", "betriebswirtin", "rechtsanwalt", "rechtsanwältin", "steuerberater",
    "steuerberaterin", "wirtschaftsprüfer", "wirtschaftsprüferin", "notar", "notarin",
})
#: Longer and distinctive enough to match on the end of a compound. Short ones stay out of this
#: list on purpose: "ing" would reject the surname "Söring".
_QUALIFICATION_SUFFIXES = (
    "kaufmann", "kauffrau", "ingenieur", "ingenieurin", "architekt", "architektin",
    "informatiker", "informatikerin", "betriebswirt", "betriebswirtin",
)


def _is_qualification(bare: str) -> bool:
    pieces = [p for p in re.split(r"[-.]", bare) if p]
    return any(p in _QUALIFICATION_WORDS for p in pieces) or bare.endswith(_QUALIFICATION_SUFFIXES)

#: A name is 2 to 4 words. One word is a first name or a job title with no way to tell which;
#: five is a sentence.
_MIN_NAME_WORDS = 2
_MAX_NAME_WORDS = 4


def extract_register_number(text: str) -> str | None:
    m = HRB_RE.search(text)
    if m:
        return f"{m.group(1).upper()} {m.group(2)}"
    return None


def extract_vat_id(text: str) -> str | None:
    m = VAT_RE.search(text)
    if m:
        return f"DE{m.group(1)}"
    return None


def extract_postcode(text: str) -> str | None:
    m = POSTCODE_RE.search(text)
    return m.group(1) if m else None


def extract_phone(text: str) -> str | None:
    m = PHONE_RE.search(text)
    if not m:
        return None
    raw = m.group(1).strip()
    # normalise whitespace, strip trailing non-digit chars
    raw = re.sub(r"\s+", " ", raw).rstrip(" .-/")
    return raw if len(raw) >= 6 else None


def extract_email(text: str) -> str | None:
    m = EMAIL_RE.search(text)
    if not m:
        return None
    addr = m.group(0).lower()
    # skip generic/placeholder addresses
    if any(skip in addr for skip in ("example.", "mustermann", "placeholder", "noreply", "no-reply")):
        return None
    return addr


def _leading_name(part: str) -> str | None:
    """Take the person's name off the front of `part`, or None if it does not start with one.

    A leading run, not the whole string. Impressum lines rarely stop after the name:
    "Geschäftsführerin: Erika Mustermann Diplom-Kauffrau eingetragen beim Amtsgericht" is one
    line. Requiring the entire line to look like a name threw that away; taking the first two to
    four capitalised words keeps it. The run stops at the first word that cannot be part of a
    name — a lowercase word, a number, a street, a legal form.
    """
    words = part.split()
    if words and words[0].strip(".,").lower() in _HONORIFICS:
        words = words[1:]

    taken: list[str] = []
    for word in words:
        bare = word.strip(".,;:").lower()
        # A particle only counts inside a name ("von Weizsäcker"), never at the start or end.
        if bare in _NAME_PARTICLES:
            if not taken:
                break
            taken.append(word)
            continue
        if not _NAME_TOKEN_RE.match(word):
            break
        # Sub-tokens too, so "Beteiligungs-GmbH" is caught. Checking the whole word against the
        # set misses it, and a company name then reads as a manager.
        if any(piece in _NOT_A_PERSON for piece in re.split(r"[-.]", bare) if piece):
            break
        if bare.endswith(_PLACE_SUFFIXES):
            break
        # A title before the name reads as part of it; a qualification after the name ends it.
        if _is_qualification(bare) and len(taken) >= _MIN_NAME_WORDS:
            break
        # A leading all-caps run is an acronym or a brand, never a first name. Allowed later in
        # the run, because some Impressums write the surname as "Max MUSTERMANN".
        if not taken and word.isupper() and len(word) >= 3:
            break
        taken.append(word)
        if len(taken) == _MAX_NAME_WORDS:
            break

    while taken and taken[-1].strip(".,;:").lower() in _NAME_PARTICLES:
        taken.pop()
    if len(taken) < _MIN_NAME_WORDS:
        return None
    name = " ".join(taken).strip(" ,;.")
    return name if 5 <= len(name) <= 60 else None


def extract_ceo_names(text: str) -> list[str]:
    """Return the manager names named on an Impressum page.

    For each "Geschäftsführer" / "Vertreten durch" / "Managing Director" label, look at what
    follows it on the same line and on the next line. The next line matters: German Impressums
    routinely put the label and the name in separate elements, and the phrasing "Vertreten durch
    den Geschäftsführer" puts a whole clause between the label and the name.
    """
    names: list[str] = []
    for match in _CEO_LABEL_RE.finditer(text):
        rest = re.sub(r"^\s*[:\-–]?[^\S\n]*", "", text[match.end():])
        # Same line first, then the next non-empty one. Two lines is enough for the layouts
        # that occur; going further starts picking up the address block below.
        lines = [line for line in rest.split("\n")[:3] if line.strip()][:2]
        for line in lines:
            cleaned = _CEO_NOISE.sub("", line).strip(" ,;")
            found_on_this_line = False
            for part in re.split(r"\s*(?:,\s*|\s+und\s+|\s*&\s*|\s*/\s*)\s*", cleaned):
                name = _leading_name(part.strip())
                if name:
                    names.append(name)
                    found_on_this_line = True
            # Stop at the first line that produced a name, so the address line underneath is
            # never read as a second manager.
            if found_on_this_line:
                break

    seen: set[str] = set()
    deduped = []
    for n in names:
        if n.lower() not in seen:
            seen.add(n.lower())
            deduped.append(n)
    return deduped


def page_text(html: str) -> str:
    """Turn an Impressum page into plain text, keeping one line per HTML element.

    The line breaks are the whole point. An Impressum names its managing director on its own
    line, or right after a heading:

        Geschäftsführer:
        Max Mustermann
        Registergericht: Amtsgericht Osnabrück HRB 12345

    `CEO_RE` above stops its capture at a line break — that is what `[^\\n\\r;|<]` means. This
    used to join every element with a space instead, so there were no line breaks left to stop
    at, the capture swallowed 70 characters of whatever followed the label, and the name filter
    in `extract_ceo_names` then threw the whole run away for having too many words. The name was
    on the page and was discarded at the last step. Measured over the Impressum pages of 29
    companies whose website we had already verified: 20 gave up a phone number or an email
    address, only 7 gave up a name.

    Horizontal whitespace is still collapsed, so a name wrapped across two inline tags stays on
    one line. Only element boundaries produce a break.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
    except Exception:
        # No parser: block-level tags become breaks, everything else becomes a space, so the
        # fallback keeps the same shape as the parsed path rather than a different one.
        text = re.sub(r"(?i)<\s*(?:br|/?p|/?div|/?li|/?tr|/?h[1-6]|/?td)\b[^>]*>", "\n", html)
        text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[^\S\n]+", " ", text)
    return re.sub(r"\s*\n\s*", "\n", text).strip()


def extract_from_html(html: str) -> dict[str, Any]:
    text = page_text(html)

    return {
        "register_number": extract_register_number(text),
        "vat_id":          extract_vat_id(text),
        "postcode":        extract_postcode(text),
        "phone":           extract_phone(text),
        "email":           extract_email(text),
        "ceo_names":       extract_ceo_names(text),
        "text_snippet":    text[:500],
    }
