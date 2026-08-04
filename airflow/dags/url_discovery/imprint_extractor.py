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
# Noise that often follows the name on the same line
_CEO_NOISE = re.compile(r"\s*\(.*?\)\s*|,\s*\d.*$|,\s*[A-Z][a-z]+ \d.*$")


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


def extract_ceo_names(text: str) -> list[str]:
    """Return a list of individual manager names found in the imprint text."""
    names: list[str] = []
    for m in CEO_RE.finditer(text):
        raw = m.group(1).strip()
        raw = _CEO_NOISE.sub("", raw).strip(" ,;")
        # split on common name separators
        parts = re.split(r"\s*(?:,\s*|\s+und\s+|\s*/\s*)\s*", raw)
        for part in parts:
            part = part.strip()
            # must look like a name: 2–5 words, each capitalised, reasonable length
            words = part.split()
            if (
                2 <= len(words) <= 5
                and all(re.match(r"[A-ZÄÖÜ\-\.]", w) for w in words)
                and 5 <= len(part) <= 60
            ):
                names.append(part)
    seen: set[str] = set()
    deduped = []
    for n in names:
        if n not in seen:
            seen.add(n)
            deduped.append(n)
    return deduped


def extract_from_html(html: str) -> dict[str, Any]:
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)

    return {
        "register_number": extract_register_number(text),
        "vat_id":          extract_vat_id(text),
        "postcode":        extract_postcode(text),
        "phone":           extract_phone(text),
        "email":           extract_email(text),
        "ceo_names":       extract_ceo_names(text),
        "text_snippet":    text[:500],
    }
