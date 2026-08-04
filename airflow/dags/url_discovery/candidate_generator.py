
from __future__ import annotations

import re

LEGAL_SUFFIX_RE = re.compile(
    r"\b(?:"
    r"GmbH\s*&\s*Co\.?\s*KGaA"
    r"|GmbH\s*&\s*Co\.?\s*KG"
    r"|GmbH\s*&\s*Co\.?"
    r"|GmbH"
    r"|AG\s*&\s*Co\.?\s*KG"
    r"|KGaA"
    r"|AG"
    r"|KG"
    r"|OHG"
    r"|GbR"
    r"|PartGmbB"
    r"|PartG"
    r"|UG\s*\(haftungsbeschränkt\)"
    r"|UG"
    r"|e\.?\s*V\.?"
    r"|e\.?\s*K\.?"
    r"|SE"
    r"|mbH"
    r"|Ltd\.?"
    r"|Inc\.?"
    r")\b",
    flags=re.IGNORECASE,
)

_UMLAUT = {"ä": "ae", "ö": "oe", "ü": "ue", "Ä": "ae", "Ö": "oe", "Ü": "ue", "ß": "ss"}


def strip_legal_suffixes(name: str) -> str:
    name = LEGAL_SUFFIX_RE.sub("", name)
    return re.sub(r"[\s,.\-&/]+$", "", name).strip()


def normalize_to_slug(text: str, hyphenate: bool = True) -> str:
    for k, v in _UMLAUT.items():
        text = text.replace(k, v)
    text = text.lower()
    sep = "-" if hyphenate else ""
    text = re.sub(r"[^a-z0-9]+", sep, text)
    if hyphenate:
        text = re.sub(r"-+", "-", text).strip("-")
    return text


def generate_candidates(company_name: str, city: str | None = None) -> list[str]:
    stripped = strip_legal_suffixes(company_name)
    if not stripped:
        return []

    slug = normalize_to_slug(stripped, hyphenate=True)
    slug_flat = normalize_to_slug(stripped, hyphenate=False)
    words = [w for w in slug.split("-") if len(w) > 1]

    domains: list[str] = []

    def add(domain: str) -> None:
        if domain and len(domain) > 3:
            domains.append(f"https://{domain}")

    if slug:
        add(f"{slug}.de")
    if slug_flat and slug_flat != slug:
        add(f"{slug_flat}.de")

    if words:
        add(f"{words[0]}.de")

    if len(words) >= 2:
        two_flat = "".join(words[:2])
        two_hyph = "-".join(words[:2])
        add(f"{two_flat}.de")
        if two_hyph != slug:
            add(f"{two_hyph}.de")

    if city:
        city_slug = normalize_to_slug(city, hyphenate=False)
        if city_slug and slug:
            add(f"{slug}-{city_slug}.de")
            if slug_flat != slug:
                add(f"{slug_flat}{city_slug}.de")

    seen: set[str] = set()
    result: list[str] = []
    for u in domains:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result
