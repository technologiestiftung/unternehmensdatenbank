from __future__ import annotations

import re
from typing import Any

AUTO_ACCEPT_SCORE = 100
MIN_STORE_SCORE = -50

_LEGAL_SUFFIX_RE = re.compile(
    r"\s*(?:GmbH(?:\s*&\s*Co\.?\s*KG)?|AG|e\.?\s*V\.?|KG|OHG|UG\s*\(haftungsbeschränkt\)|UG|GbR"
    r"|SE|KGaA|Ltd\.?|Inc\.?|Corp\.?|S\.?A\.?S\.?|SARL|s\.r\.o\.?)\s*$",
    re.IGNORECASE,
)

_GENERIC_SLUGS = frozenset({
    "bau", "handel", "service", "services", "beratung", "consulting",
    "management", "solutions", "solution", "group", "holding", "invest",
    "immobilien", "verwaltung", "marketing", "media", "technik", "digital",
    "import", "export", "transport", "logistik", "gesundheit", "pflege",
    "gesellschaft", "berlin", "haus", "projekt", "techno", "neue",
})


def name_uniqueness(company_name: str) -> float:
    """
    Return 0.0 (very generic) → 1.0 (highly unique) for a company name.

    Used to scale URL-match confidence when imprint verification is absent or
    partial: unique names (long, multi-word, non-dictionary) deserve higher
    confidence from a pattern match than a single generic word would.
    """
    cleaned = _LEGAL_SUFFIX_RE.sub("", company_name).strip()
    words = [w for w in re.split(r"[\s\-_/&+.]+", cleaned) if len(w) >= 3]
    if not words:
        return 0.05

    slug = "".join(words).lower()
    for old, new in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")):
        slug = slug.replace(old, new)

    slug_len = len(slug)
    n_words = len(words)

    # Length: ≤4 chars → 0.0, ≥14 chars → 1.0
    length_score = max(0.0, min(1.0, (slug_len - 4) / 10.0))
    # Multi-word: 1 word → 0.0, 2 words → 0.5, 3+ words → 1.0
    word_score = min(1.0, (n_words - 1) / 2.0)

    combined = length_score * 0.55 + word_score * 0.45

    # Severe penalty when ALL significant words are common dictionary terms
    slug_lower_words = {w.lower() for w in words}
    if slug_lower_words.issubset(_GENERIC_SLUGS):
        combined *= 0.4

    return max(0.05, min(1.0, combined))


def _reg_digits(reg: str) -> str:
    m = re.search(r"\d+", reg or "")
    return m.group(0) if m else ""


def score_candidate(extracted: dict[str, Any], company: Any) -> int:
    score = 0

    ext_reg = extracted.get("register_number") or ""
    comp_reg = company.registration_number or ""

    if ext_reg and comp_reg:
        ext_d = _reg_digits(ext_reg)
        comp_d = _reg_digits(comp_reg)
        if ext_d and comp_d:
            if ext_d == comp_d:
                score += 100
            else:
                score -= 200

    if extracted.get("postcode") and company.postcode:
        if extracted["postcode"] == company.postcode:
            score += 20

    return score
