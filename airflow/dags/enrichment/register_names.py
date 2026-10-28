"""Company names as the register writes them: the matching key, and the legal form at the end.

Shared by the OffeneRegister import (`scripts/prepare_offeneregister_import.py`) and the DAGs that
create companies from a register source (`register_announcements`), so a company created either way
gets the same `normalized_name` and `legal_form`.
"""
from __future__ import annotations

import re

LEGAL_SUFFIX_RE = re.compile(
    r"\b(gmbh(?:\s*&\s*co\.\s*kg)?|ug|haftungsbeschränkt|ag|eg|kg|ohg|gbr|se|kgaa|mbh|e\.v\.)\b",
    re.IGNORECASE,
)


def normalize_company_name(value: str) -> str:
    """The matching key for a name: lower case, no legal form, no punctuation, single spaces."""
    value = value.lower().replace("&", " und ")
    value = re.sub(r"[^0-9a-zäöüß]+", " ", value, flags=re.IGNORECASE)
    value = LEGAL_SUFFIX_RE.sub(" ", value)
    return re.sub(r"\s+", " ", value).strip()


# Longest first: "GmbH & Co. KG" must win over "KG", "UG (haftungsbeschränkt)" over "UG".
LEGAL_FORMS = [
    ("GmbH & Co. KG", r"gmbh\s*&\s*co\.?\s*kg"),
    ("UG & Co. KG", r"ug\s*\(haftungsbeschränkt\)\s*&\s*co\.?\s*kg"),
    # legal_form() strips a trailing ")", so the closing parenthesis is optional here.
    ("UG", r"\(haftungsbeschränkt\)?|\bug"),
    ("gGmbH", r"ggmbh"),
    ("GmbH", r"gmbh|g\.\s*m\.\s*b\.\s*h|\bm\.?\s*b\.?\s*h|mit beschränkter haftung"),
    ("PartG mbB", r"\bmbb"),
    ("KGaA", r"kgaa"),
    ("AG", r"\bag|aktiengesellschaft"),
    ("SE", r"\bse"),
    ("Ltd.", r"\bltd|\blimited"),
    ("e. V.", r"\be\.\s*v"),
    ("eG", r"\beg"),
    ("e. K.", r"\be\.\s*k(fm|ffr|fr)?"),
    ("OHG", r"ohg"),
    ("KG", r"\bk\.?\s*g|kommanditgesellschaft"),
    ("PartG", r"partg|partnerschaft"),
]
_LEGAL_FORM_RES = [(form, re.compile(rf"(?:{pattern})$", re.IGNORECASE)) for form, pattern in LEGAL_FORMS]
# Words that often follow the legal form: "Beispiel GmbH Berlin", "… GmbH Steuerberatungsgesellschaft".
_TRAILING_WORDS = re.compile(
    r"(?:[\s,]+(?:berlin|deutschland|in liquidation|i\.\s*l|-|steuerberatungsgesellschaft"
    r"|wirtschaftsprüfungsgesellschaft|rechtsanwaltsgesellschaft|rechtsanwälte)[.)]*)+$",
    re.IGNORECASE,
)


def legal_form(name: str) -> str | None:
    """The legal form from the END of the name; the dump has no field for it."""
    core = _TRAILING_WORDS.sub("", name.strip()).rstrip(" .)")
    for form, pattern in _LEGAL_FORM_RES:
        if pattern.search(core):
            return form
    return None
