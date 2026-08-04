from __future__ import annotations

import re
from typing import Any

AUTO_ACCEPT_SCORE = 100
MIN_STORE_SCORE = -50


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
