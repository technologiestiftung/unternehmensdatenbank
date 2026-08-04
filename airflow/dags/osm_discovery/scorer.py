from __future__ import annotations

import os
from typing import Any

from rapidfuzz import fuzz

OSM_MIN_MATCH_SCORE = float(os.getenv("OSM_MIN_MATCH_SCORE", "0.65"))


def score_osm_candidate(record: dict[str, Any], company: Any) -> float:
    name_score = fuzz.token_sort_ratio(record.get("name") or "", company.company_name or "") / 100.0
    postcode_score = (
        1.0 if company.postcode and record.get("postcode") == company.postcode else 0.0
    )
    city_score = (
        1.0
        if company.city and company.city.lower() in (record.get("city") or "").lower()
        else 0.0
    )
    return name_score * 0.50 + postcode_score * 0.30 + city_score * 0.20


def best_candidate(
    records: list[dict[str, Any]], company: Any
) -> tuple[dict[str, Any], float] | None:
    scored = [(r, score_osm_candidate(r, company)) for r in records]
    scored.sort(key=lambda x: x[1], reverse=True)
    if scored and scored[0][1] >= OSM_MIN_MATCH_SCORE:
        return scored[0]
    return None
