"""Look a company up in OpenStreetMap and see whether the map knows its website.

**OpenStreetMap (OSM)** is a free map of the world that anybody can edit, like Wikipedia for
places. Shops, offices and workshops are entered as points with tags: `name`, `addr:postcode`,
and often `website`. If somebody has mapped the company, the map may already hold the answer.

**Overpass** is the query service for OSM. The query this asks is: "inside the area with this
postcode, find anything whose name looks like this company". The reply is a list of map
objects; `osm_discovery.scorer` picks the best one by comparing name, postcode and city.

Both the query and the scoring are reused verbatim from the `osm_discovery` DAG, so a match
found here and a match found by that DAG mean the same thing. What this module adds:

1. **Backoff.** The public Overpass service allows roughly two queries at a time and answers
   429 when it has had enough. The DAG stops the whole run on a 429; a measurement run over
   1000 companies has to wait and carry on instead.
2. **A cache on disk.** The same company is never asked twice, so re-running is free.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from osm_discovery.overpass_client import (
    OverpassError,
    OverpassRateLimitError,
    search_company,
)
from osm_discovery.scorer import score_osm_candidate

log = logging.getLogger(__name__)

#: How sure the name+address match must be before the map entry is believed at all. The
#: `osm_discovery` DAG defaults to 0.65 and this follows it. The scale: 0.5 x name similarity
#: + 0.3 x postcode agreement + 0.2 x city agreement.
MIN_MATCH_SCORE = float(os.getenv("COMBINED_OSM_MIN_MATCH_SCORE", "0.65"))

#: Above this, the match is treated as strong and earns a confidence bonus. 0.80 is only
#: reachable with a good name similarity AND a postcode agreement.
STRONG_MATCH_SCORE = float(os.getenv("COMBINED_OSM_STRONG_MATCH_SCORE", "0.80"))

#: Waits after a rate limit, in seconds. Four tries, then the company is given up on. Bounded
#: on purpose: `search_company` reports a gateway timeout through the same exception as a rate
#: limit, and a query that times out will time out again however long you wait.
BACKOFF_S = (5.0, 15.0, 30.0, 60.0)

#: Ask Overpass for the CITY area, never the postcode area, even when the postcode is known.
#:
#: `area["postal_code"="49074"]` makes the public server search its whole postcode-boundary
#: index and it answered **504 Gateway Timeout**, while the same query against
#: `area["name"="Osnabrück"]["admin_level"~"^[6-9]$"]` came back in about a second. Since
#: `search_company` prefers the postcode whenever it has one, and every company in this dataset
#: has one, the postcode path is the only one the DAG ever takes — and it times out.
#:
#: Nothing is lost by this. The postcode is still used, by `score_osm_candidate`, which
#: compares the postcode ON the map object with the company's. Narrowing the *search* by
#: postcode was never what made the match trustworthy.
QUERY_BY_CITY = os.getenv("COMBINED_OSM_QUERY_BY_CITY", "true").lower() == "true"

#: Only one Overpass query at a time, process-wide. The public service is a shared resource
#: and answers 429 quickly when several threads hit it. The pipeline runs many companies in
#: parallel, so without this lock the OSM stage would spend its whole time being rate-limited.
_QUERY_LOCK = threading.Lock()


@dataclass
class OsmMatch:
    osm_id: str
    name: str
    match_score: float
    website: str | None
    phone: str | None
    email: str | None
    postcode: str | None
    city: str | None
    category: str | None

    @property
    def strong(self) -> bool:
        return self.match_score >= STRONG_MATCH_SCORE


class OsmCache:
    """Company id -> raw Overpass reply, kept in one JSON file.

    Overpass is slow and rate-limited, so asking it twice for the same company is the single
    most wasteful thing this pipeline could do.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._data: dict[str, list[dict[str, Any]]] = {}
        if path.exists():
            try:
                self._data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                log.warning("OSM cache at %s unreadable, starting fresh", path)

    def get(self, company_id: str) -> list[dict[str, Any]] | None:
        with self._lock:
            return self._data.get(company_id)

    def put(self, company_id: str, records: list[dict[str, Any]]) -> None:
        with self._lock:
            self._data[company_id] = records
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(self._data, ensure_ascii=False), encoding="utf-8")


def _query_with_backoff(company: Any) -> list[dict[str, Any]]:
    """One Overpass query, retried through rate limits. Raises only when it truly gave up."""
    postcode = None if QUERY_BY_CITY and company.city else (company.postcode or None)
    for attempt, wait_s in enumerate((0.0, *BACKOFF_S)):
        if wait_s:
            log.info("Overpass rate-limited, waiting %.0fs (attempt %d)", wait_s, attempt)
            time.sleep(wait_s)
        try:
            with _QUERY_LOCK:
                return search_company(
                    company_name=company.company_name,
                    postcode=postcode,
                    city=company.city or None,
                )
        except OverpassRateLimitError:
            continue
    raise OverpassRateLimitError("Overpass still rate-limiting after every backoff step")


def lookup_in_snapshot(company: Any, snapshot: Any) -> tuple[OsmMatch | None, int]:
    """Find this company in a city-wide snapshot instead of asking Overpass about it.

    Same scoring, same bar, no network. See `osm_snapshot.py` for why this exists: asking the
    public service per company costs 11-15 s and gets rate-limited, and one query for the whole
    city costs 6 s in total.

    The second return value is the number of objects whose NAME was close enough to be worth
    scoring — not the number of objects in the city. A 0 there means nothing in Osnabrück is
    called anything like this company; a 3 means something is, and its address disagreed.
    """
    record, near_misses = snapshot.best_for(company)
    if not record:
        return None, near_misses
    return (
        OsmMatch(
            osm_id=record.get("osm_id") or "",
            name=record.get("name") or "",
            match_score=record.get("match_score") or 0.0,
            website=record.get("website"),
            phone=record.get("phone"),
            email=record.get("email"),
            postcode=record.get("postcode") or None,
            city=record.get("city") or None,
            category=record.get("osm_category") or None,
        ),
        near_misses,
    )


def lookup(company: Any, cache: OsmCache | None = None) -> tuple[OsmMatch | None, int]:
    """Find this company on the map. Returns (best match or None, number of map objects seen).

    A returned match does not necessarily carry a website — plenty of mapped businesses have
    no `website` tag. The match itself is still worth having, because it confirms the company
    exists at that address, and the pipeline records the `osm_id`.

    Raises `OverpassError` when the service refused the query. Deliberately not swallowed: an
    earlier version returned an empty list on failure, which reads identically to "the map has
    nothing for this company" — and that is exactly how a 406 on every single request stayed
    invisible.
    """
    cached = cache.get(company.id) if cache else None
    if cached is None:
        records = _query_with_backoff(company)
        if cache:
            cache.put(company.id, records)
    else:
        records = cached

    if not records:
        return None, 0

    scored = sorted(
        ((r, score_osm_candidate(r, company)) for r in records),
        key=lambda pair: pair[1],
        reverse=True,
    )
    best, best_score = scored[0]
    if best_score < MIN_MATCH_SCORE:
        return None, len(records)

    return (
        OsmMatch(
            osm_id=best.get("osm_id") or "",
            name=best.get("name") or "",
            match_score=round(best_score, 4),
            website=best.get("website"),
            phone=best.get("phone"),
            email=best.get("email"),
            postcode=best.get("postcode") or None,
            city=best.get("city") or None,
            category=best.get("osm_category") or None,
        ),
        len(records),
    )
