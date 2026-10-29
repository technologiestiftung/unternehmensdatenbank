"""Download every map object in one city that carries a website or an email, once.

The obvious way to use OpenStreetMap is to ask it about one company at a time. Measured against
the public Overpass service on 2026-08-12, that costs **11-15 seconds per company** and gets
HTTP 429 "too many requests" about half the time — 5 companies took 3.2 minutes. For 1000
companies that is over an hour of waiting for an answer the map mostly does not have.

This module does the opposite. **One** query pulls every named object inside a city's bounding
box that has a `website` or an `email` tag. For Osnabrück that is about 2000 objects, 900 KB,
and it came back in **6 seconds**. The result is written to disk, and every company is then
matched against it in memory. Matching 1000 companies costs no network at all.

Two objects can only be compared on what OSM actually holds, so read this before trusting a
match:

- Only objects **with a website or an email** are downloaded. An object with neither cannot
  contribute a URL, so fetching it would be pure cost.
- About **a quarter of them carry no address at all** — no postcode, no city. The scoring
  function (shared with the `osm_discovery` DAG, unchanged on purpose) gives at most 0.5 for a
  perfect name match, and the bar is 0.65, so those objects can never win. That is a limit of
  the map data, not a bug to work around by loosening the bar.

The bounding box is a setting, not a discovery: this pipeline is pointed at one city at a time.
`COMBINED_OSM_BBOX` must be changed before running it anywhere but Berlin.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

# `_to_record` is the one place that knows which OSM tags mean "website", "phone" and "email"
# (`website`, `contact:website`, `url`, `contact:url` — four spellings for one idea). Importing a
# private name is the lesser evil: the alternative is a second copy of that tag list, which would
# drift, and a snapshot match would then stop meaning what a DAG match means.
from enrichment.overpass_client import (
    OSM_OVERPASS_URL,
    OVERPASS_USER_AGENT,
    OverpassError,
    _to_record,
)
from enrichment.osm_scorer import score_osm_candidate

from enrichment import name_frequency

log = logging.getLogger(__name__)

#: South, west, north, east. Berlin plus a small margin. A margin is harmless: an object from a
#: neighbouring town still has to match on name AND address before it can win, and its postcode
#: will not agree.
COMBINED_OSM_BBOX = os.getenv("COMBINED_OSM_BBOX", "52.33,13.08,52.68,13.77")

#: How long a snapshot on disk is trusted. OSM changes slowly and a company's website tag changes
#: even more slowly, so a month is generous rather than risky.
SNAPSHOT_MAX_AGE_DAYS = float(os.getenv("COMBINED_OSM_SNAPSHOT_MAX_AGE_DAYS", "30"))

#: Overpass allows a per-query time budget. This one query is doing six bounding-box scans, so it
#: needs more than the per-company default of 30 s.
FETCH_TIMEOUT_S = float(os.getenv("COMBINED_OSM_SNAPSHOT_TIMEOUT_S", "300"))

#: Same bar as the `osm_discovery` DAG: 0.5 x name similarity + 0.3 x postcode + 0.2 x city.
MIN_MATCH_SCORE = float(os.getenv("COMBINED_OSM_MIN_MATCH_SCORE", "0.65"))

#: The name similarity a map object must reach BEFORE any address agreement is allowed to count,
#: measured on both names with the legal form removed. This is the guard that makes the whole
#: stage trustworthy, and it was added after a measurement, not on principle.
#:
#: The shared scorer is 0.5 x name + 0.3 x postcode + 0.2 x city. Inside a single town the
#: address half is nearly free — thousands of companies share a postcode — so a company only has
#: to reach 0.3 name similarity to clear the 0.65 bar. And "Hagenhoff Werbeagentur GmbH & Co. KG"
#: against "byteways GmbH & Co. KG" scores 0.59 on names, because eight of the characters they
#: share are the legal form. Run over all 14,443 companies with no floor, that produced **2,172
#: matches, of which the hand-checked sample was almost entirely wrong**: Hilling IT GmbH matched
#: MONOLITH Grill GmbH, Drehteam GmbH matched Bitnamic GmbH.
#:
#: With the legal form stripped those pairs score 0.20, 0.50 and 0.38, while the true match
#: PMT Technology GmbH -> PMT Technology rises from 0.85 to 1.00. So the floor separates them
#: cleanly, and it is applied to the STRIPPED names for exactly that reason.
MIN_STRIPPED_NAME_SIMILARITY = float(os.getenv("COMBINED_OSM_MIN_NAME_SIMILARITY", "70"))

#: How many name candidates the cheap fuzzy pass hands to the real scorer. More than one, because
#: the scorer also weighs the address and can reorder them.
PREFILTER_LIMIT = int(os.getenv("COMBINED_OSM_PREFILTER_LIMIT", "25"))

#: Strips the legal form off a company name. Same list as `overpass_client._LEGAL_SUFFIX_RE`,
#: which uses it to build the search pattern — the scorer never did, which is the bug above.
_LEGAL_SUFFIX_RE = re.compile(
    r"\b(?:GmbH\s*&\s*Co\.?\s*KG[aA]?|GmbH\s*&\s*Co\.?|GmbH|AG\s*&\s*Co\.?\s*KG"
    r"|KGaA|AG|KG|OHG|GbR|PartGmbB|PartG|UG\s*\(haftungsbeschränkt\)|UG"
    r"|e\.?\s*V\.?|e\.?\s*K\.?|SE|mbH|Ltd\.?|Inc\.?)\b",
    flags=re.IGNORECASE,
)


def strip_legal_form(name: str) -> str:
    """"Mergus Verlag GmbH" -> "Mergus Verlag". Nobody writes the legal form on a shop sign."""
    return re.sub(r"\s{2,}", " ", _LEGAL_SUFFIX_RE.sub(" ", name or "")).strip(" ,.-")


#: Deciding which words identify a business lives in `name_frequency`, because the URL side of
#: the pipeline needs exactly the same judgement. Re-exported here so callers that only deal with
#: OpenStreetMap have one import.
common_words = name_frequency.common_words
distinctive_words = name_frequency.distinctive_words

#: What a company with NO distinctive word left has to reach instead.
#:
#: "K & M Bauunternehmen GmbH & Co. KG" reduces to nothing: "K" and "M" are too short, "GmbH &
#: Co. KG" is the legal form, and "Bauunternehmen" is a trade that many companies here share.
#: Same for "GR Consulting e. K.". There is then no word whose sharing could prove identity, so
#: the shared-word test has nothing to test — and skipping it let both match a different company
#: in the same trade. Such a company may only match a name that is almost exactly its own.
NO_DISTINCTIVE_WORD_MIN_SIMILARITY = float(
    os.getenv("COMBINED_OSM_NO_DISTINCTIVE_MIN_SIMILARITY", "95")
)



#: The tags that make an object worth downloading. Four spellings of "website" and two of
#: "email", matching what `_to_record` reads.
_FETCH_TAGS = ("website", "contact:website", "url", "contact:url", "email", "contact:email")


def build_query(bbox: str) -> str:
    """One Overpass query: every named object in the box carrying any of the tags we can use.

    `nwr` means node, way and relation at once. `out tags` returns tags without geometry — we
    want the company's website, not its shape, and geometry would multiply the download size.
    """
    parts = "\n".join(f'  nwr["name"]["{tag}"]({bbox});' for tag in _FETCH_TAGS)
    return f"[out:json][timeout:{int(FETCH_TIMEOUT_S)}];\n(\n{parts}\n);\nout tags;\n"


@dataclass
class Snapshot:
    """Every usable map object for one city, plus the machinery to match a company against it."""

    records: list[dict[str, Any]]
    bbox: str
    fetched_at: str
    #: The map objects' names with the legal form removed, in the same order as `records`.
    #: Comparing stripped against stripped is what stops "GmbH & Co. KG" from being treated as
    #: evidence that two businesses are the same business.
    _stripped_names: list[str] = field(default_factory=list, repr=False)
    #: Words that too many companies share to identify one. Set by `set_generic_words`; empty
    #: means the check falls back to the hand-written lists alone.
    generic: set[str] = field(default_factory=set, repr=False)

    def __post_init__(self) -> None:
        self._stripped_names = [strip_legal_form(r.get("name") or "") for r in self.records]

    def set_generic_words(self, words: set[str]) -> None:
        """Teach the matcher which words are too common in THIS dataset to prove identity."""
        self.generic = words

    @property
    def with_website(self) -> int:
        return sum(1 for r in self.records if r.get("website"))

    def best_for(self, company: Any) -> tuple[dict[str, Any] | None, int]:
        """The map object most likely to BE this company. Returns (record or None, near misses).

        Two steps, and the order is the whole point.

        1. **The name has to survive on its own.** Both names, legal form removed, compared with
           `rapidfuzz`. Anything under `MIN_STRIPPED_NAME_SIMILARITY` is thrown out before the
           address is looked at, so a shared postcode can never rescue a name that does not
           match. See that constant for the measurement that forced this.
        2. **Then the shared scorer decides the number.** `osm_discovery.scorer` unchanged, so a
           match here means the same thing as a match found by the `osm_discovery` DAG.

        The second return value is how many objects survived step 1. It separates two blanks: a
        0 means nothing in this town is called anything like this company, while a 3 means
        something is and its address disagreed.
        """
        name = company.company_name or ""
        if not name or not self.records:
            return None, 0

        # Imported here, not at the top: the hub's Airflow parses the DAG files without our packages.
        from rapidfuzz import fuzz, process

        stripped = strip_legal_form(name) or name
        near = process.extract(
            stripped,
            self._stripped_names,
            scorer=fuzz.token_sort_ratio,
            limit=PREFILTER_LIMIT,
            score_cutoff=MIN_STRIPPED_NAME_SIMILARITY,
        )
        if not near:
            return None, 0

        # A shared word only counts if it could identify ONE business. Without this, "McFit
        # Osnabrück GmbH & Co. KG" matched "IG Metall Osnabrück" and "K & M Bauunternehmen"
        # matched "Bauunternehmen Fennker": similar names, same town, completely different firms.
        # Both share exactly one word, and it is the kind of word half the town shares.
        ours = distinctive_words(name, company.city, self.generic)
        if ours:
            near = [
                hit for hit in near
                if ours & distinctive_words(
                    self.records[hit[2]].get("name") or "", company.city, self.generic
                )
            ]
        else:
            # Nothing in this name identifies one business, so only a near-identical name will do.
            near = [hit for hit in near if hit[1] >= NO_DISTINCTIVE_WORD_MIN_SIMILARITY]
        if not near:
            return None, 0

        # The name similarity is recorded next to the score, because "0.71" on its own does not
        # tell you whether the name or the address carried the match.
        scored = sorted(
            (
                (self.records[index], score_osm_candidate(self.records[index], company), ratio)
                for _name, ratio, index in near
            ),
            key=lambda triple: triple[1],
            reverse=True,
        )
        best, best_score, best_ratio = scored[0]
        if best_score < MIN_MATCH_SCORE:
            return None, len(near)
        return (
            {
                **best,
                "match_score": round(best_score, 4),
                "name_similarity": round(best_ratio / 100.0, 4),
            },
            len(near),
        )


def _age_days(fetched_at: str) -> float:
    try:
        stamp = time.mktime(time.strptime(fetched_at, "%Y-%m-%dT%H:%M:%S"))
    except Exception:  # noqa: BLE001
        return float("inf")
    return (time.time() - stamp) / 86400.0


def fetch(bbox: str = COMBINED_OSM_BBOX) -> Snapshot:
    """Run the one big query. Raises `OverpassError` rather than returning an empty snapshot.

    An empty snapshot and a refused query look identical downstream — every company would come
    back unmatched and the run would look like "OSM knows nothing about this city".
    """
    query = build_query(bbox)
    log.info("Fetching OSM snapshot for bbox %s (one query, expect 5-30 s)", bbox)
    started = time.monotonic()
    try:
        resp = requests.post(
            OSM_OVERPASS_URL,
            data={"data": query},
            headers={"User-Agent": OVERPASS_USER_AGENT},
            timeout=FETCH_TIMEOUT_S + 30,
        )
        if resp.status_code in (429, 504):
            raise OverpassError(
                f"Overpass answered {resp.status_code} — it is busy. This is ONE query, so just "
                "run it again in a minute."
            )
        resp.raise_for_status()
        elements = resp.json().get("elements") or []
    except OverpassError:
        raise
    except requests.exceptions.RequestException as exc:
        raise OverpassError(f"OSM snapshot query failed: {exc}") from exc

    records = [_to_record(element) for element in elements]
    snapshot = Snapshot(
        records=records,
        bbox=bbox,
        fetched_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
    )
    log.info(
        "OSM snapshot: %d objects (%d with a website) in %.1f s",
        len(records), snapshot.with_website, time.monotonic() - started,
    )
    return snapshot


def load_or_fetch(path: Path, bbox: str = COMBINED_OSM_BBOX, *, refresh: bool = False) -> Snapshot:
    """The snapshot on disk, downloaded first if it is missing, stale or for another city."""
    if not refresh and path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            stored_bbox = raw.get("bbox")
            age = _age_days(raw.get("fetched_at") or "")
            if stored_bbox == bbox and age <= SNAPSHOT_MAX_AGE_DAYS:
                snapshot = Snapshot(
                    records=raw.get("records") or [],
                    bbox=stored_bbox,
                    fetched_at=raw.get("fetched_at") or "",
                )
                log.info(
                    "OSM snapshot from disk: %d objects (%d with a website), fetched %s",
                    len(snapshot.records), snapshot.with_website, snapshot.fetched_at,
                )
                return snapshot
            log.info(
                "OSM snapshot on disk is for bbox %s and %.0f days old — refetching",
                stored_bbox, age,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("OSM snapshot at %s unreadable (%s), refetching", path, exc)

    snapshot = fetch(bbox)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "bbox": snapshot.bbox,
                "fetched_at": snapshot.fetched_at,
                "records": snapshot.records,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    log.info("Wrote %s", path)
    return snapshot
