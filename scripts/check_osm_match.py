"""Guard the OpenStreetMap matcher against matching two different businesses.

Free to run: no database, no network, no API key. Run it after touching
`combined_discovery/osm_snapshot.py` or `osm_discovery/scorer.py`.

## What went wrong, and why this file exists

The shared scorer is `0.5 x name similarity + 0.3 x postcode agreement + 0.2 x city agreement`,
and the bar is 0.65. Every company in this dataset is in the same town, so the address half of
that sum is nearly free — which leaves a company needing only 0.3 of name similarity to clear the
bar. Two things then produced matches between unrelated firms:

1. **The legal form counted as name.** "Hagenhoff Werbeagentur GmbH & Co. KG" against "byteways
   GmbH & Co. KG" scores 0.59 on names, because most of what they share is "GmbH & Co. KG".
2. **A shared trade word counted as identity.** "K & M Bauunternehmen" against "Bauunternehmen
   Fennker", "Schuhhaus Schulte" against "Schuhhaus Sunderdiek", "McFit Osnabrück" against "IG
   Metall Osnabrück". Same town, same trade, different company.

Run over all 14,443 Osnabrück companies, that produced **2,172 matches (15%)** of which the
hand-checked sample was almost entirely wrong. With both holes closed it produces **292 (2%)**,
and the sample is almost entirely right. The fixtures below are those hand-checked pairs.

The cases are real Osnabrück businesses and real OSM entries. No personal data: company names,
map names and postcodes only.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from combined_discovery import name_frequency, osm_snapshot  # noqa: E402


@dataclass
class Case:
    company_name: str
    postcode: str | None
    osm_name: str
    osm_postcode: str | None
    want_match: bool
    why: str


#: The company names the "which words are too common to identify a business" measurement runs
#: over. In production that is all 14,443 rows of `public.companies`; here it is a small corpus
#: that contains the same kind of repetition, and `MIN_COMPANIES` below is scaled to it.
#:
#: The repeated words are the point: `bauunternehmen`, `schuhhaus`, `consulting`, `werbeagentur`
#: and `osnabruck` each appear in more than one company, which is what marks them as describing
#: a trade rather than naming a business.
COMPANY_CORPUS = [
    "K & M Bauunternehmen GmbH & Co. KG",
    "Shala Bauunternehmen UG",
    "Fennker Bauunternehmen GmbH",
    "Schuhhaus Schulte GmbH",
    "Schuhhaus Sunderdiek GmbH",
    "GR Consulting e. K.",
    "Vela Dare Consulting GmbH & Co. KG",
    "ECCO! Werbeagentur GmbH",
    "bwc Werbeagentur GmbH",
    "Hagenhoff Werbeagentur GmbH & Co. KG",
    "McFit Osnabrück GmbH & Co. KG",
    "Friedensgarten Osnabrück e. V.",
    "DRK Osnabrücker-Land GmbH",
    "Bürgerstiftung Osnabrück",
    "PMT Technology GmbH",
    "Modularity AG",
    "Zweirad Liebig Verwaltungs GmbH",
    "byteways GmbH & Co. KG",
    "Hubertus Wesseler GmbH",
    "Heinicke-electronic GmbH",
    "Fautz Catering GmbH & Co. KG",
    "Onat Market GmbH",
]

#: Two companies sharing a word is enough to call it common, because the corpus above has 22
#: names rather than 14,443. Production uses 8 out of 14,443.
MIN_COMPANIES = 2

CASES = [
    # ---- must match: the same business, written differently -----------------------------------
    Case("PMT Technology GmbH", "49084", "PMT Technology", "49084", True,
         "the legal form is the only difference"),
    Case("Modularity AG", "49078", "Modularity AG", "49078", True, "identical"),
    Case("Heinicke-electronic GmbH", "49080", "Heinicke electronic", "49080", True,
         "a hyphen is not a difference"),
    Case("Zweirad Liebig Verwaltungs GmbH", "49076", "Zweirad Liebig", "49076", True,
         "the administrative shell of the shop that IS mapped"),
    Case("Hubertus Wesseler GmbH", "49076", "hubertus wesseler GmbH & Co.KG", "49076", True,
         "different legal form, same business, lower case in the map"),
    Case("Onat Market GmbH", "49074", "Onat Market", "49074", True, "legal form only"),
    Case("Fautz Catering GmbH & Co. KG", "49080", "Fautz Catering", "49080", True,
         "legal form only"),

    # ---- must NOT match: different businesses that look similar -------------------------------
    Case("Hagenhoff Werbeagentur GmbH & Co. KG", "49080", "byteways GmbH & Co. KG", "49080",
         False, "shares only the legal form — the original bug"),
    Case("K & M Bauunternehmen GmbH & Co. KG", "49084", "Bauunternehmen Fennker", "49084", False,
         "shares only the trade"),
    Case("Shala Bauunternehmen UG", "49084", "Bauunternehmen Fennker", "49084", False,
         "shares only the trade"),
    Case("Schuhhaus Schulte GmbH", "49074", "Schuhhaus Sunderdiek", "49074", False,
         "shares only the kind of shop"),
    Case("McFit Osnabrück GmbH & Co. KG", "49074", "IG Metall Osnabrück", "49074", False,
         "shares only the town, which every company here shares"),
    Case("Friedensgarten Osnabrück e. V.", "49078", "Fitnesspark Osnabrück", "49078", False,
         "shares only the town"),
    Case("GR Consulting e. K.", "49074", "VELA DARE Consulting GmbH & Co. KG", "49074", False,
         "shares only the word consulting"),
    Case("ECCO! Werbeagentur GmbH", "49080", "bwc Werbeagentur GmbH", "49080", False,
         "shares only the trade"),
    Case("Bürgerstiftung Osnabrück", "49074", "Bistum Osnabrück", "49074", False,
         "shares only the town"),
    Case("Hilling IT GmbH", "49084", "MONOLITH Grill GmbH", "49084", False,
         "shares only the legal form"),
    Case("Drehteam GmbH", "49076", "Bitnamic GmbH", "49076", False,
         "shares only the legal form"),
]


class _Company:
    """The three attributes the scorer reads. Not the real CompanyCandidate, so this file needs
    no database import."""

    def __init__(self, name: str, postcode: str | None) -> None:
        self.company_name = name
        self.postcode = postcode
        self.city = "Osnabrück"


def main() -> int:
    generic = name_frequency.common_words(COMPANY_CORPUS, min_companies=MIN_COMPANIES)

    failures = 0
    for case in CASES:
        snapshot = osm_snapshot.Snapshot(
            records=[{
                "osm_id": "node/1",
                "name": case.osm_name,
                "postcode": case.osm_postcode or "",
                "city": "Osnabrück",
                "website": "https://example.com",
                "phone": None,
                "email": None,
                "osm_category": "",
            }],
            bbox="test",
            fetched_at="",
        )
        snapshot.set_generic_words(generic)
        record, _near = snapshot.best_for(_Company(case.company_name, case.postcode))
        matched = record is not None
        ok = matched == case.want_match
        failures += int(not ok)
        score = record.get("match_score") if record else "-"
        print(
            f"{'ok  ' if ok else 'FAIL'} match={str(matched):<5} want={str(case.want_match):<5} "
            f"score={str(score):<7} {case.company_name[:38]:<38} -> {case.osm_name[:30]:<30} "
            f"({case.why})"
        )

    print(f"\n{len(CASES) - failures}/{len(CASES)} as intended")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
