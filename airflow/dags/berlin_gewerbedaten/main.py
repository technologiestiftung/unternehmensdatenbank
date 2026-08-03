"""
Berlin Gewerbedaten pipeline for Airflow.

This module fetches Berlin Gewerbedaten from the public WFS endpoint,
enriches records with official addresses and OSM name candidates, writes a
JSON export to persistent storage, and then loads the result into the local
Postgres schema used by this repository.
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterable

import numpy as np
import requests
from pyproj import Transformer
from rapidfuzz import fuzz
from scipy.spatial import KDTree

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
WFS_BASE_URL = os.getenv(
    "WFS_BASE_URL",
    "https://gdi.berlin.de/services/wfs/gewerbedaten",
)
WFS_PARAMS: dict[str, str] = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "typeNames": "gewerbedaten:gewerbedaten",
    "outputFormat": "application/json",
    "srsName": "EPSG:25833",
    "count": str(int(os.getenv("WFS_PAGE_SIZE", "2000"))),
}
# Offset into the full WFS dataset — set to skip records already loaded
WFS_START_INDEX = int(os.getenv("WFS_START_INDEX", "0"))

PRIMARY_OVERPASS_URL = os.getenv(
    "OVERPASS_URL", "https://overpass-api.de/api/interpreter"
)
ALLOW_PUBLIC_OVERPASS_FALLBACK = (
    os.getenv("ALLOW_PUBLIC_OVERPASS_FALLBACK", "false").lower() == "true"
)
OVERPASS_URLS = [PRIMARY_OVERPASS_URL]
if ALLOW_PUBLIC_OVERPASS_FALLBACK:
    OVERPASS_URLS.extend(
        [
            "https://overpass.kumi.systems/api/interpreter",
            "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
        ]
    )
# Radius in metres to search for nearby OSM POIs
OVERPASS_RADIUS = int(os.getenv("OVERPASS_RADIUS", "80"))
# Delay between Overpass calls (they ask for reasonable use)
OVERPASS_DELAY_S = float(os.getenv("OVERPASS_DELAY_S", "1.5"))
# Number of parallel threads for Overpass queries (local instance is safe to increase)
OVERPASS_WORKERS = int(os.getenv("OVERPASS_WORKERS", "6"))
# Minimum rapidfuzz token_set_ratio score to accept a POI name (0–100)
NAME_MATCH_MIN_SCORE = int(os.getenv("NAME_MATCH_MIN_SCORE", "35"))
SKIP_NAME_LOOKUP = os.getenv("SKIP_NAME_LOOKUP", "false").lower() == "true"

# Nominatim — fallback address lookup if Adressen index is unavailable
NOMINATIM_URL = os.getenv(
    "NOMINATIM_URL", "https://nominatim.openstreetmap.org/reverse"
)
NOMINATIM_DELAY_S = float(os.getenv("NOMINATIM_DELAY_S", "2.0"))
NOMINATIM_RETRIES = int(os.getenv("NOMINATIM_RETRIES", "3"))

# Berlin Adressen WFS — official address register, all ~500k Berlin addresses
ADRESSEN_WFS_URL = os.getenv(
    "ADRESSEN_WFS_URL",
    "https://gdi.berlin.de/services/wfs/adressen_berlin",
)
# Max distance in metres to accept an address match from the KDTree
ADRESSEN_MAX_DIST_M = float(os.getenv("ADRESSEN_MAX_DIST_M", "60"))
# Local cache file — downloaded once, reused on subsequent runs
ADRESSEN_CACHE_FILE = os.getenv(
    "ADRESSEN_CACHE_FILE", str(DATA_DIR / "adressen_cache.json")
)

# Set to "true" to skip all geo-enrichment
SKIP_GEOCODE = os.getenv("SKIP_GEOCODE", "false").lower() == "true"
SKIP_ADDRESS_LOOKUP = (
    SKIP_GEOCODE or os.getenv("SKIP_ADDRESS_LOOKUP", "false").lower() == "true"
)

OUTPUT_FILE = os.getenv("OUTPUT_FILE", str(DATA_DIR / "gewerbedaten.jsonl"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", str(DATA_DIR / "gewerbedaten_checkpoint.json"))
# Set to "true" to ignore any existing checkpoint and restart from scratch
RESET_CHECKPOINT = os.getenv("RESET_CHECKPOINT", "false").lower() == "true"

# Set to 0 to fetch everything
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "0"))
DB_COMMIT_BATCH_SIZE = int(os.getenv("DB_COMMIT_BATCH_SIZE", "250"))

PG_DSN = os.getenv(
    "PG_DSN",
    "host=postgres port=5432 dbname=unternehmensdatenbank user=app password=app",
)
SOURCE_KEY = os.getenv("GEWERBEDATEN_SOURCE_KEY", "berlin_gewerbedaten")
SOURCE_NAME = os.getenv("GEWERBEDATEN_SOURCE_NAME", "Berlin Gewerbedaten WFS")
SOURCE_TYPE = os.getenv("GEWERBEDATEN_SOURCE_TYPE", "open_data_wfs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def _ensure_parent_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Coordinate transformer  EPSG:25833 → WGS84
# ---------------------------------------------------------------------------
_transformer = Transformer.from_crs("EPSG:25833", "EPSG:4326", always_xy=True)
# Degrees → metres approximation for Berlin (~52°N)
_DEG_TO_M_LAT = 111_320.0
_DEG_TO_M_LON = 111_320.0 * 0.6088  # cos(52°)


def epsg25833_to_wgs84(x: float, y: float) -> tuple[float, float]:
    """Return (lon, lat) in WGS84."""
    lon, lat = _transformer.transform(x, y)
    return lon, lat


# ---------------------------------------------------------------------------
# Berlin Adressen WFS — KDTree spatial index
# ---------------------------------------------------------------------------

@dataclass
class _AddrRecord:
    street: str
    hnr: str
    plz: str
    city: str
    lat: float
    lon: float

    @property
    def formatted(self) -> str:
        return f"{self.street} {self.hnr}, {self.plz} {self.city}".strip(", ")


class AddressIndex:
    """
    Downloads all ~500k Berlin address points from the official Adressen WFS,
    builds a KDTree for O(log n) nearest-neighbour lookups, and caches the
    result locally so subsequent runs are instant.

    Usage:
        idx = AddressIndex.load(session)
        address = idx.nearest(lat=52.52, lon=13.40, max_dist_m=60)
    """

    def __init__(self, records: list[_AddrRecord]) -> None:
        self._records = records
        # Build KDTree on (lat_scaled, lon_scaled) so Euclidean distance ≈ metres
        coords = np.array(
            [(r.lat * _DEG_TO_M_LAT, r.lon * _DEG_TO_M_LON) for r in records],
            dtype=np.float64,
        )
        self._tree = KDTree(coords)
        log.info("AddressIndex: KDTree built with %d records.", len(records))

    def nearest(self, lat: float, lon: float, max_dist_m: float = ADRESSEN_MAX_DIST_M) -> str | None:
        """Return the formatted address of the nearest point within max_dist_m metres."""
        q = np.array([lat * _DEG_TO_M_LAT, lon * _DEG_TO_M_LON])
        dist, idx = self._tree.query(q, k=1)
        if dist <= max_dist_m:
            return self._records[idx].formatted
        return None

    # ------------------------------------------------------------------
    @classmethod
    def load(cls, session: requests.Session) -> "AddressIndex":
        """
        Return an AddressIndex, using a local JSON cache when available.
        On first run (or if the cache is missing) downloads from the WFS.
        """
        if os.path.exists(ADRESSEN_CACHE_FILE):
            log.info("Loading Adressen index from cache: %s", ADRESSEN_CACHE_FILE)
            return cls._from_cache(ADRESSEN_CACHE_FILE)

        log.info("Downloading Berlin Adressen WFS — this runs once and is cached …")
        records = cls._download(session)
        cls._save_cache(records, ADRESSEN_CACHE_FILE)
        return cls(records)

    @classmethod
    def _download(cls, session: requests.Session) -> list[_AddrRecord]:
        records: list[_AddrRecord] = []
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": "adressen_berlin",
            "outputFormat": "application/json",
            "srsName": "EPSG:4326",  # already WGS84 — no conversion needed
            "count": "2000",
        }
        start_index = 0

        while True:
            params["startIndex"] = str(start_index)
            log.info("  Adressen WFS startIndex=%d (total so far: %d) …", start_index, len(records))
            resp = session.get(ADRESSEN_WFS_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", [])
            if not features:
                break

            for f in features:
                p = f.get("properties", {})
                geom = f.get("geometry", {})
                coords = geom.get("coordinates", [None, None])
                if coords[0] is None:
                    continue
                hnr = str(p.get("hnr") or "")
                if p.get("hnr_zusatz"):
                    hnr += str(p["hnr_zusatz"])
                records.append(_AddrRecord(
                    street=p.get("str_name") or "",
                    hnr=hnr,
                    plz=p.get("plz") or "",
                    city=p.get("bez_name") or "Berlin",
                    lat=coords[1],
                    lon=coords[0],
                ))

            start_index += len(features)
            if len(features) < int(params["count"]):
                break

        log.info("Adressen WFS download complete: %d addresses.", len(records))
        return records

    @staticmethod
    def _save_cache(records: list[_AddrRecord], path: str) -> None:
        _ensure_parent_dir(path)
        data = [
            {"street": r.street, "hnr": r.hnr, "plz": r.plz,
             "city": r.city, "lat": r.lat, "lon": r.lon}
            for r in records
        ]
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        log.info("Adressen cache saved → %s (%d records)", path, len(data))

    @classmethod
    def _from_cache(cls, path: str) -> "AddressIndex":
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        records = [
            _AddrRecord(
                street=d["street"], hnr=d["hnr"], plz=d["plz"],
                city=d["city"], lat=d["lat"], lon=d["lon"],
            )
            for d in data
        ]
        return cls(records)


class JsonLinesWriter:
    """Write records as JSON Lines.

    In normal mode: writes to a .tmp file and atomically renames on clean exit.
    In append mode: writes directly to the final file (used when resuming from
    a checkpoint — the existing content is already valid).
    """

    def __init__(self, path: str, append: bool = False) -> None:
        self.path = path
        self._tmp_path = path + ".tmp"
        self._append = append
        self._fh: Any | None = None
        self.count = 0

    def __enter__(self) -> "JsonLinesWriter":
        _ensure_parent_dir(self.path)
        if self._append:
            self._fh = open(self.path, "a", encoding="utf-8")
        else:
            self._fh = open(self._tmp_path, "w", encoding="utf-8")
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        if not self._append:
            if exc_type is None:
                import os
                os.replace(self._tmp_path, self.path)
            else:
                log.warning("JsonLinesWriter exiting with error — keeping %s, not replacing %s", self._tmp_path, self.path)

    def write(self, record: dict[str, Any]) -> None:
        assert self._fh is not None
        self._fh.write(json.dumps(record, ensure_ascii=False))
        self._fh.write("\n")
        self.count += 1


def iter_output_records(path: str) -> Iterable[dict[str, Any]]:
    with open(path, encoding="utf-8") as fh:
        first_non_ws = ""
        while True:
            char = fh.read(1)
            if not char:
                return
            if not char.isspace():
                first_non_ws = char
                break

        fh.seek(0)

        if first_non_ws == "[":
            for record in json.load(fh):
                yield record
            return

        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                log.warning("Skipping malformed JSONL line %d (%s): %.120r", lineno, exc, line)


# ---------------------------------------------------------------------------
# WFS pagination
# ---------------------------------------------------------------------------
def iter_features(session: requests.Session, start_index: int = 0) -> Generator[dict, None, None]:
    """Yield all features from the WFS endpoint, page by page."""
    params = dict(WFS_PARAMS)
    total: int | None = None
    yielded = 0

    while True:
        params["startIndex"] = str(start_index)
        log.info("Fetching WFS features startIndex=%d …", start_index)
        resp = session.get(WFS_BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if total is None:
            total = data.get("numberMatched", "?")
            log.info("Total features reported by server: %s", total)

        features: list[dict] = data.get("features", [])
        if not features:
            break

        for feature in features:
            yield feature
            yielded += 1
            if MAX_RECORDS and yielded >= MAX_RECORDS:
                log.info("Reached MAX_RECORDS limit (%d). Stopping.", MAX_RECORDS)
                return

        start_index += len(features)

        # Stop if we received fewer records than the page size
        if len(features) < int(WFS_PARAMS["count"]):
            break


# ---------------------------------------------------------------------------
# OSM type → human-readable German description  (for scoring against IHK/NACE)
# ---------------------------------------------------------------------------
_OSM_TYPE_DESC: dict[str, str] = {
    "supermarket":      "Lebensmitteleinzelhandel Supermarkt",
    "convenience":      "Einzelhandel Lebensmittel",
    "beverages":        "Einzelhandel mit Getränken",
    "alcohol":          "Einzelhandel mit Getränken Alkohol",
    "bakery":           "Bäckerei Konditorei Backwaren",
    "butcher":          "Fleischerei Metzgerei",
    "deli":             "Feinkost Einzelhandel",
    "greengrocer":      "Obst Gemüse Einzelhandel",
    "clothes":          "Bekleidung Einzelhandel Textilien",
    "shoes":            "Schuhhandel Einzelhandel",
    "electronics":      "Elektronik Einzelhandel",
    "hardware":         "Bau Haushaltswaren Einzelhandel",
    "furniture":        "Möbel Einzelhandel",
    "florist":          "Blumen Einzelhandel",
    "hairdresser":      "Friseur Friseurhandwerk",
    "beauty":           "Kosmetik Schönheitspflege",
    "optician":         "Optiker Augenoptik",
    "jewellery":        "Schmuck Einzelhandel",
    "books":            "Bücher Einzelhandel",
    "stationery":       "Schreibwaren Bürobedarf Einzelhandel",
    "newsagent":        "Zeitschriften Zeitungen Schreibwaren",
    "kiosk":            "Kiosk Zeitschriften Einzelhandel",
    "laundry":          "Wäscherei Reinigung",
    "dry_cleaning":     "Chemische Reinigung",
    "car_repair":       "Kraftfahrzeugreparatur Kfz",
    "car":              "Kraftfahrzeughandel Kfz",
    "bicycle":          "Fahrrad Einzelhandel Reparatur",
    "restaurant":       "Restaurant Gaststätte Gastronomie",
    "cafe":             "Café Kaffee Gastronomie",
    "fast_food":        "Imbiss Schnellimbiss Gastronomie",
    "bar":              "Bar Gastronomie",
    "pub":              "Kneipe Gastronomie",
    "ice_cream":        "Eissalon Gastronomie",
    "pharmacy":         "Apotheke",
    "doctors":          "Arzt Medizin Gesundheit",
    "dentist":          "Zahnarzt",
    "veterinary":       "Tierarzt",
    "bank":             "Bank Kreditinstitut",
    "insurance":        "Versicherung",
    "estate_agent":     "Immobilien Grundstücksverwaltung",
    "lawyer":           "Rechtsanwalt",
    "accountant":       "Steuerberater Buchhaltung",
    "architect":        "Architekt",
    "advertising":      "Werbung Marketing",
    "it":               "Informationstechnologie EDV",
    "travel_agent":     "Reisebüro Tourismus",
    "post":             "Post Kurier",
    "gym":              "Fitnessstudio Sport",
    "school":           "Schule Bildung",
    "kindergarten":     "Kita Kindergarten",
    "hotel":            "Hotel Beherbergung",
    "hostel":           "Hostel Beherbergung",
    "workshop":         "Werkstatt Handwerk",
    "craft":            "Handwerk",
    "tailor":           "Schneiderei Bekleidungshandwerk",
    "painter":          "Malerei Malerhandwerk",
    "electrician":      "Elektroinstallation",
    "plumber":          "Sanitär Klempner",
}


# ---------------------------------------------------------------------------
# Overpass API — fetch named POIs near a coordinate
# ---------------------------------------------------------------------------
_overpass_cache: dict[tuple[float, float], list[dict]] = {}
_overpass_cache_lock = threading.Lock()
_overpass_thread_local = threading.local()


def _get_overpass_session() -> requests.Session:
    """Return a per-thread requests.Session for Overpass queries."""
    if not hasattr(_overpass_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"Accept": "application/json"})
        _overpass_thread_local.session = s
    return _overpass_thread_local.session


_BUSINESS_TAG_FILTER = '[~"^(shop|amenity|office|craft|tourism)$"~"."]'

# German keywords drawn from IHK/NACE descriptions that indicate a business
# likely to have an OSM shop/amenity/office/craft entry.  Records whose IHK
# branch description contains none of these are skipped — querying Overpass
# for consulting firms, marketing agencies, etc. wastes ~70% of calls.
_OSM_LIKELY_KEYWORDS: frozenset[str] = frozenset([
    "supermarkt", "lebensmittel", "bäckerei", "konditorei", "metzgerei",
    "fleischerei", "getränk", "obst", "gemüse", "feinkost",
    "restaurant", "gaststätte", "imbiss", "café", "kiosk", "bar", "kneipe",
    "eissalon", "catering", "event-cater",
    "friseur", "kosmetik", "schönheit", "optiker", "juwelier", "schmuck",
    "apotheke", "arzt", "zahnarzt", "tierarzt", "gesundheit",
    "bank", "sparkasse", "versicherung", "immobilien", "makler",
    "rechtsanwalt", "notar", "steuerberater", "buchführung",
    "hotel", "hostel", "pension", "beherbergung",
    "fitnessstudio", "sport", "schwimm", "yoga", "kampfsport",
    "schule", "bildung", "kita", "kindergarten", "nachhilfe",
    "einzelhandel", "großhandel", "fachhandel",
    "werkstatt", "kfz", "kraftfahrzeug", "fahrrad",
    "reisebüro", "tourismus",
    "wäscherei", "reinigung", "chemische reinigung",
    "schneiderei", "textil", "bekleidung", "schuh",
    "maler", "elektroinstallation", "sanitär", "klempner",
    "blumen", "florist", "möbel", "elektronik", "bücher", "schreibwaren",
    "tankstelle", "parkplatz", "post", "kurier",
])


def _ihk_likely_has_osm_presence(record: dict) -> bool:
    """Return True if the IHK/NACE description suggests a physical business that
    is likely to appear in OSM (retail, hospitality, healthcare, etc.)."""
    for field in ("ihk_branch_desc", "nace_desc", "sector"):
        val = (record.get(field) or "").lower()
        if any(kw in val for kw in _OSM_LIKELY_KEYWORDS):
            return True
    return False


def _overpass_query(lat: float, lon: float) -> str:
    # Only fetch elements that carry a business-type tag (shop, amenity, office,
    # craft, tourism).  Plain [name] returns streets and parks which have no
    # matching osm_type and always score 0 against IHK descriptions.
    r, bf = OVERPASS_RADIUS, _BUSINESS_TAG_FILTER
    return (
        f"[out:json][timeout:15];"
        f"(node(around:{r},{lat},{lon})[name]{bf};"
        f"way(around:{r},{lat},{lon})[name]{bf};);"
        f"out body;"
    )


def fetch_nearby_pois(lat: float, lon: float) -> list[dict]:
    """Return list of {{name, osm_type, tags}} for named OSM elements near (lat, lon).

    Thread-safe: uses a per-thread requests.Session and a lock-protected cache.
    """
    key = (round(lat, 4), round(lon, 4))
    with _overpass_cache_lock:
        if key in _overpass_cache:
            return _overpass_cache[key]

    time.sleep(OVERPASS_DELAY_S)
    query = _overpass_query(lat, lon)
    result: list[dict] = []
    session = _get_overpass_session()

    for url in OVERPASS_URLS:
        try:
            resp = session.post(
                url,
                data={"data": query},
                headers={"User-Agent": "berlin-gewerbedaten-pipeline/1.0"},
                timeout=25,
            )
            if not resp.ok:
                log.debug("Overpass %s returned %s — trying next mirror", url, resp.status_code)
                continue
            elements = resp.json().get("elements", [])
            for e in elements:
                tags = e.get("tags", {})
                name = tags.get("name")
                if not name:
                    continue
                osm_type = (
                    tags.get("shop")
                    or tags.get("amenity")
                    or tags.get("office")
                    or tags.get("craft")
                    or tags.get("tourism")
                    or ""
                )
                if not osm_type:
                    continue  # street / park / building — no business type, always scores 0
                result.append({"name": name, "osm_type": osm_type, "tags": tags})
            break
        except Exception as exc:  # noqa: BLE001
            log.debug("Overpass error (%s): %s", url, exc)
            continue

    if not result and not ALLOW_PUBLIC_OVERPASS_FALLBACK:
        log.debug(
            "Overpass primary endpoint %s returned no usable result and public fallback is disabled.",
            PRIMARY_OVERPASS_URL,
        )

    with _overpass_cache_lock:
        _overpass_cache[key] = result
    return result


# ---------------------------------------------------------------------------
# Name matching — rapidfuzz token_set_ratio
# ---------------------------------------------------------------------------

def _score_candidate(candidate_name: str, osm_type: str, record: dict) -> float:
    """
    Score a POI name+OSM-type against the record's IHK/NACE/sector descriptions.

    Uses token_set_ratio which handles:
      - partial word overlap  ("Einzelhandel mit Getränken" ↔ "beverages")
      - different word order
      - extra words in either string
    Returns 0–100.
    """
    targets = [
        record.get("ihk_branch_desc") or "",
        record.get("nace_desc") or "",
        record.get("sector") or "",
    ]
    osm_desc = _OSM_TYPE_DESC.get(osm_type, osm_type or "")

    best = 0.0
    for target in targets:
        if not target:
            continue
        # Score the POI name directly against the industry description
        best = max(best, fuzz.token_set_ratio(candidate_name, target))
        # Score the OSM type's German description against the industry description
        if osm_desc:
            best = max(best, fuzz.token_set_ratio(osm_desc, target))
    return best


def best_name_match(
    pois: list[dict], record: dict
) -> tuple[str | None, float]:
    """
    Pick the best matching POI name for a record.
    Returns (name, score) or (None, 0) if no candidate exceeds the threshold.
    """
    if not pois:
        return None, 0.0

    scored = [
        (_score_candidate(p["name"], p["osm_type"], record), p["name"], p["osm_type"])
        for p in pois
    ]
    scored.sort(reverse=True)
    top_score, top_name, top_type = scored[0]

    if top_score >= NAME_MATCH_MIN_SCORE:
        log.debug("Name match: %r (type=%r score=%.1f)", top_name, top_type, top_score)
        return top_name, top_score

    return None, top_score


# ---------------------------------------------------------------------------
# Nominatim reverse geocoding — address only (street + postcode)
# ---------------------------------------------------------------------------
_nominatim_cache: dict[tuple[float, float], dict] = {}


def reverse_geocode_address(
    session: requests.Session, lat: float, lon: float
) -> dict[str, str | None]:
    """Return street address dict from Nominatim for (lat, lon)."""
    key = (round(lat, 4), round(lon, 4))
    if key in _nominatim_cache:
        return _nominatim_cache[key]

    result: dict = {}
    for attempt in range(1, NOMINATIM_RETRIES + 1):
        time.sleep(NOMINATIM_DELAY_S)
        try:
            resp = session.get(
                NOMINATIM_URL,
                params={"lat": lat, "lon": lon, "format": "jsonv2", "addressdetails": "1"},
                headers={"User-Agent": "berlin-gewerbedaten-pipeline/1.0"},
                timeout=15,
            )
            if resp.status_code == 429:
                wait = NOMINATIM_DELAY_S * (2 ** attempt)
                log.warning("Nominatim 429 — backing off %.1fs (attempt %d/%d)", wait, attempt, NOMINATIM_RETRIES)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            result = resp.json()
            break
        except Exception as exc:  # noqa: BLE001
            log.warning("Nominatim error for (%s, %s): %s", lat, lon, exc)
            break

    addr = result.get("address", {})
    parsed = {
        "street": _join(addr.get("road"), addr.get("house_number")),
        "postcode_geo": addr.get("postcode"),
        "city_geo": addr.get("city") or addr.get("town") or addr.get("village"),
    }
    _nominatim_cache[key] = parsed
    return parsed


def _join(*parts: str | None, sep: str = " ") -> str | None:
    joined = sep.join(p for p in parts if p)
    return joined or None


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _safe_int(val: Any) -> int | None:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def build_record(
    feature: dict[str, Any],
    name: str | None,
    name_score: float,
    address: str | None,
    lon: float | None,
    lat: float | None,
) -> dict[str, Any]:
    """Map one WFS feature to a flat dict matching the target schema."""
    props = feature.get("properties", {})

    return {
        # --- identifiers ---
        "ref_no": props.get("opendata_id"),

        # --- location ---
        "lat": lat,
        "lon": lon,

        # --- address & name ---
        "name": name,
        "name_match_score": round(name_score, 1),  # confidence 0–100
        "address": address,
        "postcode": props.get("postcode"),
        "city": props.get("city"),
        "district": props.get("bezirk"),
        "locality": props.get("ortsteil"),

        # --- industry classification ---
        "ihk_branch_id": props.get("ihk_branch_id"),
        "ihk_branch_desc": props.get("ihk_branch_desc"),
        "nace_id": props.get("nace_id"),
        "nace_desc": props.get("nace_desc"),
        "sector_id": props.get("branch_top_level_id"),
        "sector": props.get("branch_top_level_desc"),

        # --- company details from WFS ---
        "legal_form": props.get("business_type"),
        "number_of_employees": props.get("employees_range"),
        "business_age_years": _safe_int(props.get("business_age")),

        # --- fields not in source: null placeholders ---
        "company_purpose": None,
        "establishment_date": None,
        "management_contacts": None,
        "capital": None,
        "registration_no": None,
        "tags": [],
        "url": None,
        "funding": None,
        "cluster": None,
        "revenue_yoy": None,
        "revenue_current": None,

        # --- metadata ---
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def enrich_missing(path: str) -> None:
    """Re-enrich records where name is still null using Overpass + rapidfuzz."""
    records = list(iter_output_records(path))

    missing = [r for r in records if not r.get("name") and r.get("lat") is not None]
    log.info("Re-enriching %d records with missing name …", len(missing))

    http = requests.Session()
    http.headers.update({"Accept": "application/json"})

    # Also load address index for any records still missing an address
    addr_index: AddressIndex | None = None
    if any(not r.get("address") for r in missing):
        try:
            addr_index = AddressIndex.load(http)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not load AddressIndex: %s", exc)

    for i, record in enumerate(missing, 1):
        pois = fetch_nearby_pois(http, record["lat"], record["lon"])
        name, score = best_name_match(pois, record)
        record["name"] = name
        record["name_match_score"] = round(score, 1)
        if not record.get("address") and addr_index:
            record["address"] = addr_index.nearest(record["lat"], record["lon"])
        log.info("[%d/%d] %s → name=%r (score=%.1f)", i, len(missing), record["ref_no"], name, score)

    with JsonLinesWriter(path) as writer:
        for record in records:
            writer.write(record)
    log.info("Saved %d records → %s", len(records), path)
    log.info("Re-enrichment done.")


# ---------------------------------------------------------------------------
# PostgreSQL loader
# ---------------------------------------------------------------------------

def _best_company_name(record: dict[str, Any]) -> str:
    if record.get("name"):
        return str(record["name"])
    return f"{SOURCE_KEY}:{record['ref_no']}"


def _record_address_text(record: dict[str, Any]) -> str | None:
    if record.get("address"):
        return str(record["address"])

    locality_parts = [record.get("postcode"), record.get("city")]
    locality = " ".join(str(part) for part in locality_parts if part)
    district = record.get("district")

    if locality and district:
        return f"{locality}, {district}"
    return locality or district


def _company_metadata(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_key": SOURCE_KEY,
        "source_ref_no": record.get("ref_no"),
        "ingested_at": record.get("ingested_at"),
        "name_match_score": record.get("name_match_score"),
    }


def _has_resolved_name(record: dict[str, Any]) -> bool:
    return bool(record.get("name"))


def _find_existing_company(cur: Any, record: dict[str, Any], company_name: str) -> str | None:
    if not _has_resolved_name(record) or not record.get("postcode"):
        return None

    cur.execute(
        """
        SELECT id
        FROM public.companies
        WHERE normalized_name = public.normalize_company_name(%s)
          AND postcode = %s
        ORDER BY
          CASE WHEN address = %s THEN 0 ELSE 1 END,
          updated_at DESC
        LIMIT 1
        """,
        (company_name, record["postcode"], _record_address_text(record)),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _upsert_company(cur: Any, record: dict[str, Any], company_name: str, *, company_id: str | None) -> tuple[str, bool]:
    from psycopg2.extras import Json

    address_text = _record_address_text(record)
    metadata = _company_metadata(record)
    wz_code = record.get("nace_id")

    if company_id:
        cur.execute(
            """
            UPDATE public.companies
            SET
              company_name = CASE WHEN %s THEN %s ELSE company_name END,
              normalized_name = CASE WHEN %s THEN public.normalize_company_name(%s) ELSE normalized_name END,
              legal_form = %s,
              address = %s,
              wz_code = %s,
              postcode = %s,
              city = %s,
              district = %s,
              locality = %s,
              metadata = %s,
              canonical_updated_at = now(),
              data_last_updated_at = now()
            WHERE id = %s
            RETURNING id
            """,
            (
                _has_resolved_name(record),
                company_name,
                _has_resolved_name(record),
                company_name,
                record.get("legal_form"),
                address_text,
                wz_code,
                record.get("postcode"),
                record.get("city"),
                record.get("district"),
                record.get("locality"),
                Json(metadata),
                company_id,
            ),
        )
        return cur.fetchone()[0], False

    cur.execute(
        """
        INSERT INTO public.companies (
          company_name,
          normalized_name,
          legal_form,
          address,
          wz_code,
          postcode,
          city,
          district,
          locality,
          metadata,
          canonical_updated_at,
          data_last_updated_at
        )
        VALUES (
          %s,
          public.normalize_company_name(%s),
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          now(),
          now()
        )
        RETURNING id
        """,
        (
            company_name,
            company_name,
            record.get("legal_form"),
            address_text,
            wz_code,
            record.get("postcode"),
            record.get("city"),
            record.get("district"),
            record.get("locality"),
            Json(metadata),
        ),
    )
    return cur.fetchone()[0], True


def _build_observations(record: dict[str, Any]) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []

    if record.get("name"):
        score = record.get("name_match_score")
        confidence = None if score is None else max(0.0, min(float(score) / 100.0, 1.0))
        observations.append(
            {
                "field_name": "company_name",
                "value_text": record["name"],
                "confidence": confidence,
                "metadata": {"match_score": score},
                "make_canonical": True,
            }
        )

    address_text = _record_address_text(record)
    if address_text:
        observations.append(
            {
                "field_name": "address",
                "value_text": address_text,
                "confidence": 0.95 if record.get("address") else 0.75,
                "metadata": {"from_official_address_index": bool(record.get("address"))},
                "make_canonical": True,
            }
        )

    if record.get("legal_form"):
        observations.append(
            {
                "field_name": "legal_form",
                "value_text": record["legal_form"],
                "confidence": 0.9,
                "metadata": {},
                "make_canonical": True,
            }
        )

    if record.get("number_of_employees"):
        observations.append(
            {
                "field_name": "employee_count_range",
                "value_text": record["number_of_employees"],
                "confidence": 0.85,
                "metadata": {},
                "make_canonical": False,
            }
        )

    if record.get("business_age_years") is not None:
        observations.append(
            {
                "field_name": "business_age_years",
                "value_number": record["business_age_years"],
                "confidence": 0.8,
                "metadata": {},
                "make_canonical": False,
            }
        )

    for field_name, key in (
        ("postcode", "postcode"),
        ("city", "city"),
        ("district", "district"),
        ("locality", "locality"),
        ("ihk_branch_desc", "ihk_branch_desc"),
        ("nace_desc", "nace_desc"),
        ("sector", "sector"),
    ):
        if record.get(key):
            observations.append(
                {
                    "field_name": field_name,
                    "value_text": record[key],
                    "confidence": 0.8,
                    "metadata": {},
                    "make_canonical": False,
                }
            )

    if record.get("lat") is not None and record.get("lon") is not None:
        observations.append(
            {
                "field_name": "coordinates",
                "value_jsonb": {"lat": record["lat"], "lon": record["lon"]},
                "confidence": 0.95,
                "metadata": {},
                "make_canonical": False,
            }
        )

    return observations


def _insert_observation(
    cur: Any,
    *,
    company_id: str,
    source_record_id: str,
    source_id: str,
    sync_run_id: str,
    observation: dict[str, Any],
) -> str:
    from psycopg2.extras import Json

    cur.execute(
        """
        INSERT INTO public.company_field_observations (
          company_id,
          company_source_record_id,
          source_id,
          source_sync_run_id,
          field_name,
          value_text,
          value_number,
          value_date,
          value_jsonb,
          observed_at,
          confidence,
          metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s)
        RETURNING id
        """,
        (
            company_id,
            source_record_id,
            source_id,
            sync_run_id,
            observation["field_name"],
            observation.get("value_text"),
            observation.get("value_number"),
            observation.get("value_date"),
            Json(observation["value_jsonb"]) if observation.get("value_jsonb") is not None else None,
            observation.get("confidence"),
            Json(observation.get("metadata", {})),
        ),
    )
    return cur.fetchone()[0]


def _set_canonical_observation(
    cur: Any,
    *,
    company_id: str,
    field_name: str,
    observation_id: str,
    source_id: str,
) -> None:
    from psycopg2.extras import Json

    cur.execute(
        """
        UPDATE public.company_field_observations
        SET is_canonical = false
        WHERE company_id = %s
          AND field_name = %s
          AND is_canonical = true
        """,
        (company_id, field_name),
    )
    cur.execute(
        """
        UPDATE public.company_field_observations
        SET is_canonical = true
        WHERE id = %s
        """,
        (observation_id,),
    )
    cur.execute(
        """
        INSERT INTO public.company_canonical_field_sources (
          company_id,
          field_name,
          source_id,
          company_field_observation_id,
          selected_at,
          metadata
        )
        VALUES (%s, %s, %s, %s, now(), %s)
        ON CONFLICT (company_id, field_name)
        DO UPDATE SET
          source_id = EXCLUDED.source_id,
          company_field_observation_id = EXCLUDED.company_field_observation_id,
          selected_at = EXCLUDED.selected_at,
          metadata = EXCLUDED.metadata
        """,
        (company_id, field_name, source_id, observation_id, Json({})),
    )


def load_output_to_db(path: str = OUTPUT_FILE) -> dict[str, int]:
    """Load the exported JSON file into the local Postgres schema."""
    import psycopg2
    from psycopg2.extras import Json

    connection = psycopg2.connect(PG_DSN)
    source_id: str | None = None
    sync_run_id: str | None = None
    stats = {"records_seen": 0, "records_created": 0, "records_updated": 0, "records_failed": 0}
    records_since_commit = 0

    try:
        with connection:
            with connection.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.sources (
                      key,
                      name,
                      source_type,
                      base_url,
                      priority,
                      default_sync_interval_minutes,
                      metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (key)
                    DO UPDATE SET
                      name = EXCLUDED.name,
                      source_type = EXCLUDED.source_type,
                      base_url = EXCLUDED.base_url,
                      metadata = public.sources.metadata || EXCLUDED.metadata
                    RETURNING id
                    """,
                    (
                        SOURCE_KEY,
                        SOURCE_NAME,
                        SOURCE_TYPE,
                        WFS_BASE_URL,
                        20,
                        1440,
                        Json({"pipeline": "berlin_gewerbedaten", "output_file": path}),
                    ),
                )
                source_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO public.source_sync_runs (
                      source_id,
                      sync_type,
                      status
                    )
                    VALUES (%s, 'full', 'running')
                    RETURNING id
                    """,
                    (source_id,),
                )
                sync_run_id = cur.fetchone()[0]

        with connection.cursor() as cur:
            for index, record in enumerate(iter_output_records(path), start=1):
                stats["records_seen"] += 1
                cur.execute("SAVEPOINT gewerbedaten_record")
                try:
                    external_id = str(record["ref_no"])
                    company_name = _best_company_name(record)

                    cur.execute(
                        """
                        SELECT id, company_id
                        FROM public.company_source_records
                        WHERE source_id = %s
                          AND external_id = %s
                        """,
                        (source_id, external_id),
                    )
                    existing = cur.fetchone()

                    source_record_id: str | None = None
                    if existing:
                        source_record_id, company_id = existing
                    else:
                        company_id = _find_existing_company(cur, record, company_name)

                    company_id, created = _upsert_company(
                        cur,
                        record,
                        company_name,
                        company_id=company_id,
                    )
                    if created:
                        stats["records_created"] += 1
                    else:
                        stats["records_updated"] += 1

                    cur.execute(
                        """
                        INSERT INTO public.company_source_records (
                          company_id,
                          source_id,
                          external_id,
                          external_url,
                          source_company_name,
                          source_payload,
                          first_seen_at,
                          last_seen_at,
                          last_synced_at,
                          is_deleted_in_source,
                          is_active
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, now(), now(), now(), false, true)
                        ON CONFLICT (source_id, external_id)
                        DO UPDATE SET
                          company_id = EXCLUDED.company_id,
                          external_url = EXCLUDED.external_url,
                          source_company_name = EXCLUDED.source_company_name,
                          source_payload = EXCLUDED.source_payload,
                          last_seen_at = now(),
                          last_synced_at = now(),
                          is_deleted_in_source = false,
                          is_active = true
                        RETURNING id
                        """,
                        (
                            company_id,
                            source_id,
                            external_id,
                            None,
                            record.get("name") or company_name,
                            Json(record),
                        ),
                    )
                    source_record_id = cur.fetchone()[0]

                    for observation in _build_observations(record):
                        observation_id = _insert_observation(
                            cur,
                            company_id=company_id,
                            source_record_id=source_record_id,
                            source_id=source_id,
                            sync_run_id=sync_run_id,
                            observation=observation,
                        )
                        if observation.get("make_canonical"):
                            _set_canonical_observation(
                                cur,
                                company_id=company_id,
                                field_name=observation["field_name"],
                                observation_id=observation_id,
                                source_id=source_id,
                            )

                    cur.execute("RELEASE SAVEPOINT gewerbedaten_record")
                    records_since_commit += 1
                    if records_since_commit >= DB_COMMIT_BATCH_SIZE:
                        connection.commit()
                        records_since_commit = 0
                        log.info(
                            "Loaded %d records so far (%d created, %d updated, %d failed).",
                            stats["records_seen"],
                            stats["records_created"],
                            stats["records_updated"],
                            stats["records_failed"],
                        )
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT gewerbedaten_record")
                    cur.execute("RELEASE SAVEPOINT gewerbedaten_record")
                    stats["records_failed"] += 1
                    log.exception(
                        "Failed to load source record %s (%d processed so far)",
                        record.get("ref_no"),
                        stats["records_seen"],
                    )

            connection.commit()

        with connection:
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.source_sync_runs
                    SET
                      status = %s,
                      finished_at = now(),
                      records_seen = %s,
                      records_created = %s,
                      records_updated = %s,
                      records_failed = %s
                    WHERE id = %s
                    """,
                    (
                        "success" if stats["records_failed"] == 0 else "partial",
                        stats["records_seen"],
                        stats["records_created"],
                        stats["records_updated"],
                        stats["records_failed"],
                        sync_run_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE public.sources
                    SET
                      last_synced_at = now(),
                      next_sync_at = now() + interval '1 day'
                    WHERE id = %s
                    """,
                    (source_id,),
                )
    except Exception:
        if sync_run_id:
            with connection:
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.source_sync_runs
                        SET
                          status = 'failed',
                          finished_at = now(),
                          records_seen = %s,
                          records_created = %s,
                          records_updated = %s,
                          records_failed = %s,
                          error_message = %s
                        WHERE id = %s
                        """,
                        (
                            stats["records_seen"],
                            stats["records_created"],
                            stats["records_updated"],
                            stats["records_failed"],
                            "Fatal loader error. Check Airflow task logs.",
                            sync_run_id,
                        ),
                    )
        raise
    finally:
        connection.close()

    log.info(
        "Loaded %d records into Postgres (%d created, %d updated, %d failed).",
        stats["records_seen"],
        stats["records_created"],
        stats["records_updated"],
        stats["records_failed"],
    )
    return stats


# ---------------------------------------------------------------------------
# Checkpoint — survive restarts without re-processing already-written records
# ---------------------------------------------------------------------------

def _read_checkpoint() -> dict:
    """Return the saved checkpoint dict, or {} if none exists / RESET_CHECKPOINT is set."""
    if RESET_CHECKPOINT:
        return {}
    try:
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_checkpoint(wfs_index: int) -> None:
    """Atomically persist the current WFS index so a restart can resume from here."""
    tmp = CHECKPOINT_FILE + ".tmp"
    _ensure_parent_dir(CHECKPOINT_FILE)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"wfs_index": wfs_index}, f)
    os.replace(tmp, CHECKPOINT_FILE)


def _delete_checkpoint() -> None:
    try:
        os.remove(CHECKPOINT_FILE)
    except FileNotFoundError:
        pass


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run() -> None:
    """Fetch and export the source data to JSONL."""
    http = requests.Session()
    http.headers.update({"Accept": "application/json"})

    # --- Step 0: resume from checkpoint if available ---
    checkpoint = _read_checkpoint()
    resume_index = checkpoint.get("wfs_index", WFS_START_INDEX)
    resuming = resume_index > 0
    if resuming:
        log.info("Resuming from checkpoint: WFS index %d", resume_index)
    else:
        log.info("Starting fresh from WFS index %d", resume_index)

    # --- Step 1: build address index (downloaded once, cached locally) ---
    addr_index: AddressIndex | None = None
    if not SKIP_ADDRESS_LOOKUP:
        try:
            addr_index = AddressIndex.load(http)
        except Exception as exc:  # noqa: BLE001
            log.warning("AddressIndex unavailable, addresses will be null: %s", exc)

    # --- Step 2: fetch Gewerbedaten and enrich ---
    # Overpass queries are I/O-bound (HTTP to local Overpass); run them in
    # parallel across OVERPASS_WORKERS threads.  We accumulate a batch of
    # features, submit futures for all that need Overpass, then flush the
    # batch in order so the JSONL output remains deterministic.
    #
    # Checkpoint is written after every batch so a restart resumes from the
    # last completed batch rather than the beginning of the dataset.
    _OVERPASS_BATCH = 200  # features buffered per flush cycle

    written = 0

    def _flush_batch(
        batch: list[tuple[dict, float | None, float | None, str | None, Future | None]],
    ) -> int:
        count = 0
        for feature, lon, lat, address, fut in batch:
            name: str | None = None
            name_score: float = 0.0
            if fut is not None:
                pois = fut.result()
                score_context = {
                    "ihk_branch_desc": feature.get("properties", {}).get("ihk_branch_desc"),
                    "nace_desc": feature.get("properties", {}).get("nace_desc"),
                    "sector": feature.get("properties", {}).get("branch_top_level_desc"),
                }
                name, name_score = best_name_match(pois, score_context)
            writer.write(build_record(feature, name, name_score, address, lon, lat))
            count += 1
        return count

    # Append to existing JSONL when resuming so already-written records are kept.
    with JsonLinesWriter(OUTPUT_FILE, append=resuming) as writer:
        with ThreadPoolExecutor(max_workers=OVERPASS_WORKERS) as pool:
            batch: list[tuple[dict, float | None, float | None, str | None, Future | None]] = []

            for feature in iter_features(http, start_index=resume_index):
                geom = feature.get("geometry") or {}
                coords = geom.get("coordinates", [None, None])
                x, y = coords[0], coords[1]

                if x is not None and y is not None:
                    lon, lat = epsg25833_to_wgs84(x, y)
                else:
                    lon, lat = None, None

                address: str | None = None
                if not SKIP_ADDRESS_LOOKUP and lon is not None and addr_index:
                    address = addr_index.nearest(lat, lon)

                fut: Future | None = None
                if not SKIP_NAME_LOOKUP and not SKIP_GEOCODE and lon is not None and _ihk_likely_has_osm_presence(feature.get("properties", {})):
                    fut = pool.submit(fetch_nearby_pois, lat, lon)

                batch.append((feature, lon, lat, address, fut))

                if len(batch) >= _OVERPASS_BATCH:
                    written += _flush_batch(batch)
                    batch.clear()
                    _write_checkpoint(resume_index + written)
                    log.info("Collected %d records so far … (checkpoint saved)", written)

            # flush remaining
            if batch:
                written += _flush_batch(batch)

    log.info("Saved %d records → %s", written, OUTPUT_FILE)
    log.info("Done. Total records: %d", written)

    # All records written successfully — remove the checkpoint so the next
    # scheduled run starts fresh.
    _delete_checkpoint()
    log.info("Checkpoint deleted — next run will start from scratch.")


def fetch_and_load() -> None:
    """Fetch the source data and load the resulting JSON into Postgres."""
    run()
    load_output_to_db(OUTPUT_FILE)


# ---------------------------------------------------------------------------
# Airflow DAG (optional — only imported when Airflow is present)
# ---------------------------------------------------------------------------

def _make_dag():  # noqa: ANN201
    """Return an Airflow DAG. Import-safe: no-op if Airflow is not installed."""
    if os.getenv("ENABLE_BERLIN_GEWERBEDATEN_DAG", "false").lower() != "true":
        log.info("Berlin Gewerbedaten DAG is disabled by default.")
        return None
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        return None

    with DAG(
        dag_id="berlin_gewerbedaten_to_json",
        description="Fetch Berlin Gewerbedaten WFS, export to JSON, and load into Postgres",
        schedule="@daily",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        tags=["berlin", "gewerbedaten", "wfs", "postgres"],
    ) as dag:
        fetch_and_export = PythonOperator(
            task_id="fetch_and_export",
            python_callable=run,
        )
        load_json_to_postgres = PythonOperator(
            task_id="load_json_to_postgres",
            python_callable=load_output_to_db,
        )

        fetch_and_export >> load_json_to_postgres

    return dag


dag = _make_dag()  # Airflow auto-discovers this

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run()
