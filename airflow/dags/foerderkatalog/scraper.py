# Vendored unchanged from https://github.com/technologiestiftung/udb-foerderkatalog-scraper
# (scraper.py at caac874). Fix bugs upstream and copy the file again; foerderkatalog/main.py
# only uses FoekatClient, scrape and build_grant_rows.
#!/usr/bin/env python3
"""Förderkatalog des Bundes – Scraper für Förderungen in Berlin.

Sammelt alle im Förderkatalog erfassten Förderungen für in Berlin ansässige
Förderempfänger und schreibt zwei CSV:
  * foekat_berlin_by_company.csv   – je Unternehmen aggregiert
  * foekat_berlin_foerderungen.csv – eine schlanke Zeile je Förderung

Grundlage für die Anreicherung der Berliner Unternehmensdatenbank.

Ablauf: Session aufbauen → Detailsuche (Bundesland Berlin) → kompletten
CSV-Export laden → Trefferzahl validieren → bereinigen → aggregieren/schreiben.
Klemmt der Direktexport, wird nach Laufzeit-Jahren aufgeteilt (Dedup über FKZ).

Nutzung:
    pip install -r requirements.txt
    python main.py
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry

log = logging.getLogger("foekat")


# --------------------------------------------------------------------------- #
# Konfiguration
# --------------------------------------------------------------------------- #

# Technische Konstanten (serverseitig vorgegeben – nicht in config.toml).
BASE = "https://foerderportal.bund.de/foekat/jsp/SucheAction.do"
SERVER_ENCODING = "ISO-8859-15"
USER_AGENT = (
    "udb-foerderkatalog-scraper/1.0 (research; enrichment of Berlin company database)"
)

CONFIG_PATH = "config.toml"


@dataclass(frozen=True)
class Config:
    """Benutzer-Einstellungen aus config.toml (siehe dort für Erläuterungen)."""

    bundesland: str = "Berlin"
    ziel: str = "ZE"            # "ZE" = Förderempfänger, "ST" = ausführende Stelle
    nur_laufende: str = "N"     # "J" = nur laufende Vorhaben, "N" = alle
    jahr_von: int = 1960
    jahr_bis: int = 2027
    delay: float = 1.0          # Sekunden Pause zwischen Requests (fair scrapen)
    timeout: int = 60           # Sekunden pro Request
    verzeichnis: str = "."      # Zielordner für alle Ausgabedateien
    out_agg: str = "foekat_berlin_by_company.csv"
    out_grants: str = "foekat_berlin_foerderungen.csv"
    out_raw: str = "foekat_berlin_raw.csv"
    save_raw: bool = False


def load_config(path: str = CONFIG_PATH) -> Config:
    """config.toml lesen; fehlende Datei/Schlüssel → Standardwerte aus Config."""
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError:
        log.warning("%s nicht gefunden – nutze Standardwerte.", path)
        return Config()

    suche = data.get("suche", {})
    zeitraum = data.get("zeitraum", {})
    netzwerk = data.get("netzwerk", {})
    ausgabe = data.get("ausgabe", {})
    default = Config()
    return Config(
        bundesland=suche.get("bundesland", default.bundesland),
        ziel=suche.get("ziel", default.ziel),
        nur_laufende="J" if suche.get("nur_laufende", False) else "N",
        jahr_von=zeitraum.get("jahr_von", default.jahr_von),
        jahr_bis=zeitraum.get("jahr_bis", default.jahr_bis),
        delay=netzwerk.get("delay", default.delay),
        timeout=netzwerk.get("timeout", default.timeout),
        verzeichnis=ausgabe.get("verzeichnis", default.verzeichnis),
        out_agg=ausgabe.get("aggregat", default.out_agg),
        out_grants=ausgabe.get("foerderungen", default.out_grants),
        out_raw=ausgabe.get("roh", default.out_raw),
        save_raw=ausgabe.get("roh_speichern", default.save_raw),
    )


CONFIG = load_config()


# --------------------------------------------------------------------------- #
# HTTP-Client
# --------------------------------------------------------------------------- #

_TREFFER_RE = re.compile(r"([\d.]+)\s+Treffer\s+insgesamt", re.IGNORECASE)


@dataclass
class SearchParams:
    """Parameter für eine Detailsuche (Laufzeit optional für den Fallback)."""

    bundesland: str = field(default_factory=lambda: CONFIG.bundesland)
    laufzeit_von: str | None = None   # Format: TT.MM.JJJJ
    laufzeit_bis: str | None = None   # Format: TT.MM.JJJJ

    def as_form(self) -> dict[str, str]:
        form = {
            "actionMode": "searchlist",
            "suche.detailSuche": "true",
            "suche.nurVerbund": "N",
            "suche.lfdVhb": CONFIG.nur_laufende,
            "suche.ZeSt": CONFIG.ziel,
            "suche.bundeslandSuche[0]": self.bundesland,
        }
        if self.laufzeit_von:
            form["suche.laufzeitVonSuche[0]"] = self.laufzeit_von
        if self.laufzeit_bis:
            form["suche.laufzeitBisSuche[0]"] = self.laufzeit_bis
        return form


class FoekatClient:
    """Dünner Client für Suche und CSV-Export des Förderkatalogs.

    Als Context-Manager nutzbar: baut beim Betreten die Session auf (Cookies
    der Suchmaske) und schließt sie beim Verlassen.
    """

    def __init__(self) -> None:
        self.session = self._build_session()

    def __enter__(self) -> "FoekatClient":
        self.prime()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.session.close()

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept-Language": "de-DE,de;q=0.9",
        })
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _request(self, method: str, **kwargs) -> requests.Response:
        """Request an BASE mit einheitlichem Timeout, Fehler- und Encoding-Handling."""
        r = self.session.request(method, BASE, timeout=CONFIG.timeout, **kwargs)
        r.raise_for_status()
        r.encoding = SERVER_ENCODING
        return r

    def prime(self) -> None:
        """Suchmaske laden, damit die Session gültige Cookies erhält."""
        self._request("GET", params={"actionMode": "searchmask"})
        log.info(
            "Session initialisiert (Cookies: %s)",
            ", ".join(self.session.cookies.keys()) or "keine",
        )

    def search(self, params: SearchParams) -> int:
        """Detailsuche abschicken; liefert die vom Server gemeldete Trefferzahl."""
        text = self._request("POST", data=params.as_form()).text
        match = _TREFFER_RE.search(text)
        if match:
            return int(match.group(1).replace(".", ""))
        if "Treffer" not in text and "Ergebnis" not in text:
            raise RuntimeError(
                "Ergebnisseite nicht erkannt – das Suchformular hat sich "
                "vermutlich geändert."
            )
        return 0

    def download_csv(self) -> str:
        """CSV-Export der aktuellen Trefferliste als Text laden."""
        return self._request(
            "GET", params={"actionMode": "print", "presentationType": "csv"}
        ).text


# --------------------------------------------------------------------------- #
# Parsen & Aufbereiten (reine Funktionen)
# --------------------------------------------------------------------------- #

_WRAPPER_RE = re.compile(r'^="?(.*?)"?$', re.DOTALL)   # Excel-Text-Wrapper ="..."
_DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
_NUMBER_RE = re.compile(r"-?\d+(\.\d+)?")

# Geo-Spalten kommen doppelt vor (Förderempfänger + ausführende Stelle); der
# zweite Block bekommt zur Disambiguierung ein Suffix.
_DUP_GEO_COLS = {"Gemeindekennziffer", "Stadt/Gemeinde", "Ort", "Bundesland", "Staat"}
SUM_COL = "Fördersumme_EUR"   # berechnete, numerische Summenspalte

AGG_HEADER = [
    "Förderempfänger",
    "Stadt/Gemeinde",
    "Ort",
    "Bundesland",
    "Anzahl_Förderungen",
    "Fördersumme_EUR_gesamt",
    "Erste_Laufzeit_von",
    "Letzte_Laufzeit_bis",
    "Ressorts",
]

GRANT_HEADER = [
    "Förderempfänger",
    "Ort",
    "FKZ",
    "Fördersumme_EUR",
    "Laufzeit von",
    "Laufzeit bis",
    "Thema des geförderten Vorhabens",
    "Ressort",
    "Förderart",
]


def _unwrap(cell: str) -> str:
    """Excel-Text-Wrapper ="..." entfernen und trimmen."""
    m = _WRAPPER_RE.match(cell.strip())
    return (m.group(1) if m else cell).strip()


def _field(row: dict[str, str], key: str) -> str:
    """Feldwert getrimmt lesen (None-sicher)."""
    return (row.get(key) or "").strip()


def _date_key(value: str) -> tuple[int, int, int]:
    """'TT.MM.JJJJ' -> sortierbarer (Jahr, Monat, Tag); ungültig -> (0, 0, 0)."""
    m = _DATE_RE.fullmatch(value.strip())
    if not m:
        return (0, 0, 0)
    day, month, year = (int(g) for g in m.groups())
    return (year, month, day)


def _parse_euro(value: str) -> str:
    """Deutsche Zahl '583.639,00' -> '583639.00'. Ungültig/leer -> ''."""
    v = _unwrap(value).replace(".", "").replace(",", ".")
    return v if _NUMBER_RE.fullmatch(v) else ""


def _to_float(value: str | None) -> float:
    try:
        return float(value) if value else 0.0
    except ValueError:
        return 0.0


def _company_name(row: dict[str, str]) -> str:
    return _field(row, "Förderempfänger /Auftragnehmer")


def _fkz(row: dict[str, str]) -> str:
    for key in ("FKZ", "Fkz", "Förderkennzeichen"):
        if row.get(key):
            return row[key]
    return str(sorted(row.items()))


def _dedup_headers(headers: list[str]) -> list[str]:
    """Doppelte Geo-Spalten der ausführenden Stelle mit Suffix versehen."""
    seen: set[str] = set()
    result: list[str] = []
    for h in headers:
        if h in _DUP_GEO_COLS and h in seen:
            result.append(f"{h} (ausführende Stelle)")
        else:
            result.append(h)
            seen.add(h)
    return result


def parse_export(raw_csv: str) -> list[dict[str, str]]:
    """Roh-CSV in bereinigte Zeilen (dicts) mit numerischer Summenspalte."""
    reader = csv.reader(io.StringIO(raw_csv), delimiter=";")
    rows = [r for r in reader if any(c.strip() for c in r)]
    if not rows:
        return []

    header = _dedup_headers([_unwrap(c) for c in rows[0] if c.strip()])
    sum_src = next(
        (h for h in header if h.lower().startswith("förder- / auftragssumme")), None
    )

    out: list[dict[str, str]] = []
    for raw in rows[1:]:
        cells = [_unwrap(c) for c in raw][: len(header)]
        cells += [""] * (len(header) - len(cells))
        row = dict(zip(header, cells))
        if sum_src:
            row[SUM_COL] = _parse_euro(row.get(sum_src, ""))
        out.append(row)
    return out


@dataclass
class _CompanyAgg:
    """Akkumulator für die Förderungen eines Unternehmens."""

    name: str
    stadt: str
    ort: str
    bundesland: str
    count: int = 0
    total: float = 0.0
    first_von: str = ""
    last_bis: str = ""
    ressorts: set[str] = field(default_factory=set)

    def add(self, row: dict[str, str]) -> None:
        self.count += 1
        self.total += _to_float(row.get(SUM_COL))

        von = _field(row, "Laufzeit von")
        if von and (not self.first_von or _date_key(von) < _date_key(self.first_von)):
            self.first_von = von
        bis = _field(row, "Laufzeit bis")
        if bis and (not self.last_bis or _date_key(bis) > _date_key(self.last_bis)):
            self.last_bis = bis

        if ressort := _field(row, "Ressort"):
            self.ressorts.add(ressort)

    def as_row(self) -> dict[str, str]:
        return {
            "Förderempfänger": self.name,
            "Stadt/Gemeinde": self.stadt,
            "Ort": self.ort,
            "Bundesland": self.bundesland,
            "Anzahl_Förderungen": str(self.count),
            "Fördersumme_EUR_gesamt": f"{self.total:.2f}",
            "Erste_Laufzeit_von": self.first_von,
            "Letzte_Laufzeit_bis": self.last_bis,
            "Ressorts": ", ".join(sorted(self.ressorts)),
        }


def aggregate_by_company(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Förderungen je (Förderempfänger, Ort) zusammenfassen.

    Absteigend nach Gesamt-Fördersumme sortiert (größte Förderung zuerst).
    """
    companies: dict[tuple[str, str], _CompanyAgg] = {}
    for row in rows:
        name = _company_name(row)
        if not name:
            continue
        ort = _field(row, "Ort")
        agg = companies.get((name, ort))
        if agg is None:
            agg = companies[(name, ort)] = _CompanyAgg(
                name=name,
                stadt=_field(row, "Stadt/Gemeinde"),
                ort=ort,
                bundesland=_field(row, "Bundesland"),
            )
        agg.add(row)

    result = [agg.as_row() for agg in companies.values()]
    result.sort(key=lambda r: float(r["Fördersumme_EUR_gesamt"]), reverse=True)
    return result


def build_grant_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Schlanke Detailzeilen je Förderung, sortiert nach Unternehmen und Datum."""
    grants = [
        {
            "Förderempfänger": name,
            "Ort": _field(row, "Ort"),
            "FKZ": _fkz(row),
            "Fördersumme_EUR": row.get(SUM_COL, ""),
            "Laufzeit von": _field(row, "Laufzeit von"),
            "Laufzeit bis": _field(row, "Laufzeit bis"),
            "Thema des geförderten Vorhabens": _field(row, "Thema des geförderten Vorhabens"),
            "Ressort": _field(row, "Ressort"),
            "Förderart": _field(row, "Förderart"),
        }
        for row in rows
        if (name := _company_name(row))
    ]
    grants.sort(key=lambda r: (r["Förderempfänger"].casefold(), _date_key(r["Laufzeit von"])))
    return grants


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #

@dataclass
class ScrapeResult:
    rows: list[dict[str, str]] = field(default_factory=list)
    raw_csv: str = ""


def scrape(client: FoekatClient) -> ScrapeResult:
    """Primärweg: eine Suche über ganz Berlin + kompletter CSV-Export."""
    total = client.search(SearchParams())
    log.info("Server meldet %d Treffer für %s.", total, CONFIG.bundesland)
    if total == 0:
        return ScrapeResult()

    time.sleep(CONFIG.delay)
    raw = client.download_csv()
    rows = parse_export(raw)
    log.info("CSV-Export geladen: %d Datenzeilen.", len(rows))

    if len(rows) < total:
        log.warning(
            "Export unvollständig (%d < %d). Wechsle auf Jahres-Fallback.",
            len(rows), total,
        )
        return _scrape_by_year(client)
    return ScrapeResult(rows=rows, raw_csv=raw)


def _scrape_by_year(client: FoekatClient) -> ScrapeResult:
    """Fallback: Suche pro Laufzeit-Jahr, Merge + Dedup über FKZ."""
    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for year in range(CONFIG.jahr_von, CONFIG.jahr_bis + 1):
        params = SearchParams(laufzeit_von=f"01.01.{year}", laufzeit_bis=f"31.12.{year}")
        try:
            if client.search(params) == 0:
                log.info("%d: 0 Treffer.", year)
                time.sleep(CONFIG.delay)
                continue
            time.sleep(CONFIG.delay)
            year_rows = parse_export(client.download_csv())
        except Exception as exc:  # noqa: BLE001 - Jahr überspringen, weitermachen
            log.error("%d: Fehler – %s", year, exc)
            time.sleep(5)
            continue
        new = 0
        for row in year_rows:
            key = _fkz(row)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
            new += 1
        log.info("%d: %d neu (gesamt %d).", year, new, len(rows))
        time.sleep(CONFIG.delay)
    return ScrapeResult(rows=rows)


def _write_text(path: str, text: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        f.write(text)


def _write_csv(path: str, header: list[str], rows: list[dict[str, str]]) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=header, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    _write_text(path, buf.getvalue())


def _out_path(name: str, stamp: str) -> str:
    """Vollständiger Ausgabepfad: <verzeichnis>/<stamp>_<name>."""
    return str(Path(CONFIG.verzeichnis) / f"{stamp}_{name}")


def write_outputs(result: ScrapeResult, stamp: str) -> None:
    """Aggregat je Unternehmen + schlanke Förderungs-Detailtabelle schreiben.

    Alle Dateien landen in ``CONFIG.verzeichnis`` und erhalten das Präfix
    ``<stamp>_`` (yyyymmdd vom Laufzeitpunkt).
    """
    if CONFIG.save_raw and result.raw_csv:
        raw_path = _out_path(CONFIG.out_raw, stamp)
        _write_text(raw_path, result.raw_csv)
        log.info("Roh-Export gespeichert → %s", raw_path)

    companies = aggregate_by_company(result.rows)
    agg_path = _out_path(CONFIG.out_agg, stamp)
    _write_csv(agg_path, AGG_HEADER, companies)
    log.info("Aggregiert: %d Unternehmen → %s", len(companies), agg_path)

    grants = build_grant_rows(result.rows)
    grants_path = _out_path(CONFIG.out_grants, stamp)
    _write_csv(grants_path, GRANT_HEADER, grants)
    log.info("Förderungen: %d Einträge → %s", len(grants), grants_path)
