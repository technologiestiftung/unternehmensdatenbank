#!/usr/bin/env python3
"""Export the website results to one Excel workbook, for people who do not write SQL.

## Why this exists

`public.v_website_provenance` (V15) answers "where did this company's website come from" in one
query, but only if you have psql and know the column names. Most of the people who need to *check*
this work — is that really the company's site? is the field plausible? — want a sortable table.
This writes one `.xlsx` with three sheets, and nothing else.

`.xlsx` rather than CSV on purpose. A comma-delimited CSV opened by double-click on a German Excel
splits on the wrong character, and a UTF-8 CSV without the three-byte BOM shows `OsnabrÃ¼ck`.
Both traps are documented in `results_to_csv.py`, and both disappear with a real workbook.

## The three sheets

    Websites          one row per company that HAS a website: url, source, method, verified
    Ohne Website      active companies that still have none — the remaining work list
    Zusammenfassung   counts by source and method, and coverage by register status

## The column that matters

`verified` is TRUE when some page was actually fetched and something on it tied the page to that
company. It is FALSE only for `northdata_export`, which asserts a URL and offers no evidence.
**Filter on `verified`, not on `confidence`:** the export carries a flat 0.50, which sits above
several genuinely verified discovery findings, so sorting by confidence mixes the two kinds back
together. Measured 2026-09-07: 3,542 websites, of which 726 are evidence-backed.

## Usage

    python scripts/export_website_results.py
    python scripts/export_website_results.py -o "C:/Users/TuPhan/Downloads/websites.xlsx"
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "airflow" / "dags"))

# Host-side default, same line as every other script here: the DAG default `host=postgres` resolves
# only inside the compose network, and it has to be set before source_enrichment_common is imported.
os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("export_website_results")

DEFAULT_OUT = REPO / "airflow" / "data" / "website_results.xlsx"

WEBSITES_SQL = """
SELECT company_name              AS "Firma",
       registration_number       AS "Registernummer",
       register_status           AS "Status",
       website_url               AS "Website",
       found_by_source           AS "Quelle",
       method                    AS "Methode",
       verified                  AS "Belegt",
       website_confidence        AS "Konfidenz",
       imprint_url               AS "Impressum",
       why                       AS "Begruendung"
FROM public.v_website_provenance
WHERE website_url IS NOT NULL
ORDER BY verified DESC, found_by_source, company_name
"""

# `business_purpose` is a `companies` column and is NOT on v_website_provenance, so it is joined
# back in. It earns its place on this sheet: the Gegenstand is what tells a human whether a company
# with no website plausibly HAS one to find, or is a holding shell that never will.
MISSING_SQL = """
SELECT v.company_name            AS "Firma",
       v.registration_number     AS "Registernummer",
       v.register_status         AS "Status",
       c.city                    AS "Ort",
       c.business_purpose        AS "Gegenstand"
FROM public.v_website_provenance v
JOIN public.companies c ON c.id = v.id
WHERE v.website_url IS NULL
  AND v.register_status = 'aktiv'
ORDER BY v.company_name
"""

BY_SOURCE_SQL = """
SELECT found_by_source           AS "Quelle",
       method                    AS "Methode",
       count(*)                  AS "Websites",
       count(*) FILTER (WHERE verified) AS "davon belegt",
       round(avg(website_confidence), 4) AS "Konfidenz (Mittel)"
FROM public.v_website_provenance
WHERE website_url IS NOT NULL
GROUP BY 1, 2
ORDER BY 3 DESC
"""

BY_STATUS_SQL = """
SELECT register_status           AS "Status",
       count(*)                  AS "Firmen",
       count(website_url)        AS "mit Website",
       count(*) FILTER (WHERE verified) AS "davon belegt",
       round(100.0 * count(website_url) / count(*), 1) AS "Abdeckung %"
FROM public.v_website_provenance
GROUP BY 1
ORDER BY 2 DESC
"""

#: Everything, from `v_company_full` (V16): all 14,443 companies including the 7,673 dissolved ones,
#: our findings, and the raw export columns prefixed `export_`. `SELECT *` on purpose — the view is
#: the contract, and re-listing 55 columns here would be a second place to forget one.
#:
#: `raw` (the whole export record as JSONB) is deliberately NOT in the view and so not here: it is
#: several kilobytes per company and Excel has a 32,767-character cell limit it would breach.
EVERYTHING_SQL = """
SELECT * FROM public.v_company_full
ORDER BY register_status, company_name
"""


def frame(sql: str):
    """One query -> one DataFrame. `read_sql` wants a live connection, so open one per call."""
    import warnings

    import pandas as pd

    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        # pandas warns that only SQLAlchemy connectables are "tested" and psycopg2 is not. It reads
        # fine — every other script here uses psycopg2 directly, and adding SQLAlchemy just to
        # silence a warning would be a dependency for nothing. Filtered so the run output stays
        # readable, because a warning printed on every successful run trains people to ignore
        # warnings.
        warnings.filterwarnings("ignore", message=".*only supports SQLAlchemy connectable.*")
        return strip_timezones(pd.read_sql(sql, connection))
    finally:
        connection.close()


def strip_timezones(df):
    """Drop the timezone from every timestamp column, keeping the wall-clock time in UTC.

    Postgres hands back `timestamptz` as timezone-aware, and openpyxl refuses it outright:
    `ValueError: Excel does not support datetimes with timezones`. Excel has no concept of an
    offset, so something has to go, and the offset is the part nobody reading a spreadsheet will
    miss. Converted to UTC first rather than dropped in place, so two rows imported from different
    offsets still sort against each other correctly.

    Applies to `website_resolved_at` and `export_last_update` today; written generically because
    the next timestamp column added to the view would otherwise break the export again.
    """
    import pandas as pd

    for name in df.columns:
        if isinstance(df[name].dtype, pd.DatetimeTZDtype):
            df[name] = df[name].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def autosize(worksheet, df) -> None:
    """Widen each column to its content, capped, so nothing shows as ####.

    Excel opens with every column at the default width, which makes a URL column useless until you
    drag it. Capped at 60 because `business_purpose` runs to hundreds of characters and one column
    that wide pushes everything else off the screen.
    """
    from openpyxl.utils import get_column_letter

    for index, name in enumerate(df.columns, start=1):
        longest = max([len(str(name))] + [len(str(v)) for v in df[name].head(500)])
        worksheet.column_dimensions[get_column_letter(index)].width = min(max(longest + 2, 10), 60)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-o", "--out", type=Path, default=DEFAULT_OUT, help="path to the .xlsx")
    args = parser.parse_args()

    sheets = {
        "Alles": frame(EVERYTHING_SQL),
        "Websites": frame(WEBSITES_SQL),
        "Ohne Website": frame(MISSING_SQL),
    }
    summary_by_source = frame(BY_SOURCE_SQL)
    summary_by_status = frame(BY_STATUS_SQL)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    import pandas as pd

    # Windows locks a file that Excel has open, and the raw error is
    # `PermissionError: [Errno 13] Permission denied` with a path and no hint. Since the entire
    # point of this script is a file you open in Excel, re-running it with the previous output still
    # open is the single most likely way to use it wrong.
    try:
        args.out.open("ab").close()
    except PermissionError:
        raise SystemExit(
            f"{args.out} is open in another program (Excel locks the file it is showing).\n"
            f"Close it and re-run, or write elsewhere with -o <path>."
        )

    with pd.ExcelWriter(args.out, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)
            autosize(writer.sheets[name], df)

        # Two tables on one sheet, so "how are we doing" is a single glance rather than two tabs.
        summary_by_source.to_excel(writer, sheet_name="Zusammenfassung", index=False, startrow=0)
        offset = len(summary_by_source) + 3
        summary_by_status.to_excel(writer, sheet_name="Zusammenfassung", index=False, startrow=offset)
        autosize(writer.sheets["Zusammenfassung"], summary_by_source)

        # Freeze the header row on every sheet, and turn on Excel's filter dropdowns. Without this
        # a 3,500-row sheet is unusable: scroll down and you no longer know which column is which.
        for name in list(sheets) + ["Zusammenfassung"]:
            worksheet = writer.sheets[name]
            worksheet.freeze_panes = "A2"
            if name in sheets and len(sheets[name]):
                worksheet.auto_filter.ref = worksheet.dimensions

    total = len(sheets["Websites"])
    verified = int(sheets["Websites"]["Belegt"].sum()) if total else 0
    everything = sheets["Alles"]
    log.info("%s  (%.1f MB)", args.out, args.out.stat().st_size / 1_048_576)
    log.info("  Alles         %5d companies x %d columns — every register status, ours + export",
             len(everything), len(everything.columns))
    log.info("  Websites      %5d rows (%d evidence-backed, %d asserted by the export)",
             total, verified, total - verified)
    log.info("  Ohne Website  %5d active companies still missing one", len(sheets["Ohne Website"]))
    log.info("  Zusammenfassung: %d source/method combinations", len(summary_by_source))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
