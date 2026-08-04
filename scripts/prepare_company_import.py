#!/usr/bin/env python3

import csv
import json
import re
import sys
from pathlib import Path


HEADER_PREFIX = [
    "Interne ID",
    "Unique Key",
    "Register ID",
    "EU ID",
    "LEI",
    "Name",
    "Rechtsform",
    "ELF-Code",
    "Status",
    "Land",
    "PLZ",
    "Ort",
    "Straße",
    "Adress-Extra",
    "Latitude",
    "Longitude",
    "North Data URL",
    "Vertretungsregelung",
    "Gegenstand",
    "Branche",
    "Branchencode",
    "Tel.",
    "Fax",
    "E-Mail",
    "Website",
    "USt.-Id.",
    "Eintragung Datum",
]

OUTPUT_COLUMNS = [
    "company_name",
    "normalized_name",
    "registration_number",
    "legal_form",
    "business_purpose",
    "wz_code",
    "address",
    "established_at",
    "postcode",
    "city",
    "metadata_json",
]

def _fix_encoding(text: str) -> str:
    """Fix double-encoded text: UTF-8 bytes that were mis-read as CP1252 then re-encoded."""
    try:
        return text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
WZ_RE = re.compile(r"^\d{2}(?:\.\d{2}(?:\.\d)?)?$")
LEGAL_SUFFIX_RE = re.compile(
    r"\b(gmbh(?:\s*&\s*co\.\s*kg)?|ug|haftungsbeschränkt|ag|eg|kg|ohg|gbr|se|kgaa|mbh|e\.v\.)\b",
    re.IGNORECASE,
)


def normalize_company_name(value: str) -> str:
    value = value.lower().replace("&", " und ")
    value = re.sub(r"[^0-9a-zäöüß]+", " ", value, flags=re.IGNORECASE)
    value = LEGAL_SUFFIX_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def build_address(street: str, extra: str, postcode: str, city: str) -> str | None:
    left = " ".join(part for part in [street.strip(), extra.strip()] if part)
    right = " ".join(part for part in [postcode.strip(), city.strip()] if part)
    full = ", ".join(part for part in [left, right] if part)
    return full or None


def parse_clean_csv_line(raw_line: str) -> list[str]:
    cleaned = raw_line.rstrip("\n")
    if cleaned.startswith('"'):
        cleaned = cleaned[1:]
    cleaned = cleaned.rstrip(";")
    if cleaned.endswith('"'):
        cleaned = cleaned[:-1]
    cleaned = cleaned.replace('""', '"')
    return next(csv.reader([cleaned]))


def repair_prefix_row(row: list[str]) -> list[str]:
    if len(row) < len(HEADER_PREFIX):
        raise ValueError(f"too few columns to repair: {len(row)}")

    date_index = None
    for idx in range(26, min(len(row), 60)):
        if DATE_RE.match(row[idx]) and idx - 6 >= 20 and WZ_RE.match(row[idx - 6]):
            date_index = idx
            break

    if date_index is None:
        raise ValueError("could not locate established_at / branchcode anchor")

    branchcode_index = date_index - 6
    branche_index = branchcode_index - 1
    if branche_index <= 18:
        raise ValueError("branche anchor too early")

    gegenstand = ",".join(part.strip() for part in row[18:branche_index]).strip() or None
    branche = row[branche_index].strip() or None

    repaired = row[:18] + [
        gegenstand or "",
        branche or "",
    ] + row[branchcode_index:date_index + 1]

    if len(repaired) != len(HEADER_PREFIX):
        raise ValueError(f"repair produced {len(repaired)} columns instead of {len(HEADER_PREFIX)}")

    return repaired


def parse_relevant_fields(raw_line: str) -> dict[str, str]:
    row = parse_clean_csv_line(raw_line)
    if len(row) != len(HEADER_PREFIX):
        row = repair_prefix_row(row)
    return dict(zip(HEADER_PREFIX, row))


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: prepare_company_import.py <input_csv> <output_csv>", file=sys.stderr)
        return 2

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    repaired_rows = 0
    skipped_rows = 0
    written_rows = 0
    skipped_examples: list[str] = []

    with input_path.open("r", encoding="utf-8-sig") as src, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        header = next(csv.reader([_fix_encoding(src.readline().rstrip("\n"))]))
        if header[: len(HEADER_PREFIX)] != HEADER_PREFIX:
            raise ValueError("unexpected input header")

        writer = csv.DictWriter(dst, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for lineno, raw_line in enumerate(src, start=2):
            if not raw_line.strip():
                continue
            raw_line = _fix_encoding(raw_line)

            try:
                row = parse_clean_csv_line(raw_line)
                if len(row) != len(HEADER_PREFIX):
                    row = repair_prefix_row(row)
                    repaired_rows += 1
                data = dict(zip(HEADER_PREFIX, row))
            except Exception as exc:
                skipped_rows += 1
                if len(skipped_examples) < 25:
                    skipped_examples.append(f"line {lineno}: {exc}")
                continue

            company_name = (data.get("Name") or "").strip()
            if not company_name:
                skipped_rows += 1
                continue

            metadata = {
                "source_file": input_path.name,
                "internal_id": data.get("Interne ID") or None,
                "unique_key": data.get("Unique Key") or None,
                "eu_id": data.get("EU ID") or None,
                "lei": data.get("LEI") or None,
                "elf_code": data.get("ELF-Code") or None,
                "status": data.get("Status") or None,
                "country": data.get("Land") or None,
                "branch_description": data.get("Branche") or None,
                "north_data_url": data.get("North Data URL") or None,
                "website": data.get("Website") or None,
                "email": data.get("E-Mail") or None,
                "phone": data.get("Tel.") or None,
                "fax": data.get("Fax") or None,
                "vat_id": data.get("USt.-Id.") or None,
            }

            writer.writerow(
                {
                    "company_name": company_name,
                    "normalized_name": normalize_company_name(company_name),
                    "registration_number": (data.get("Register ID") or "").strip() or None,
                    "legal_form": (data.get("Rechtsform") or "").strip() or None,
                    "business_purpose": (data.get("Gegenstand") or "").strip() or None,
                    "wz_code": (data.get("Branchencode") or "").strip() or None,
                    "address": build_address(
                        data.get("Straße") or "",
                        data.get("Adress-Extra") or "",
                        data.get("PLZ") or "",
                        data.get("Ort") or "",
                    ),
                    "established_at": (data.get("Eintragung Datum") or "").strip() or None,
                    "postcode": (data.get("PLZ") or "").strip() or None,
                    "city": (data.get("Ort") or "").strip() or None,
                    "metadata_json": json.dumps(metadata, ensure_ascii=False),
                }
            )
            written_rows += 1

    print(
        json.dumps(
            {
                "written_rows": written_rows,
                "repaired_rows": repaired_rows,
                "skipped_rows": skipped_rows,
                "skipped_examples": skipped_examples,
                "output": str(output_path),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
