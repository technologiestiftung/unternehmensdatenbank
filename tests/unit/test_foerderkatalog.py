"""`foerderkatalog.main.to_funding_rows`: the Förderkatalog's strings become typed table rows."""
from __future__ import annotations

from datetime import date

from foerderkatalog.main import to_funding_rows


def _grant(fkz: str, **overrides: str) -> dict[str, str]:
    grant = {
        "Förderempfänger": "Beispiel GmbH",
        "Ort": "Berlin",
        "FKZ": fkz,
        "Fördersumme_EUR": "277076.00",
        "Laufzeit von": "01.10.2025",
        "Laufzeit bis": "30.09.2028",
        "Thema des geförderten Vorhabens": "Ein Thema",
        "Ressort": "BMV",
        "Förderart": "Direkte Projektförderung",
    }
    return grant | overrides


def test_types_amount_and_dates() -> None:
    (row,) = to_funding_rows([_grant("VB2508")])
    assert row == (
        "VB2508", "Beispiel GmbH", "Berlin", 277076.0,
        date(2025, 10, 1), date(2028, 9, 30), "Ein Thema", "BMV", "Direkte Projektförderung",
    )


def test_empty_fields_become_null() -> None:
    (row,) = to_funding_rows([_grant("X1", **{"Fördersumme_EUR": "", "Laufzeit bis": "", "Ressort": ""})])
    assert row[3] is None and row[5] is None and row[7] is None


def test_repeated_fkz_keeps_last_row() -> None:
    rows = to_funding_rows([_grant("X1", Ort="Alt"), _grant("X2"), _grant("X1", Ort="Neu")])
    assert [(r[0], r[2]) for r in rows] == [("X1", "Neu"), ("X2", "Berlin")]
