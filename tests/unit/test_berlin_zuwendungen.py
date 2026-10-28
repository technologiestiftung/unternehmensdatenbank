"""`berlin_zuwendungen.main`: only commercial-register recipients are kept, keyed for an exact link."""
from __future__ import annotations

from datetime import date

from berlin_zuwendungen.main import register_entry, to_funding_rows


def _grant(grant_id: int, recipient_id: str, **overrides: str) -> dict[str, object]:
    grant = {
        "id": grant_id, "name": "Beispiel GmbH", "geber": "Senatsverwaltung für Wirtschaft",
        "art": "Projektförderung", "jahr": "2023", "anschrift": "Weg 1, 10115 Berlin",
        "politikbereich": "Wirtschaft", "zweck": "Ein Zweck", "betrag": "71149",
        "empfaengerid": recipient_id,
    }
    return {**grant, **overrides}


def test_register_entry_matches_our_identifier_format() -> None:
    assert register_entry("hrb_084522") == "HRB 84522"
    assert register_entry("HRA_000012") == "HRA 12"
    assert register_entry("vr_027417") is None
    assert register_entry("hrb_") is None
    assert register_entry(None) is None


def test_keeps_only_register_recipients_as_typed_rows() -> None:
    rows = to_funding_rows([_grant(1, "hrb_084522"), _grant(2, "vr_027417"), _grant(3, "")])
    assert rows == [(
        "berlin-1", "Beispiel GmbH", "Berlin", 71149.0, date(2023, 1, 1), date(2023, 12, 31),
        "Ein Zweck", "Senatsverwaltung für Wirtschaft", "Projektförderung",
        "berlin_zuwendungen", "HRB 84522", "Wirtschaft",
    )]


def test_missing_year_and_amount_become_null() -> None:
    (row,) = to_funding_rows([_grant(4, "hrb_1", jahr="", betrag="", anschrift="ohne Ort")])
    assert row[2:6] == (None, None, None, None)
