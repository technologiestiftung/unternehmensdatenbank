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


# --- companies we lack (#103) ---------------------------------------------------------------

from datetime import date as _date  # noqa: E402

from berlin_zuwendungen.main import entry_for, is_new_firm  # noqa: E402

LINE = {"HRB": 203011, "HRA": 52000}


def test_only_a_number_above_the_snapshot_range_is_a_new_firm() -> None:
    assert is_new_firm("HRB 239529", LINE)
    assert not is_new_firm("HRB 150000", LINE)  # unknown but inside the snapshot: more likely a typo
    assert not is_new_firm("HRB 203011", LINE)
    assert not is_new_firm("HRB 239529", {})  # no OffeneRegister to compare with: create nothing


def test_a_recipient_becomes_a_register_entry_with_name_legal_form_and_city() -> None:
    entry = entry_for("HRB 239529", "Beispiel Technologies GmbH", "Berlin", _date(2024, 1, 1), 3, LINE)
    assert entry.registers == ["Berlin (Charlottenburg) HRB 239529", "Berlin (Charlottenburg) HRB 239529 B"]
    assert entry.fields == {"name": "Beispiel Technologies GmbH", "legal_form": "GmbH", "city": "Berlin"}
    assert entry.active and entry.updated == "2024-01-01"
    assert entry.external_id == "recipient:HRB 239529"


def test_a_recipient_inside_the_snapshot_range_is_only_linked_never_created() -> None:
    assert not entry_for("HRB 150000", "Beispiel GmbH", "Berlin", None, 1, LINE).active
