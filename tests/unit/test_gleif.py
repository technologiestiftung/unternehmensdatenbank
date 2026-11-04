"""`gleif.main`: Berlin register entries out of GLEIF's CSV, and which file a run reads (#88)."""
from __future__ import annotations

from datetime import datetime, timezone

from gleif.main import COLUMNS, berlin_records, file_to_read, parse_row, same_name


def row(**values: str) -> dict[str, str]:
    base = {"lei": "5299000J2N45DDNE4Y28", "name": "Beispiel Software GmbH", "authority": "RA000217",
            "register": "HRB 123456 B", "status": "ACTIVE", "street": "Musterstraße 1", "postcode": "10115",
            "city": "Berlin", "updated": "2025-08-12T10:21:33Z"}
    return {COLUMNS[key]: value for key, value in (base | values).items()}


def test_a_berlin_entry_becomes_our_identifier_and_register_fields() -> None:
    record = parse_row(row())
    assert record.registers == ["Berlin (Charlottenburg) HRB 123456", "Berlin (Charlottenburg) HRB 123456 B"]
    assert record.fields() == {
        "name": "Beispiel Software GmbH", "legal_form": "GmbH", "address": "Musterstraße 1, 10115 Berlin",
        "postcode": "10115", "city": "Berlin", "register_status": "active",
    }


def test_inactive_is_dissolved_and_a_glued_b_is_read() -> None:
    record = parse_row(row(status="INACTIVE", register="HRB 1898B"))
    assert record.status == "dissolved"
    assert record.registers[0] == "Berlin (Charlottenburg) HRB 1898"


def test_other_courts_unreadable_entries_and_addresses_outside_berlin_are_left_out() -> None:
    assert parse_row(row(authority="RA000234")) is None  # Düsseldorf
    assert parse_row(row(city="Frankfurt am Main", postcode="60313")) is None
    assert parse_row(row(city="Berlin-Mitte", postcode="")) is not None
    assert parse_row(row(city="", postcode="13509")) is not None
    assert parse_row(row(register="GsR 828")) is None  # the GbR register, which we do not hold


def test_records_are_read_line_by_line_from_the_csv() -> None:
    header = ",".join(f'"{c}"' for c in COLUMNS.values())
    lines = [header + "\n",
             ",".join(f'"{v}"' for v in row().values()) + "\n",
             ",".join(f'"{v}"' for v in row(authority="RA000234").values()) + "\n"]
    assert [r.lei for r in berlin_records(iter(lines))] == ["5299000J2N45DDNE4Y28"]


def test_a_run_reads_the_change_file_that_covers_the_gap() -> None:
    now = datetime(2026, 11, 4, tzinfo=timezone.utc)
    assert file_to_read(None, now) == "full"
    assert file_to_read("2026-10-29T00:00:00+00:00", now) == "LastWeek"
    assert file_to_read("2026-10-20T00:00:00+00:00", now) == "LastMonth"
    assert file_to_read("2026-09-01T00:00:00+00:00", now) == "full"


def test_capitals_and_punctuation_do_not_make_a_new_name() -> None:
    assert same_name("eveniat GmbH", "EVENIAT GMBH")
    assert same_name("Bürgerbewegung Finanzwende e. V.", "Bürgerbewegung Finanzwende e.V.")
    assert not same_name("VOSTELL Holding UG (haftungsbeschränkt)", "VOSTELL Holding GmbH")
