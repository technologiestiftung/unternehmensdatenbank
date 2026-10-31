"""`handelsregister_lookup.main`: one search's result list read, and only the exact register number used."""
from __future__ import annotations

import pytest

from handelsregister_lookup.main import entry_for, parse_results, planned_fields, search_fields, status_value


def row(n: int, court: str, name: str, seat: str, status: str, history: list[tuple[str, str]]) -> str:
    """One result entry, shaped as handelsregister.de writes it (2026-10-31), document links left out."""
    former = "".join(
        f'<tr role="row"><td class="ui-panelgrid-cell RegPortErg_HistorieZn marginLeft20 padding0Px" colspan="5">'
        f'<span class="marginLeft20 fontSize85">{i}.) {old}</span></td>'
        f'<td class="ui-panelgrid-cell RegPortErg_SitzStatus"><span class="fontSize85">{i}.) {where}</span></td></tr>'
        for i, (old, where) in enumerate(history, start=1)
    )
    return (
        f'<tr class="ui-widget-content" data-ri="{n}" role="row"><td colspan="9"><table role="grid"><tbody>'
        f'<tr role="row"><td class="ui-panelgrid-cell fontTableNameSize" colspan="5">Berlin  '
        f'<span class="fontWeightBold"> {court}  </span></td></tr>'
        f'<tr role="row"><td class="ui-panelgrid-cell paddingBottom20Px" colspan="5"><span class="marginLeft20">{name}</span></td>'
        f'<td class="ui-panelgrid-cell sitzSuchErgebnisse"><span class="verticalText">{seat}</span></td>'
        f'<td class="ui-panelgrid-cell"><span class="verticalText">{status}</span></td>'
        f'<td class="ui-panelgrid-cell" colspan="2"><div class="linksPanel"><a>AD</a><span>HD</span></div></td></tr>'
        f'<tr role="row"><td colspan="6"><table role="grid"><tbody><tr role="row"><td>Historie</td></tr></tbody></table>'
        f'<table role="grid"><tbody>{former}</tbody></table></td></tr>'
        f"</tbody></table></td></tr>"
    )


def page(*rows: str) -> str:
    head = '<thead><tr role="row"><th>Land</th><th>Gericht</th></tr></thead>'
    return f'<form id="ergebnissForm"><table role="grid">{head}<tbody>{"".join(rows)}</tbody></table></form>'


PAGE = page(
    row(0, "Amtsgericht Berlin (Charlottenburg) HRB 100001", "Beispiel Technologies GmbH", "Berlin", "aktuell",
        [("Beispiel UG (haftungsbeschränkt)", "Potsdam"), ("beispiel Technologies GmbH", "Berlin")]),
    row(1, "Amtsgericht Berlin (Charlottenburg) HRB 100001 B", "Andere GmbH", "Berlin", "gelöscht", []),
)


def test_an_entry_has_name_seat_status_and_former_names_in_order() -> None:
    first = parse_results(PAGE)[0]
    assert first.register == "Berlin (Charlottenburg) HRB 100001"
    assert (first.name, first.seat, first.status) == ("Beispiel Technologies GmbH", "Berlin", "aktuell")
    assert first.history == [("Beispiel UG (haftungsbeschränkt)", "Potsdam"), ("beispiel Technologies GmbH", "Berlin")]


def test_only_the_exact_register_number_counts() -> None:
    entries = parse_results(PAGE)
    assert entry_for("Berlin (Charlottenburg) HRB 100001", entries).name == "Beispiel Technologies GmbH"
    assert entry_for("Berlin (Charlottenburg) HRB 1000", entries) is None
    # register_announcements writes the portal's " B"; the result list may show the number without it.
    assert entry_for("Berlin (Charlottenburg) HRB 100001 B", entries).name == "Beispiel Technologies GmbH"
    # Two entries with the same number: nobody can tell which, so neither is used.
    assert entry_for("Berlin (Charlottenburg) HRB 100001", entries + entries[:1]) is None


def test_an_empty_result_is_no_entry_and_a_changed_page_is_an_error() -> None:
    assert parse_results(page()) == []
    with pytest.raises(RuntimeError):
        parse_results("<html><body>Wartungsarbeiten</body></html>")


def test_status_words_map_to_ours_and_unknown_ones_write_nothing() -> None:
    assert status_value(" aktuell ") == "active"
    assert status_value("gelöscht") == "dissolved"
    assert status_value("aktuell + Zweigniederlassung") is None
    # Closed on deletion, but also on a move to another court: not a status.
    assert status_value("Geschlossenes Registerblatt") is None
    entry = parse_results(page(row(0, "Amtsgericht Berlin (Charlottenburg) HRA 5", "X KG", "", "unbekannt", [])))[0]
    assert planned_fields(entry) == {"name": "X KG"}


def test_only_berlin_register_numbers_are_searched() -> None:
    assert search_fields("Berlin (Charlottenburg) VR 36945") == ("VR", "36945")
    assert search_fields("Hamburg HRB 192874") is None
    assert search_fields("Berlin (Charlottenburg) HRB 136057 B") == ("HRB", "136057")
