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
    row(1, "Amtsgericht Berlin (Charlottenburg) HRB 100002 B", "Andere GmbH", "Berlin", "gelöscht", []),
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


class FakeConnection:
    """Enough of a psycopg2 connection for `run_lookups`: each `with` is one transaction."""

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_exc) -> None:  # noqa: ANN002
        pass

    def cursor(self):  # noqa: ANN201
        return self

    def close(self) -> None:
        pass


def run_with(monkeypatch, outcomes: list) -> tuple[list, list, dict]:  # noqa: ANN001
    """`run_lookups` on ten companies, each search answering with the next outcome (a page or an error)."""
    import handelsregister_lookup.main as lookup

    events: list = []
    finished: dict = {}
    companies = [(f"c{i}", f"Berlin (Charlottenburg) HRB {100001 + i}") for i in range(10)]
    answers = iter(outcomes)

    def search(register: str) -> list:
        events.append(("search", register))
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return parse_results(answer)

    monkeypatch.setattr(lookup, "get_connection", FakeConnection)
    monkeypatch.setattr(lookup, "ensure_source", lambda *_a, **_k: "source")
    monkeypatch.setattr(lookup, "start_sync_run", lambda *_a, **_k: "run")
    monkeypatch.setattr(lookup, "companies_to_look_up", lambda _cur, _source, limit: companies[:limit])
    monkeypatch.setattr(lookup, "look_up", search)
    monkeypatch.setattr(lookup.time, "sleep", lambda s: events.append(("sleep", s)))
    monkeypatch.setattr(lookup, "write_lookup", lambda _cur, **kw: len(planned_fields(kw["entry"])) if kw["entry"] else 0)
    monkeypatch.setattr(lookup, "finish_sync_run", lambda _cur, **kw: finished.update(kw))
    return events, companies, finished


def test_every_search_waits_72_seconds_first(monkeypatch) -> None:  # noqa: ANN001
    import handelsregister_lookup.main as lookup

    events, _, finished = run_with(monkeypatch, [PAGE] + [page()] * 9)
    written = lookup.run_lookups()
    searches = [i for i, e in enumerate(events) if e[0] == "search"]
    assert len(searches) == 10
    assert all(events[i - 1] == ("sleep", 72.0) for i in searches)
    assert lookup.MAX_LOOKUPS == 50
    # Only HRB 100001 is in its result list (name, city, status); the other nine write no field.
    assert written == 3
    assert finished["records_created"] == 1 and finished["records_failed"] == 0


def test_three_failures_in_a_row_end_the_run(monkeypatch) -> None:  # noqa: ANN001
    import handelsregister_lookup.main as lookup

    events, _, finished = run_with(
        monkeypatch, [ConnectionError(), ConnectionError(), page(), ConnectionError(), ConnectionError(),
                      ConnectionError(), page()],
    )
    lookup.run_lookups()
    # Two failures, a success resets the count, then three in a row stop the run: 6 searches of 10.
    assert sum(e[0] == "search" for e in events) == 6
    assert finished["records_failed"] == 5
    assert finished["records_seen"] == 10
