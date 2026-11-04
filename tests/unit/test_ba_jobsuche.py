"""`ba_jobsuche`: ads grouped by employer, only Berlin postcodes kept, nothing but the employer."""
from __future__ import annotations

import pytest

from ba_jobsuche import main
from ba_jobsuche.main import employers


def _ad(name: str, employer: str | None, *postcodes: str) -> dict:
    return {"firma": name, "arbeitgeberKundennummerHash": employer,
            "stellenlokationen": [{"adresse": {"plz": p, "ort": "Berlin"}} for p in postcodes]}


def test_ads_are_grouped_by_employer_with_berlin_postcodes_and_a_count() -> None:
    got = employers([_ad("EMC Adam GmbH", "h1", "13583"), _ad("EMC Adam GmbH", "h1", "13583", "14476"),
                     _ad("EMC ADAM GmbH", "h1", "10115")])
    assert got == {"h1": {"name": "EMC Adam GmbH", "postcodes": ["10115", "13583"], "ads": 3}}


def test_an_ad_without_an_employer_id_or_a_name_is_skipped() -> None:
    got = employers([_ad("Beyond Imaging Health GmbH", None, "10789"), _ad("", "h2", "10115"),
                     _ad("50Hertz Transmission GmbH", "h3", "10557")])
    assert list(got) == ["h3"]


class _Response:
    def __init__(self, status: int, body: dict | None = None) -> None:
        self.status_code, self._body = status, body or {}

    def json(self) -> dict:
        return self._body


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self.headers: dict = {}
        self._responses = responses

    def get(self, *_args, **_kwargs) -> _Response:
        return self._responses.pop(0)


def _serve(monkeypatch, *responses: _Response) -> None:
    monkeypatch.setenv("BA_JOBSUCHE_API_KEY", "client-id")
    monkeypatch.setattr(main.requests, "Session", lambda: _Session(list(responses)))
    monkeypatch.setattr(main, "PAUSE_S", 0)


def test_an_empty_key_fails_before_any_request(monkeypatch) -> None:
    monkeypatch.setenv("BA_JOBSUCHE_API_KEY", "")
    monkeypatch.setattr(main.requests, "Session", lambda: pytest.fail("no request without a key"))
    with pytest.raises(RuntimeError, match="BA_JOBSUCHE_API_KEY"):
        main.read_ads()


def test_a_refused_first_page_fails_the_run(monkeypatch) -> None:
    _serve(monkeypatch, _Response(403))
    with pytest.raises(RuntimeError, match="403"):
        main.read_ads()


def test_a_refused_later_page_keeps_what_was_read(monkeypatch) -> None:
    _serve(monkeypatch, _Response(200, {"maxErgebnisse": 3, "ergebnisliste": [_ad("A GmbH", "h1")]}),
           _Response(400))
    ads, total = main.read_ads()
    assert (len(ads), total) == (1, 3)


class _Cursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc) -> None:
        pass

    def execute(self, sql: str, _params=None) -> None:
        self.statements.append(sql)


class _Connection:
    def __init__(self) -> None:
        self.cur = _Cursor()

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_exc) -> None:
        pass

    def cursor(self) -> _Cursor:
        return self.cur

    def close(self) -> None:
        pass


def _run(monkeypatch, ads: list[dict], total: int) -> list[str]:
    connection = _Connection()
    monkeypatch.setattr(main, "read_ads", lambda: (ads, total))
    monkeypatch.setattr(main, "get_connection", lambda: connection)
    monkeypatch.setattr(main, "ensure_source", lambda *_a, **_k: "source")
    monkeypatch.setattr(main, "start_sync_run", lambda *_a, **_k: "run")
    monkeypatch.setattr(main, "finish_sync_run", lambda *_a, **_k: None)
    monkeypatch.setattr(main, "match_by_name", lambda *_a: {})
    main.run_read()
    return [sql for sql in connection.cur.statements if "is_active = false" in sql]


def test_a_partial_read_sets_no_record_inactive(monkeypatch) -> None:
    assert _run(monkeypatch, [_ad("A GmbH", "h1", "10115")], total=3) == []


def test_a_read_without_a_total_sets_no_record_inactive(monkeypatch) -> None:
    assert _run(monkeypatch, [_ad("A GmbH", "h1", "10115")], total=0) == []


def test_a_total_sent_as_text_is_read_as_a_number(monkeypatch) -> None:
    _serve(monkeypatch, _Response(200, {"maxErgebnisse": "2", "ergebnisliste": [_ad("A GmbH", "h1")]}),
           _Response(200, {"maxErgebnisse": "2", "ergebnisliste": [_ad("B GmbH", "h2")]}))
    ads, total = main.read_ads()
    assert (len(ads), total) == (2, 2)


def test_an_answer_with_no_ads_fails_the_run(monkeypatch) -> None:
    _serve(monkeypatch, _Response(200, {"maxErgebnisse": 0, "ergebnisliste": []}))
    with pytest.raises(RuntimeError, match="no Berlin ads"):
        main.read_ads()


def test_a_read_with_no_ads_sets_no_record_inactive(monkeypatch) -> None:
    assert _run(monkeypatch, [], total=0) == []


def test_a_full_read_sets_unseen_records_inactive(monkeypatch) -> None:
    assert len(_run(monkeypatch, [_ad("A GmbH", "h1", "10115")], total=1)) == 1
