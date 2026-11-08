"""`purpose_field.main`: column 3 from the published business purpose, weighed over usable purposes only."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone

from enrichment import branch_classifier as bc
from enrichment import branch_observations
from enrichment import purpose_branch as pb
from purpose_field import main
from purpose_field.main import classify_purposes

RAIL = "Herstellung von Schienenfahrzeugen und Stellwerk-Technik sowie alle damit verbundenen Geschäfte."
FILLER = "Die Verwaltung eigenen Vermögens und alle damit zusammenhängenden Geschäfte der Gesellschaft."
READ_AT = datetime(2026, 11, 1, tzinfo=timezone.utc)


def _taxonomy() -> bc.Taxonomy:
    return bc.taxonomy_from_rows(
        [{"name": "Bahnindustrie", "name_en": "Rail industry"}, {"name": "Photonik", "name_en": "Photonics"}],
        [("Bahnindustrie", "Schienenfahrzeugen"), ("Bahnindustrie", "Stellwerk"), ("Photonik", "Lasertechnik")],
    )


def _purposes() -> dict[str, str | None]:
    purposes: dict[str, str | None] = {f"other-{i}": FILLER for i in range(50)}
    purposes.update({"rail": RAIL, "stub": "Lasertechnik", "empty": None})
    return purposes


def test_a_clear_purpose_gets_its_field_and_a_stub_keeps_only_the_withheld_one() -> None:
    answers = classify_purposes(_purposes(), _taxonomy())
    assert (answers["rail"].outcome, answers["rail"].branch) == (pb.ASSIGNED_KEYWORDS, "Bahnindustrie")
    assert answers["other-0"].outcome == pb.NO_KEYWORD
    # Too short to read: no field, but the one it would have had is kept, never published.
    assert (answers["stub"].outcome, answers["stub"].branch) == (pb.NO_PURPOSE, None)
    assert answers["stub"].withheld_by_floor == "Photonik"
    assert (answers["empty"].outcome, answers["empty"].withheld_by_floor) == (pb.NO_PURPOSE, None)


def test_stubs_do_not_count_for_the_weights() -> None:
    with_stubs = classify_purposes({**_purposes(), **{f"stub-{i}": "Stellwerk" for i in range(40)}}, _taxonomy())
    without = classify_purposes(_purposes(), _taxonomy())
    assert with_stubs["rail"].score == without["rail"].score


class _Cursor:
    """Just enough of a psycopg2 cursor for `run_classify`: the purposes, the values shown."""

    def __init__(self, records: list[tuple], shown: list[tuple]) -> None:
        self.records, self.shown, self.rows, self.drops = records, shown, [], []

    def execute(self, sql: str, params: tuple = ()) -> None:
        if "company_canonical_field_sources" in sql:
            self.rows = self.records
        elif "UPDATE public.company_field_observations" in sql:
            self.drops.append(params)
        elif "FROM public.company_field_observations" in sql:
            self.rows = self.shown

    def fetchall(self) -> list:
        return self.rows


class _Connection:
    def __init__(self, cur: _Cursor) -> None:
        self.cur = cur

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> nullcontext:
        return nullcontext(self.cur)

    def close(self) -> None:
        return None


def _run_classify(monkeypatch, records: list[tuple], shown: list[tuple]) -> tuple[_Cursor, list, list]:
    taxonomy = _taxonomy()
    cur, inserts, source_records = _Cursor(records, shown), [], []
    monkeypatch.setattr(main.bc, "load_taxonomy", lambda: taxonomy)
    # The write path translates to English through its own cached copy of the real YAML.
    monkeypatch.setattr(branch_observations, "_taxonomy", lambda: taxonomy)
    monkeypatch.setattr(main, "MIN_COMPANIES", 10)
    monkeypatch.setattr(main, "get_connection", lambda: _Connection(cur))
    monkeypatch.setattr(main, "ensure_source", lambda *a, **k: "source")
    monkeypatch.setattr(main, "start_sync_run", lambda *a, **k: "run")
    monkeypatch.setattr(main, "finish_sync_run", lambda *a, **k: None)
    monkeypatch.setattr(main, "upsert_source_record",
                        lambda cur, **k: source_records.append(k) or (f"record-{k['company_id']}", True))
    monkeypatch.setattr(main, "insert_text_observation", lambda cur, **k: inserts.append(k))
    main.run_classify()
    return cur, inserts, source_records


def _records() -> list[tuple]:
    return [(cid, f"Firma {cid}", text, READ_AT) for cid, text in _purposes().items()]


def test_values_are_dated_by_the_register_read_and_old_values_drop_to_zero(monkeypatch) -> None:
    shown = [("rail", "purpose_field", "Photonics"), ("gone", "purpose_field", "Photonics")]
    cur, inserts, source_records = _run_classify(monkeypatch, _records(), shown)
    assert sorted((k["company_id"], k["field_name"], k["value_text"], k["confidence"]) for k in inserts) == [
        ("rail", "purpose_field", "Rail industry", 0.60),
        ("stub", "purpose_field_withheld", "Photonics", 0.60),
    ]
    assert {k["source_updated_at"] for k in inserts} == {READ_AT}
    # A source record only where something is written; no row for the 50 without a keyword.
    assert sorted(k["company_id"] for k in source_records) == ["rail", "stub"]
    # Neither the company's old value nor one of a company without a purpose now stays shown.
    assert sorted(p[0] for p in cur.drops) == ["gone", "rail"]


def test_nothing_is_written_before_enough_usable_purposes(monkeypatch) -> None:
    records = [(cid, "Firma", text, READ_AT) for cid, text in [("rail", RAIL), *[(f"s{i}", "Kurz") for i in range(20)]]]
    cur, inserts, _ = _run_classify(monkeypatch, records, [("gone", "purpose_field", "Photonics")])
    assert inserts == [] and cur.drops == []
