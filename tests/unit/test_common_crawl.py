"""`enrichment.common_crawl` and the `common_crawl` DAG's harvest state (#56)."""
from __future__ import annotations

import json

from enrichment import common_crawl as cc
import pytest

from common_crawl.main import count_unknown_pages, next_state, own_pages, read_blocks


def _cdx(url: str, status: str = "200", mime: str = "text/html") -> str:
    entry = {"url": url, "status": status, "mime-detected": mime, "filename": "crawl-data/x.warc.gz",
             "offset": "10", "length": "20"}
    return f"de,example)/x 20260907180736 {json.dumps(entry)}"


def test_one_impressum_page_per_host_the_shortest() -> None:
    records = cc.impressum_records([
        _cdx("https://firma.de/de/impressum/kontakt"),
        _cdx("https://firma.de/impressum"),
        _cdx("https://firma.de/produkte"),
        _cdx("https://andere.de/Imprint", status="404"),
        _cdx("https://andere.de/imprint.pdf", mime="application/pdf"),
        _cdx("https://firma.com/impressum"),
        "not a cdx line",
    ])
    assert [(r.host, r.url, r.offset, r.length) for r in records] == [("firma.de", "https://firma.de/impressum", 10, 20)]


def test_register_numbers_only_from_a_page_naming_a_berlin_court() -> None:
    berlin = "<p>Amtsgericht Charlottenburg, HRB 12345 B. Mutter: HRB 999 und HRB 12345 B</p>"
    assert cc.berlin_registers(berlin) == [("HRB", "12345"), ("HRB", "999")]
    assert cc.berlin_registers("<p>Amtsgericht Hamburg, HRB 12345</p>") == []


def test_identifier_values_are_how_company_identifiers_holds_them() -> None:
    assert cc.identifier_values("HRB", "1") == ["Berlin (Charlottenburg) HRB 1", "Berlin (Charlottenburg) HRB 1 B"]


def test_the_body_after_the_warc_and_http_headers() -> None:
    raw = b"WARC/1.0\r\nWARC-Type: response\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<p>Impressum</p>"
    assert cc.warc_body(raw) == "<p>Impressum</p>"


def test_the_harvest_finishes_a_crawl_then_waits_for_the_next() -> None:
    running = {"crawl": "CC-MAIN-2026-39", "blocks": [["cdx-1.gz", 0, 1]] * 3, "next": 1}
    assert next_state(running, "CC-MAIN-2026-43") is running
    done = {**running, "next": 3}
    assert next_state(done, "CC-MAIN-2026-39") is None
    assert next_state(done, "CC-MAIN-2026-43") == {"crawl": "CC-MAIN-2026-43", "blocks": None, "next": 0}
    assert next_state(None, "CC-MAIN-2026-43") == {"crawl": "CC-MAIN-2026-43", "blocks": None, "next": 0}


def test_a_refused_block_keeps_the_blocks_before_it(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    def read_block(crawl: str, block: str) -> str:
        if block == "refused":
            raise OSError("403 Forbidden")
        return block

    monkeypatch.setattr(cc, "read_block", read_block)
    monkeypatch.setattr(cc, "impressum_records", lambda lines: [lines])
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert read_blocks(pool, "CC", ["a", "b", "refused", "c"]) == [["a"], ["b"]]
        with pytest.raises(OSError):
            read_blocks(pool, "CC", ["refused", "a"])


def test_a_page_naming_more_than_two_of_our_companies_is_a_group_page() -> None:
    matches = [("group.de", "a"), ("group.de", "b"), ("group.de", "c"),
               ("kg.de", "kg"), ("kg.de", "verwaltung"), ("kg.de", "kg")]
    assert own_pages(matches) == {"kg.de"}


def test_a_page_counts_as_unknown_only_when_none_of_its_numbers_is_ours() -> None:
    ours = "Berlin (Charlottenburg) HRB 79428 B"
    line = {"HRA": 60000, "HRB": 200000}
    pages = [
        [("HRB", "794"), ("HRB", "79428")],  # "HRB 794 28": the other reading is ours
        [("HRB", "23"), ("HRB", "239529")],  # nobody's, and one reading above the line
        [("HRA", "5000")],                   # nobody's, below the line
    ]
    assert count_unknown_pages(pages, {ours}, line) == (2, 1)
    assert count_unknown_pages(pages, {ours}, {}) == (2, 0)
