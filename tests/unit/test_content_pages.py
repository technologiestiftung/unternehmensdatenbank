"""`content_pages` (#198): a careers page and one services PDF on top of the services pages, read from the store."""
from __future__ import annotations

import base64

import pytest

from enrichment import content_pages as cp

BASE = "https://example.de/"


def minimal_pdf(text: str) -> bytes:
    """A one-page PDF with `text` on it, small enough to write by hand."""
    stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R"
        b" /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % number + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1)
    out += b"".join(b"%010d 00000 n \n" % offset for offset in offsets)
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objects) + 1, xref)
    return bytes(out)


def homepage(*links: tuple[str, str]) -> str:
    return "<html><body>" + "".join(f'<a href="{href}">{label}</a>' for href, label in links) + "</body></html>"


def test_one_careers_page_comes_on_top_of_the_full_services_cap() -> None:
    html = homepage(("/leistungen", "Leistungen"), ("/service", "Service"), ("/produkte", "Produkte"),
                    ("/shop", "Shop"), ("/ueber-uns", "Über uns"), ("/karriere", "Karriere"), ("/jobs", "Jobs"))
    links = cp.links_from_homepage(html, BASE)
    assert [link.category for link in links] == ["leistungen", "leistungen", "produkte", "produkte", "karriere"]
    assert links[-1].url == "https://example.de/karriere"


def test_one_services_pdf_is_picked_and_any_other_pdf_is_not() -> None:
    html = homepage(("/datenschutz.pdf", "Datenschutz"), ("/leistungen.pdf", "Leistungsübersicht"),
                    ("/produktkatalog.pdf", "Katalog"), ("https://other.de/leistungen.pdf", "Leistungen"))
    links = cp.links_from_homepage(html, BASE)
    assert [(link.category, link.url) for link in links] == [("pdf", "https://example.de/leistungen.pdf")]


def test_a_stored_pdf_is_read_and_anything_else_reads_as_nothing() -> None:
    assert cp.pdf_text(base64.b64encode(minimal_pdf("Wir bauen Robotik-Plattformen")).decode()) == \
        "Wir bauen Robotik-Plattformen"
    assert cp.pdf_text(base64.b64encode(b"not a pdf").decode()) == ""
    assert cp.pdf_text("") == ""


class FakeResponse:
    def __init__(self, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.body, self.headers, self.status_code = body, headers or {}, 200

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def iter_content(self, size: int):  # noqa: ANN201
        for start in range(0, len(self.body), size):
            yield self.body[start:start + size]


class FakeSession:
    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self.responses, self.asked = responses, []

    def get(self, url: str, **_: object) -> FakeResponse:
        self.asked.append(url)
        return self.responses[url]


@pytest.fixture
def store(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("COMBINED_PAGE_CACHE_DIR", str(tmp_path))


def test_careers_and_pdf_text_are_read_after_the_services_pages(store, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: ANN001
    html = homepage(("/leistungen", "Leistungen"), ("/karriere", "Karriere"), ("/leistungen.pdf", "Leistungen"))
    fake = FakeSession({
        "https://example.de/leistungen": FakeResponse(b""),
        "https://example.de/karriere": FakeResponse(b""),
        "https://example.de/leistungen.pdf": FakeResponse(minimal_pdf("Signaltechnik")),
    })
    pages = {"https://example.de/leistungen": "<p>Schienenfahrzeuge</p>",
             "https://example.de/karriere": "<p>Python-Entwickler</p>"}
    monkeypatch.setattr(cp, "_fetch", lambda url: pages[url])
    monkeypatch.setattr(cp, "session", lambda: fake)

    assert cp.content_for(BASE, html).text == "Schienenfahrzeuge Python-Entwickler Signaltechnik"
    # The PDF is stored raw, so a replay reads it without a download.
    fake.asked.clear()
    assert cp.content_for(BASE, html, fetch=False).pages["pdf"] == ["Signaltechnik"]
    assert fake.asked == []


@pytest.mark.parametrize("response", [
    FakeResponse(minimal_pdf("x"), {"Content-Length": str(cp.MAX_PDF_BYTES + 1)}),  # declared too large
    FakeResponse(b"%PDF-" + b"0" * (cp.MAX_PDF_BYTES + 1)),                        # too large, undeclared
    FakeResponse(b"<html>not a pdf</html>"),                                        # not a PDF at all
])
def test_a_pdf_too_large_or_not_a_pdf_is_not_read(store, monkeypatch: pytest.MonkeyPatch, response) -> None:  # noqa: ANN001
    monkeypatch.setattr(cp, "session", lambda: FakeSession({"https://example.de/a.pdf": response}))
    assert cp._fetch_pdf("https://example.de/a.pdf") == ""
