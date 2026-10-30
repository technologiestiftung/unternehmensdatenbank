"""`enrichment.corpus` and the page store: a page downloaded once is served to every DAG."""
from __future__ import annotations

import gzip
import json
from datetime import datetime, timedelta, timezone

import pytest

from enrichment import corpus, http_client, page_cache


class S3Error(Exception):
    """Named like minio's, which is how the corpus tells an S3 answer from a dead connection."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, datetime]] = {}

    def bucket_exists(self, _bucket: str) -> bool:
        return True

    def stat_object(self, _bucket: str, name: str):  # noqa: ANN201
        if name not in self.objects:
            raise S3Error("NoSuchKey")
        return type("Stat", (), {"last_modified": self.objects[name][1]})()

    def get_object(self, _bucket: str, name: str):  # noqa: ANN201
        body = self.objects[name][0]
        return type("Response", (), {"read": lambda self: body, "close": lambda self: None,
                                     "release_conn": lambda self: None})()

    def put_object(self, _bucket: str, name: str, data, length: int, **_kw) -> None:  # noqa: ANN001
        self.objects[name] = (data.read(length), datetime.now(timezone.utc))


@pytest.fixture
def s3(monkeypatch):  # noqa: ANN001, ANN201
    fake = FakeS3()
    monkeypatch.setenv("CORPUS_S3_ENDPOINT", "s3.test:9000")
    monkeypatch.delenv("COMBINED_PAGE_CACHE_DIR", raising=False)
    monkeypatch.setattr(corpus, "_client", fake)
    monkeypatch.setattr(corpus, "_bucket_ready", True)
    monkeypatch.setattr(corpus, "_unreachable", False)
    return fake


def test_a_stored_record_comes_back_until_it_is_too_old(s3) -> None:  # noqa: ANN001
    corpus.put("page", "https://a.de", {"html": "<p>A</p>"})
    assert corpus.get("page", "https://a.de") == {"html": "<p>A</p>"}
    name = corpus.object_key("page", "https://a.de")
    s3.objects[name] = (s3.objects[name][0], datetime.now(timezone.utc) - timedelta(days=91))
    assert corpus.get("page", "https://a.de") is None  # older than CORPUS_MAX_AGE_DAYS: read again


def test_a_missing_object_is_a_miss_and_keeps_the_store_on(s3) -> None:  # noqa: ANN001
    assert corpus.get("page", "https://nie-gesehen.de") is None
    assert corpus.enabled()


def test_an_unreachable_store_is_switched_off_for_the_run(s3, monkeypatch) -> None:  # noqa: ANN001
    def refuse(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise ConnectionRefusedError

    monkeypatch.setattr(s3, "stat_object", refuse)
    assert corpus.get("page", "https://a.de") is None
    assert not corpus.enabled()  # no timeout per page for the rest of the run


def test_the_page_cache_uses_the_corpus_when_no_directory_is_set(s3) -> None:  # noqa: ANN001
    page_cache.put("probe", "https://b.de", {"html": "<p>B</p>", "status_code": 200})
    assert page_cache.get("probe", "https://b.de")["html"] == "<p>B</p>"
    assert json.loads(gzip.decompress(s3.objects[corpus.object_key("probe", "https://b.de")][0]))["html"] == "<p>B</p>"


def test_fetch_page_serves_a_page_discovery_probed_without_downloading_it(s3, monkeypatch) -> None:  # noqa: ANN001
    page_cache.put("probe", "https://c.de", {"html": "<p>C</p>", "status_code": 200, "final_url": "https://c.de"})
    monkeypatch.setattr(http_client, "session", lambda: pytest.fail("downloaded although stored"))
    assert http_client.fetch_page("https://c.de") == "<p>C</p>"
    assert page_cache.get("page", "https://c.de")["html"] == "<p>C</p>"  # now shared under `page` too


def test_nothing_is_indexed_without_a_corpus(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.delenv("CORPUS_S3_ENDPOINT", raising=False)
    corpus.record_document(None, company_id="x", source_id="y", kind="webpage", url="u", text="t", namespace="page")
