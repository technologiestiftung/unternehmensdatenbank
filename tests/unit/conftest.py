"""Shared fixtures for the unit tests."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from enrichment import corpus


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
        if name not in self.objects:
            raise S3Error("NoSuchKey")
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
    monkeypatch.setattr(corpus, "MODEL_RETRY_SECONDS", 0)
    return fake
