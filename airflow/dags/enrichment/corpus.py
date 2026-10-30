"""The company corpus: every text we read about a company, stored once in S3 (issue #61).

Two jobs, one store:

1. **Download once.** Every page a DAG downloads (`page_cache`, which uses this as its backend when
   S3 is configured) is kept for `CORPUS_MAX_AGE_DAYS` (90, the recheck interval) and served to every
   other DAG from here: discovery, `website_contacts`, `website_keywords`, `manager_ner`, `wikidata`
   used to fetch the same Impressum each on their own.
2. **A corpus to train on.** `company_documents` in Postgres says which texts belong to which company:
   its web pages, a Wikipedia article, a Wikidata description. A model is trained and compared on the
   same texts instead of on whatever the web shows that day.

S3 holds gzipped JSON, one object per document, keyed by kind and a hash of the URL; a re-read
overwrites it (latest version only, decided 2026-10-30). Postgres holds the index only.

**Personal data.** Impressum pages name people with addresses, emails and phone numbers, and they are
stored in full (decided by the developer, 2026-10-30). Pipelines only: neither S3 nor
`company_documents` is exposed through Hasura. CLAUDE.md: nothing of it leaves local development
without a privacy review.

Configured by `CORPUS_S3_ENDPOINT` (host:port), `CORPUS_S3_ACCESS_KEY`, `CORPUS_S3_SECRET_KEY`,
`CORPUS_S3_BUCKET`, `CORPUS_S3_SECURE`. Without an endpoint every function here is a no-op, and a
store that fails behaves exactly like an empty one: nothing here raises.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

log = logging.getLogger(__name__)

#: The kinds of text the corpus holds; `company_documents.kind` has the same CHECK (V8).
KINDS = ("webpage", "wikipedia", "wikidata_description")

_lock = threading.Lock()
_client: Any = None
_bucket_ready = False
#: Set after S3 failed to answer: the rest of the run works without the store instead of waiting for
#: a timeout on every page (8 threads, thousands of pages).
_unreachable = False


def _setting(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def enabled() -> bool:
    """Is there an S3 store? Read at call time, so a test can set the variable and go."""
    return bool(_setting("CORPUS_S3_ENDPOINT")) and not _unreachable


def _failed(exc: Exception, what: str) -> None:
    """A missing object is a miss; anything that is not an S3 answer means S3 is not there."""
    global _unreachable
    if type(exc).__name__ == "S3Error":
        if getattr(exc, "code", "") != "NoSuchKey":
            log.debug("corpus: %s failed: %s", what, getattr(exc, "code", exc))
        return
    if not _unreachable:
        log.warning("Company corpus unreachable (%s); this run continues without it.", type(exc).__name__)
    _unreachable = True


def max_age() -> timedelta:
    return timedelta(days=int(_setting("CORPUS_MAX_AGE_DAYS", "90") or "90"))


def bucket() -> str:
    return _setting("CORPUS_S3_BUCKET", "company-corpus")


def object_key(namespace: str, key: str) -> str:
    """`webpage/ab/abcdef….json.gz`: a hash, because a URL holds `/`, `?` and may be very long."""
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"{namespace}/{digest[:2]}/{digest}.json.gz"


def _s3() -> Any:
    """One client for all threads (the minio client is thread-safe); the bucket made on first use."""
    global _client, _bucket_ready
    with _lock:
        if _client is None:
            from minio import Minio

            import urllib3

            _client = Minio(
                _setting("CORPUS_S3_ENDPOINT"),
                access_key=_setting("CORPUS_S3_ACCESS_KEY"),
                secret_key=_setting("CORPUS_S3_SECRET_KEY"),
                secure=_setting("CORPUS_S3_SECURE", "false").lower() in {"1", "true", "yes"},
                # Short timeouts and no retries: an S3 that does not answer is switched off, not waited for.
                http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=3, read=30), maxsize=16,
                                                retries=urllib3.Retry(total=0)),
            )
        if not _bucket_ready:
            if not _client.bucket_exists(bucket()):
                _client.make_bucket(bucket())
            _bucket_ready = True
        return _client


def get(namespace: str, key: str, *, fresher_than: timedelta | None = None) -> dict[str, Any] | None:
    """The stored record, or None: missing, older than `fresher_than` (default `max_age()`), or the
    store unreachable. Never raises."""
    if not enabled():
        return None
    try:
        client = _s3()
        name = object_key(namespace, key)
        stat = client.stat_object(bucket(), name)
        if stat.last_modified < datetime.now(timezone.utc) - (fresher_than or max_age()):
            return None
        response = client.get_object(bucket(), name)
        try:
            return json.loads(gzip.decompress(response.read()))
        finally:
            response.close()
            response.release_conn()
    except Exception as exc:  # noqa: BLE001  a miss and a broken store must look the same
        _failed(exc, f"reading {namespace}")
        return None


def put(namespace: str, key: str, record: dict[str, Any]) -> str | None:
    """Store a record, overwriting the previous version. Returns the object key, None on failure."""
    if not enabled():
        return None
    try:
        body = gzip.compress(json.dumps(record, ensure_ascii=False).encode("utf-8"))
        name = object_key(namespace, key)
        _s3().put_object(bucket(), name, BytesIO(body), len(body), content_type="application/gzip")
        return name
    except Exception as exc:  # noqa: BLE001
        _failed(exc, f"writing {namespace}")
        return None


def record_document(
    cur: Any, *, company_id: str, source_id: str, kind: str, url: str, text: str,
    namespace: str, language: str | None = None, read_at: Any = None,
) -> None:
    """Index one stored text as belonging to a company (`company_documents`). The text itself is in S3
    under `object_key(namespace, url)`; nothing is indexed when the corpus is off."""
    if not enabled() or not text:
        return
    if kind not in KINDS:
        raise ValueError(f"unknown kind {kind!r}")
    data = text.encode("utf-8")
    cur.execute(
        """
        INSERT INTO public.company_documents
          (company_id, source_id, kind, url, language, read_at, s3_key, sha256, bytes)
        VALUES (%s, %s, %s, %s, %s, COALESCE(%s, now()), %s, %s, %s)
        ON CONFLICT (company_id, source_id, kind, url) DO UPDATE SET
          language = EXCLUDED.language, read_at = EXCLUDED.read_at, s3_key = EXCLUDED.s3_key,
          sha256 = EXCLUDED.sha256, bytes = EXCLUDED.bytes
        """,
        (company_id, source_id, kind, url, language, read_at, object_key(namespace, url),
         hashlib.sha256(data).hexdigest(), len(data)),
    )
