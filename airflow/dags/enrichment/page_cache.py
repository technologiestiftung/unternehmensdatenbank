"""Keep the downloaded pages, so changing what we read out of them costs nothing.

A combined-discovery run over 1,000 companies takes about 2.4 hours, and roughly 29% of that is
spent fetching pages. Until this module existed, none of those pages were kept. That had one
consequence that shaped every decision about the pipeline: **any change to what we extract from a
page could only be evaluated by running the whole thing again.** Improving the postcode regex, or
reading a company name out of a page's structured data, meant 2.4 hours per attempt. So those
changes did not get made, and the language model kept being asked questions that a regex could have
answered.

With the pages on disk, a second run reads them back and the same work takes minutes. The scoring
side was already replayable — `results.jsonl` stores the evidence and the methods, which is
everything `confidence.score` needs — so this closes the other half.

Two things are cached, and what is NOT cached is the point:

    probe:<url>      the HTTP result — final url, status code, and the HTML
    imprint:<url>    which page turned out to be the Impressum, and its HTML

Both store raw HTML and nothing derived. Whether a page looks parked, which postcode is on it, what
its register number is — all of that is recomputed on every read. Caching a *conclusion* would
freeze the very code this cache exists to let people change.

Two backends, the same calls:

- **S3, the company corpus** (`enrichment.corpus`, issue #61): on whenever `CORPUS_S3_ENDPOINT` is set,
  which is how the DAGs run. A page is served for `CORPUS_MAX_AGE_DAYS` (90, the recheck interval) and
  then downloaded again, so a DAG sees the web of the last 90 days and every DAG shares one download.
- **A directory** (`COMBINED_PAGE_CACHE_DIR`): for measurement runs, and it wins when set. Its entries
  never expire: a measurement wants the page as it was when the run happened, so a re-scored
  comparison is against the same input. Delete the directory to start again.

Neither set: nothing is cached, and every function here is a no-op.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from enrichment import corpus

log = logging.getLogger(__name__)

#: Where to keep pages. Unset = no caching at all, and every function here becomes a no-op.
CACHE_DIR = os.getenv("COMBINED_PAGE_CACHE_DIR", "").strip()


def _directory() -> bool:
    return bool(os.getenv("COMBINED_PAGE_CACHE_DIR", "").strip())


def enabled() -> bool:
    """Is there a cache to use? Read at call time, so a test can set the variable and go."""
    return _directory() or corpus.enabled()


def _root() -> Path:
    return Path(os.getenv("COMBINED_PAGE_CACHE_DIR", "").strip())


def _path_for(namespace: str, key: str) -> Path:
    """One file per cached page, in a two-level directory.

    The file name is a hash of the URL, not the URL: a URL contains `/`, `?` and `:`, can be longer
    than a file name is allowed to be, and differs between two entries only in its query string.
    The first two hex characters become a directory so that 3,000 pages do not land in one folder,
    which is slow to list on Windows and unpleasant to open in an editor.
    """
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return _root() / namespace / digest[:2] / f"{digest}.json.gz"


def get(namespace: str, key: str) -> dict[str, Any] | None:
    """The cached record, or None for a miss.

    Never raises. A cache that fails has to behave exactly like a cache that is empty, or a corrupt
    file would take down a four-hour run.
    """
    if not _directory():
        return corpus.get(namespace, key)
    path = _path_for(namespace, key)
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return None
    except Exception as exc:  # noqa: BLE001
        log.debug("page cache unreadable for %s %s: %s", namespace, key, type(exc).__name__)
        return None


def put(namespace: str, key: str, record: dict[str, Any]) -> None:
    """Store a record. Never raises, for the same reason `get` does not.

    Written to a temporary file and moved into place, because the runner fetches with eight threads
    and a half-written file read back as a page is worse than no cache. `os.replace` is atomic on
    both Windows and Linux.
    """
    if not _directory():
        corpus.put(namespace, key, record)
        return
    path = _path_for(namespace, key)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=path.name, suffix=".tmp", delete=False
        )
        try:
            with gzip.GzipFile(fileobj=handle, mode="wb") as zipped:
                zipped.write(json.dumps(record, ensure_ascii=False).encode("utf-8"))
        finally:
            handle.close()
        os.replace(handle.name, path)
    except Exception as exc:  # noqa: BLE001
        log.debug("page cache unwritable for %s %s: %s", namespace, key, type(exc).__name__)


def stats() -> dict[str, int]:
    """How many pages are cached, per namespace. For the runner's opening log line."""
    if not _directory():
        return {}
    counts: dict[str, int] = {}
    root = _root()
    if not root.is_dir():
        return counts
    for namespace_dir in root.iterdir():
        if namespace_dir.is_dir():
            counts[namespace_dir.name] = sum(1 for _ in namespace_dir.rglob("*.json.gz"))
    return counts
