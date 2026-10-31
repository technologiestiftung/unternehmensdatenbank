"""Impressum pages in the Common Crawl that name a Berlin register number (issue #56).

The Common Crawl is a public web archive, a new crawl every few weeks. Its URL index is plain
gzipped text (CDX), sorted by reversed host name, so every `.de` page sits in one stretch of it:
37,821 blocks of 3,000 lines in CC-MAIN-2026-39, 10.6 GB. `cluster.idx` says where each block
starts. A block is read with one HTTP range request, and so is each page from the archive, so
nothing needs a disk or a library beyond `requests`.

From each block: one Impressum page per host (`impressum_records`). From each page: the register
numbers, only when it names a Berlin court (`berlin_registers`). Which company a number belongs to,
and whether the host is that company's own site, is decided elsewhere: a register number proves a
page is ABOUT a company, never that it IS its site (`enrichment.proposals`).

Pilot on 2026-10-30 (5,000 of the 33,465 Impressum hosts of one index file, #56): 94 pages with a
Berlin register number, 69 hosts of our companies, 21 companies proven by `verify_proposed`.
"""
from __future__ import annotations

import gzip
import io
import json
import re
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse

from enrichment import url_patterns
from enrichment.http_client import session
from enrichment.imprint_extractor import extract_register_numbers

DATA = "https://data.commoncrawl.org/"
COLLECTIONS = "https://index.commoncrawl.org/collinfo.json"
#: Reversed host names: every `.de` host starts with "de,".
TLD_PREFIX = "de,"
IMPRESSUM_PATH = re.compile(r"impressum|imprint", re.IGNORECASE)
#: A page names a Berlin court: Berlin's register court is the Amtsgericht Charlottenburg.
BERLIN_COURT = re.compile(r"charlottenburg|amtsgericht\s+berlin", re.IGNORECASE)
#: "HRB 12345", "HRB 12345 B": register type and number.
_REGISTER = re.compile(r"^(HRA|HRB|GnR|PR|VR|GsR)\s*(\d+)")


@dataclass(frozen=True)
class Record:
    """Where one page sits in the archive."""

    host: str
    url: str
    filename: str
    offset: int
    length: int


def _get(url: str, *, byte_range: tuple[int, int] | None = None, attempts: int = 5) -> bytes:
    """GET with backoff: the archive answers 503 "slow down" under load."""
    headers = {"Range": f"bytes={byte_range[0]}-{byte_range[0] + byte_range[1] - 1}"} if byte_range else {}
    for attempt in range(attempts):
        try:
            response = session().get(url, headers=headers, timeout=60)
            if response.status_code not in (429, 500, 502, 503, 504):
                response.raise_for_status()
                return response.content
        except (OSError, ValueError):
            if attempt == attempts - 1:
                raise
        time.sleep(2 ** attempt)
    raise OSError(f"{url}: still refused after {attempts} attempts")


def latest_crawl() -> str:
    """The newest crawl's id, e.g. "CC-MAIN-2026-43"."""
    return json.loads(_get(COLLECTIONS))[0]["id"]


def tld_blocks(crawl: str, prefix: str = TLD_PREFIX) -> list[tuple[str, int, int]]:
    """(cdx file, offset, length) of every index block holding hosts under `prefix`, in order.

    The block before the first match is included too: a block is named by its first line, so the
    first hosts of the TLD can sit at the end of the block before.
    """
    # About 100 MB, read once per crawl; iterated, not split, so it is held once.
    raw = _get(f"{DATA}cc-index/collections/{crawl}/indexes/cluster.idx")
    blocks, previous = [], None
    for line in io.BytesIO(raw):
        key, filename, offset, length, _ = line.decode().rstrip("\n").split("\t")
        block = (filename, int(offset), int(length))
        if key.startswith(prefix):
            if not blocks and previous:
                blocks.append(previous)
            blocks.append(block)
        elif blocks:
            break
        previous = block
    return blocks


def read_block(crawl: str, block: tuple[str, int, int]) -> list[str]:
    filename, offset, length = block
    raw = _get(f"{DATA}cc-index/collections/{crawl}/indexes/{filename}", byte_range=(offset, length))
    return gzip.decompress(raw).decode("utf-8", "replace").splitlines()


def impressum_records(lines: Iterable[str], tld: str = ".de") -> list[Record]:
    """One Impressum page per host from CDX lines: the shortest URL whose path names it, a 200
    answer and HTML."""
    best: dict[str, Record] = {}
    for line in lines:
        parts = line.split(" ", 2)
        if len(parts) < 3:
            continue
        try:
            entry = json.loads(parts[2])
        except ValueError:
            continue
        url = entry.get("url", "")
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if (not host.endswith(tld) or entry.get("status") != "200"
                or entry.get("mime-detected", entry.get("mime")) != "text/html"
                or not IMPRESSUM_PATH.search(parsed.path)):
            continue
        if host not in best or len(url) < len(best[host].url):
            best[host] = Record(host, url, entry["filename"], int(entry["offset"]), int(entry["length"]))
    return list(best.values())


def read_page(record: Record) -> str:
    """The page's HTML from the archive: one gzip member, a WARC header, an HTTP header, the body."""
    raw = gzip.decompress(_get(DATA + record.filename, byte_range=(record.offset, record.length)))
    return warc_body(raw)


def warc_body(raw: bytes) -> str:
    return raw.split(b"\r\n\r\n", 2)[-1].decode("utf-8", "replace")


def berlin_registers(html: str) -> list[tuple[str, str]]:
    """(type, number) of every register entry on a page that names a Berlin court; else none."""
    text = url_patterns.visible_text(html)
    if not BERLIN_COURT.search(text):
        return []
    found = []
    for entry in extract_register_numbers(text):
        match = _REGISTER.match(entry)
        if match and (match[1], match[2]) not in found:
            found.append((match[1], match[2]))
    return found


def identifier_values(register_type: str, number: str) -> list[str]:
    """How `company_identifiers` (scheme `register`) holds a Berlin entry, with and without the B."""
    base = f"Berlin (Charlottenburg) {register_type} {number}"
    return [base, f"{base} B"]
