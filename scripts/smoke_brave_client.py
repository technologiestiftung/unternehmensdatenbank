#!/usr/bin/env python3
"""First-contact check for brave_http: does the request shape match Brave's live API?

Worth running before the DAG, because a wrong request shape fails QUIETLY — it returns an empty
result list that is indistinguishable from "the search found nothing". That is exactly how a
malformed Parallel REST call went unnoticed for weeks. `brave_http` was written from Brave's
OpenAPI schema and has never executed against the live service, so this run doubles as contract
verification.

The DAG will not start until this has passed: on success the marker below is written, and
`search_discovery_runner.require_smoke_check` refuses the batch without it.

Runs on the host. `scripts/` is mounted into the containers, but READ-ONLY
(`./scripts:/opt/airflow/scripts:ro`), so this could be imported in there and could not write its
marker. `airflow/data` is mounted read-write and is the host side of `/opt/airflow/data`, which is
what makes the file written here the same file the task reads.

    python scripts/smoke_brave_client.py

It loads `.env` itself so the key never has to be typed onto a command line. One Brave call.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _load_env_file() -> None:
    """Read .env into os.environ without overwriting anything already set."""
    env_file = REPO / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env_file()

for base in (REPO / "airflow" / "dags", Path("/opt/airflow/dags")):
    if base.is_dir():
        sys.path.insert(0, str(base))
        sys.path.insert(0, str(base / "brave_search_discovery"))
        break

import brave_http  # noqa: E402

QUERY = '"Zaunkontor GmbH" Osnabrück Impressum'

#: The host side of the DAG's `/opt/airflow/data/brave_search_discovery_smoke_ok.json`. Kept in
#: step with `ProviderSpec.smoke_marker`, which derives the same name from the source key.
MARKER = REPO / "airflow" / "data" / "brave_search_discovery_smoke_ok.json"


def main() -> int:
    brave_http.require_brave_api_key()

    # Brave caps `q` at 400 characters / 50 words, below what a long German company name plus
    # address produces, so the query sent is not always the query asked for. Print what was sent.
    effective = brave_http._truncate_query(QUERY)
    print(f"query={effective!r}")
    if effective != QUERY:
        print(f"  TRUNCATED from {len(QUERY)} chars")
    print(f"count={brave_http.BRAVE_COUNT} country={brave_http.BRAVE_COUNTRY} "
          f"lang={brave_http.BRAVE_SEARCH_LANG}")

    response = brave_http.search([QUERY])

    # The real contract check. `_hits_from_body` reads body["web"]["results"], so a renamed or
    # restructured envelope has to be visible even when hits are non-zero.
    keys = sorted(response.raw[0]) if response.raw else []
    print(f"hits={len(response.hits)}  response_keys={keys}")

    if not response.hits:
        print("EMPTY — suspect a request-shape mismatch before concluding 'no results'.")
        print("No marker written; the DAG stays refused.")
        return 1

    for hit in response.hits[:5]:
        print(f"  {len(hit.text):5d} chars  {hit.url[:70]}")
        print(f"                 {hit.title[:70]}")

    # Brave returns snippets only — `description` plus up to five `extra_snippets`, a few hundred
    # characters — not page text. Empty text everywhere means those fields moved, not that the
    # pages are blank. No note about `cost_usd`: Brave reports no cost field at all, so None is
    # normal here and warning about it would make every healthy run look broken.
    if all(not hit.text for hit in response.hits):
        print("NOTE: every hit has empty text — `description`/`extra_snippets` may have moved.")

    MARKER.parent.mkdir(parents=True, exist_ok=True)
    MARKER.write_text(
        json.dumps(
            {
                "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "hits": len(response.hits),
                "response_keys": keys,
                "query": effective,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nwrote {MARKER.relative_to(REPO)} — brave_search_discovery may now run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
