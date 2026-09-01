#!/usr/bin/env python3
"""First-contact check for exa_http: does the request shape match Exa's live API?

Worth running before the DAG, because a wrong request shape fails QUIETLY — it returns an empty
result list that is indistinguishable from "the search found nothing". That is exactly how a
malformed Parallel REST call went unnoticed for weeks.

Runs on the host. `scripts/` is mounted into the containers, but read-only
(`./scripts:/opt/airflow/scripts:ro`), and the key lives in the host's `.env`.

    python scripts/smoke_exa_client.py

It loads `.env` itself so the key never has to be typed onto a command line.
"""
from __future__ import annotations

import os
import sys
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
        sys.path.insert(0, str(base / "exa_search_discovery"))
        break

import exa_http  # noqa: E402

QUERY = '"Zaunkontor GmbH" Osnabrück Impressum'


def main() -> int:
    print(f"type={exa_http.EXA_SEARCH_TYPE} mode={exa_http.EXA_CONTENT_MODE} "
          f"num={exa_http.EXA_NUM_RESULTS}")
    response = exa_http.search([QUERY])

    print(f"hits={len(response.hits)}  cost_usd={response.cost_usd}")
    if not response.hits:
        print("EMPTY — suspect a request-shape mismatch before concluding 'no results'.")
        return 1
    for hit in response.hits[:5]:
        print(f"  {len(hit.text):5d} chars  {hit.url[:70]}")
        print(f"                 {hit.title[:70]}")
    if response.cost_usd is None:
        print("NOTE: no costDollars in the response — the cost field moved or was omitted.")
    if all(not h.text for h in response.hits):
        print("NOTE: every hit has empty text — contents payload may not be taking effect.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
