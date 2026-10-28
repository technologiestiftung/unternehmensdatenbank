"""Find the Impressum page that actually carries verification data.

Thin wrapper over `url_discovery.http_client` + `url_discovery.imprint_extractor`, used by the
search-provider DAGs. It exists because of one measured shortfall, not because main's
extraction is wrong — `extract_from_html` and `score_candidate` are reused untouched.

`enrichment.http_client.fetch_impressum()` accepts the first page that looks like an Impressum,
and that page need not carry the register number. When it was written (it then accepted almost
every homepage, since fixed), measured over 8 company sites with known-good Impressum pages:

    register number found  --  fetch_impressum 0/8  --  explicit path probe 3/8

That matters because `scorer.score_candidate()` gives +100 (→ confidence 1.0) only for a
register-number match, and +20 for a postcode. With homepages, the top rung of the confidence
ladder is unreachable and every website observation lands in the pattern-only band 0.365–0.65.

Strategy here: try main's `fetch_impressum()` first, so its link-following still wins when it
works; only when the result yields no register number do we probe the explicit paths. So the
extra requests are spent exactly where main's behaviour falls short.

main's `url_discovery` is deliberately left as-is — this module changes nothing for it.
"""
from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urljoin


from enrichment.http_client import HEADERS, fetch_impressum, session
from enrichment.imprint_extractor import extract_from_html


log = logging.getLogger(__name__)

#: Probed in order, only when main's fetch_impressum produced no register number. Kept short
#: on purpose: each entry is an extra GET per candidate URL.
PROBE_PATHS = tuple(
    p.strip()
    for p in os.getenv("IMPRINT_PROBE_PATHS", "/impressum/,/impressum,/imprint,/impressum.html").split(",")
    if p.strip()
)
IMPRINT_PROBE_TIMEOUT_S = float(os.getenv("IMPRINT_PROBE_TIMEOUT_S", "12"))
IMPRINT_PROBE_ENABLED = os.getenv("IMPRINT_PROBE_ENABLED", "true").lower() == "true"


def _quality(extracted: dict[str, Any]) -> tuple[int, int, int]:
    """Rank extractions: a register number first, then a VAT id, then any named person.

    Only `register_number` and `postcode` influence score_candidate(), but vat_id and
    ceo_names are what make a page worth keeping when no register number exists anywhere.
    """
    return (
        1 if extracted.get("register_number") else 0,
        1 if extracted.get("vat_id") else 0,
        1 if extracted.get("ceo_names") else 0,
    )


def best_imprint(
    base_url: str, homepage: str | None = None
) -> tuple[str, str, dict[str, Any]] | None:
    """Return (html, imprint_url, extracted) for the most informative Impressum page found.

    None when nothing could be fetched at all. Never raises on network problems — a candidate
    that cannot be reached is simply not verifiable.

    `homepage` is the already-downloaded HTML of `base_url`, if the caller has it. Handing it over
    saves one GET per candidate; see `http_client.fetch_impressum`. Callers who do not have it pass
    nothing and the behaviour is unchanged.
    """
    best: tuple[tuple[int, int, int], str, str, dict[str, Any]] | None = None

    try:
        got = fetch_impressum(base_url, homepage=homepage)
    except Exception as exc:  # noqa: BLE001
        log.debug("fetch_impressum failed for %s: %s", base_url, type(exc).__name__)
        got = None
    if got:
        html, imprint_url = got
        extracted = extract_from_html(html)
        best = (_quality(extracted), html, imprint_url, extracted)

    # Main's result already verifies, or probing is switched off — stop here.
    if not IMPRINT_PROBE_ENABLED or (best and best[0][0]):
        return (best[1], best[2], best[3]) if best else None

    # Against the SITE, not against whatever page was handed in. `search_discovery_runner` passes
    # the search engine's hit, which is usually deep, and `/impressum` glued onto
    # `/planen/service/kontakt` is a 404 every time. Same bug, and same fix, as
    # `url_discovery.http_client._find_impressum_link`.
    for path in PROBE_PATHS:
        candidate = urljoin(base_url, path)
        if best and candidate == best[2]:
            continue
        try:
            response = session().get(candidate, headers=HEADERS, timeout=IMPRINT_PROBE_TIMEOUT_S)
        except Exception:  # noqa: BLE001  an unreachable path is the expected outcome
            continue
        if response.status_code != 200:
            continue
        extracted = extract_from_html(response.text)
        quality = _quality(extracted)
        if best is None or quality > best[0]:
            best = (quality, response.text, response.url, extracted)
        if quality[0]:  # register number found — nothing better to look for
            break

    if best is None:
        return None
    return best[1], best[2], best[3]
