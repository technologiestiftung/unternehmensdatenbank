from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

import requests

log = logging.getLogger(__name__)

OSM_OVERPASS_URL = os.getenv("OSM_OVERPASS_URL", "https://overpass-api.de/api/interpreter")
OSM_DISCOVERY_TIMEOUT_S = float(os.getenv("OSM_DISCOVERY_TIMEOUT_S", "30"))
OSM_DISCOVERY_REQUEST_DELAY_S = float(os.getenv("OSM_DISCOVERY_REQUEST_DELAY_S", "1.0"))

_LEGAL_SUFFIX_RE = re.compile(
    r"\b(?:GmbH\s*&\s*Co\.?\s*KG[aA]?|GmbH\s*&\s*Co\.?|GmbH|AG\s*&\s*Co\.?\s*KG"
    r"|KGaA|AG|KG|OHG|GbR|PartGmbB|PartG|UG\s*\(haftungsbeschränkt\)|UG"
    r"|e\.?\s*V\.?|e\.?\s*K\.?|SE|mbH|Ltd\.?|Inc\.?)\b",
    flags=re.IGNORECASE,
)
_WEBSITE_TAGS = ("website", "contact:website", "url", "contact:url")
_PHONE_TAGS = ("phone", "contact:phone", "contact:mobile")
_EMAIL_TAGS = ("email", "contact:email")


class OverpassError(RuntimeError):
    pass


class OverpassRateLimitError(OverpassError):
    pass


def _name_regex(company_name: str) -> str:
    cleaned = _LEGAL_SUFFIX_RE.sub("", company_name).strip()
    words = [w for w in re.split(r"[\s\-_/&+]+", cleaned) if len(w) >= 3]
    if not words:
        return re.escape(company_name[:20])
    if len(words) >= 2:
        return f"{re.escape(words[0])}.*{re.escape(words[1])}"
    return re.escape(words[0])


def _tag(tags: dict[str, str], keys: tuple[str, ...]) -> str | None:
    for k in keys:
        v = tags.get(k)
        if v:
            return v.strip()
    return None


def _to_record(element: dict[str, Any]) -> dict[str, Any]:
    tags = element.get("tags") or {}
    return {
        "osm_id": f"{element.get('type', 'node')}/{element.get('id', 0)}",
        "name": tags.get("name") or "",
        "postcode": tags.get("addr:postcode") or "",
        "city": tags.get("addr:city") or "",
        "street": tags.get("addr:street") or "",
        "housenumber": tags.get("addr:housenumber") or "",
        "website": _tag(tags, _WEBSITE_TAGS),
        "phone": _tag(tags, _PHONE_TAGS),
        "email": _tag(tags, _EMAIL_TAGS),
        "osm_category": tags.get("office") or tags.get("amenity") or tags.get("shop") or "",
    }


def search_company(
    company_name: str,
    postcode: str | None = None,
    city: str | None = None,
) -> list[dict[str, Any]]:
    if not postcode and not city:
        return []

    if OSM_DISCOVERY_REQUEST_DELAY_S > 0:
        time.sleep(OSM_DISCOVERY_REQUEST_DELAY_S)

    regex = _name_regex(company_name)

    if postcode:
        area_setup = f'area["postal_code"="{postcode}"]->.a;'
        area_ref = "(area.a)"
    else:
        safe_city = city.replace('"', '\\"')  # type: ignore[union-attr]
        area_setup = f'area["name"="{safe_city}"]["admin_level"~"^[6-9]$"]->.a;'
        area_ref = "(area.a)"

    query = (
        f'[out:json][timeout:{int(OSM_DISCOVERY_TIMEOUT_S)}];\n'
        f'{area_setup}\n'
        f'(\n'
        f'  node["name"~"{regex}",i]{area_ref};\n'
        f'  way["name"~"{regex}",i]{area_ref};\n'
        f'  relation["name"~"{regex}",i]{area_ref};\n'
        f');\n'
        f'out tags;\n'
    )

    try:
        resp = requests.post(
            OSM_OVERPASS_URL,
            data={"data": query},
            timeout=OSM_DISCOVERY_TIMEOUT_S + 5,
        )
        if resp.status_code == 429:
            raise OverpassRateLimitError("Overpass rate limit hit.")
        if resp.status_code == 504:
            raise OverpassRateLimitError("Overpass gateway timeout (overloaded).")
        resp.raise_for_status()
        elements = resp.json().get("elements") or []
        return [_to_record(e) for e in elements]
    except OverpassRateLimitError:
        raise
    except requests.exceptions.RequestException as exc:
        raise OverpassError(f"Overpass query failed: {exc}") from exc
