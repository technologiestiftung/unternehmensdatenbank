"""`wirtschaftsatlas.main`: which layers are companies, how an entry is read, and that the atlas only proposes."""
from __future__ import annotations

from enrichment.confidence import METHOD_WIRTSCHAFTSATLAS, UNVERIFIED_CAP, WEBSITE_MIN_CONFIDENCE, score
from wirtschaftsatlas.main import company_layers, entries


def layer(name: str, url: str) -> dict:
    return {"name": name, "type": "GeoJSONLayer", "url": {"de": url, "en": url.replace("/de/", "/en/")}}


CONFIG = {
    "layers": [
        layer("ai", "data/wsv-live/api/de/layer/ai.geojson"),
        layer("startups", "data/wsv-live/api/de/layer/startups.geojson"),
        layer("schools", "data/wsv-live/api/de/layer/schools.geojson"),
        layer("coworking", "data/wsv-live/api/de/layer/cowork.geojson"),
        layer("networks", "data/wsv-live/api/de/layer/net.geojson"),
        {"name": "rent", "type": "GeoJSONLayer", "url": "data/wohnmarkt/rents.json"},
    ],
    "contentTree": [
        {"name": "Sectors.DigiTech.Artificial intelligence", "layerName": "ai"},
        {"name": "Location Information.Startup Metropolis.Startups", "layerName": "startups"},
        {"name": "Location Information.Living in Berlin.Schools.High schools", "layerName": "schools"},
        {"name": "Location Information.Locations.Flex space / Co-working / BC", "layerName": "coworking"},
        {"name": "Sectors.DigiTech.Associations, networks and initiatives", "layerName": "networks"},
        {"name": "Location Information.Rents", "layerName": "rent"},
        {"name": "Sectors.DigiTech", "layerName": None},
    ],
}


def test_only_company_layers_are_read() -> None:
    assert company_layers(CONFIG) == [
        ("data/wsv-live/api/de/layer/ai.geojson", "Sectors.DigiTech.Artificial intelligence"),
        ("data/wsv-live/api/de/layer/startups.geojson", "Location Information.Startup Metropolis.Startups"),
    ]


def test_an_address_served_as_a_python_literal_is_read() -> None:
    feature = {"properties": {"name": " Beispiel Technologies GmbH ", "website": "https://beispiel.example/",
                              "address": "{'street': 'Musterstr. 1', 'city': 'Berlin', 'zip': '10115'}"}}
    assert entries([feature], "Sectors.DigiTech.Artificial intelligence") == [{
        "name": "Beispiel Technologies GmbH", "street": "Musterstr. 1", "postcode": "10115", "city": "Berlin",
        "website": "https://beispiel.example/", "layer": "Sectors.DigiTech.Artificial intelligence",
    }]


def test_a_feature_without_a_name_or_with_a_broken_address_does_not_fail() -> None:
    assert entries([{"properties": {"name": ""}}], "x") == []
    [row] = entries([{"properties": {"name": "A GmbH", "address": "{broken"}}], "x")
    assert (row["postcode"], row["city"]) == ("", "")


def test_an_atlas_website_nothing_on_the_page_confirms_stays_below_the_bar() -> None:
    # The atlas only proposes, matched by name: its listing alone must never publish a website.
    verdict = score(methods=[METHOD_WIRTSCHAFTSATLAS], evidence=set(), name_uniqueness=1.0)
    assert verdict.value <= UNVERIFIED_CAP < WEBSITE_MIN_CONFIDENCE
    assert verdict.methods == [METHOD_WIRTSCHAFTSATLAS]
