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


def test_a_layer_gives_a_technologiefeld_only_when_its_layers_name_one_field() -> None:
    from wirtschaftsatlas.main import single_field

    assert single_field({"Artificial Intelligence"}) == "Artificial Intelligence"
    assert single_field({"Robotics", "Drones"}) is None  # torn: the layers disagree
    assert single_field(set()) is None


def test_a_layer_ranks_between_the_models_and_the_keywords() -> None:
    from enrichment.branch_observations import DECIDED_BY_KEYWORDS, DECIDED_BY_MODEL_TIEBREAK, confidence_for
    from pipeline_coordinator.resolver import _FIELD_MIN_CONF
    from wirtschaftsatlas.main import LAYER_FIELD_CONFIDENCE

    assert _FIELD_MIN_CONF < confidence_for(DECIDED_BY_MODEL_TIEBREAK) < LAYER_FIELD_CONFIDENCE
    assert LAYER_FIELD_CONFIDENCE < confidence_for(DECIDED_BY_KEYWORDS)


class _Cursor:
    """A cursor that returns the grouped layer rows and records the drop UPDATE."""

    def __init__(self, rows: list[tuple]) -> None:
        self.rows, self.executed, self.rowcount = rows, [], 0

    def execute(self, sql: str, params: tuple) -> None:
        self.executed.append((sql, params))
        if "UPDATE" in sql:
            self.rowcount = 2  # two older layer fields the atlas no longer gives

    def fetchall(self) -> list[tuple]:
        return self.rows


def test_write_layer_fields_writes_one_field_companies_and_drops_the_rest(monkeypatch) -> None:
    """Issue #148: one field -> observation at 0.56; torn -> nothing; everything else -> confidence 0."""
    import wirtschaftsatlas.main as atlas

    calls = []

    def fake_insert(cur, **kwargs):
        calls.append(kwargs)
        return f"obs-{kwargs['company_id']}"

    monkeypatch.setattr(atlas, "insert_text_observation", fake_insert)
    cur = _Cursor([
        ("c1", "r1", ["Artificial Intelligence"], ["KI", "KI-Startups"]),
        ("c2", "r2", ["Robotics", "Drones"], ["Robotik", "Drohnen"]),
    ])
    changed = atlas.write_layer_fields(cur, source_id="s", sync_run_id="run")

    assert [c["company_id"] for c in calls] == ["c1"]  # the torn company c2 gets no field
    assert calls[0]["field_name"] == "technology_field"
    assert calls[0]["value_text"] == "Artificial Intelligence"
    assert calls[0]["confidence"] == 0.56
    assert calls[0]["metadata"] == {"layers": ["KI", "KI-Startups"]}
    update_sql, update_params = cur.executed[-1]
    assert "confidence = 0" in update_sql and update_params == ("s", ["obs-c1"])  # c1 kept, the rest dropped
    assert changed == 1 + 2
