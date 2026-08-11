# Documentation

**Start here if you want to run something:**
[running-discovery.md](running-discovery.md) — which discovery jobs you can run today, how to
start one, and the SQL that answers "what did it find?".

- [data-model.md](data-model.md): schema overview, table responsibilities, and sync lifecycle
- [core-queries.sql](core-queries.sql): canonical query catalog for writes, reads, and field resolution
- [geospatial.md](geospatial.md): optional local Overpass and Nominatim setup for Berlin
- [berlin-gewerbedaten-pipeline.md](berlin-gewerbedaten-pipeline.md): Airflow DAG and Postgres loading flow for the Berlin Gewerbedaten importer
- [source-enrichment-dags.md](source-enrichment-dags.md): **stale** — describes Kununu and LinkedIn DAGs that no longer exist. Use [../airflow/dags/README.md](../airflow/dags/README.md) instead.
- [website-and-owner-discovery-results.md](website-and-owner-discovery-results.md): measured answer to "can AI find a company's website and owner from just a name and address?" — 100 Osnabrück companies, register number on vs off, two Mistral models, tokens per business
