# Documentation

- [data-model.md](data-model.md): schema overview, table responsibilities, and sync lifecycle
- [core-queries.sql](core-queries.sql): canonical query catalog for writes, reads, and field resolution
- [geospatial.md](geospatial.md): optional local Overpass and Nominatim setup for Berlin
- [berlin-gewerbedaten-pipeline.md](berlin-gewerbedaten-pipeline.md): Airflow DAG and Postgres loading flow for the Berlin Gewerbedaten importer
- [source-enrichment-dags.md](source-enrichment-dags.md): active DAG layout for Mistral discovery, Kununu, and LinkedIn enrichment
- [website-and-owner-discovery-results.md](website-and-owner-discovery-results.md): measured answer to "can AI find a company's website and owner from just a name and address?" — 100 Osnabrück companies, register number on vs off, two Mistral models, tokens per business
