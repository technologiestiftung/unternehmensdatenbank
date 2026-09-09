# Documentation

**Start here if you want to run something:**
[running-discovery.md](running-discovery.md) — which discovery jobs you can run today, how to
start one, and the SQL that answers "what did it find?".

- [data-model.md](data-model.md): schema overview, table responsibilities, and sync lifecycle
- [core-queries-current.sql](core-queries-current.sql): **the queries that work.** Canonical websites
  per source, the enriched view, the `company_managers` duplicate check, the `verified_by` split,
  source priorities, and the four resolver invariants. Verified against the live database (V20).
- [core-queries.sql](core-queries.sql): ⚠️ **largely outdated, do not run.** The old catalog, written
  against V1–V8. Most of it addresses ten `companies` columns that V9 dropped, and four queries use
  the observation name `website_url`, which has never existed (it is `website`). **Its query 3 is
  dangerous** — it inserts a source at priority 10, which would outrank every real source and
  re-order the resolver for every field. Kept because queries 4–35 still document the real
  observation/canonical lifecycle correctly. Read them; run nothing.
- [guards-and-tests.md](guards-and-tests.md): the seven guard scripts, the 17 pytest tests, what each
  defends, how the `PostToolUse` hook picks five of them, and the rule about changing a pinned number
- [geospatial.md](geospatial.md): optional local Overpass and Nominatim setup for Berlin
- [berlin-gewerbedaten-pipeline.md](berlin-gewerbedaten-pipeline.md): Airflow DAG and Postgres loading flow for the Berlin Gewerbedaten importer
- [source-enrichment-dags.md](source-enrichment-dags.md): **stale** — describes Kununu and LinkedIn DAGs that no longer exist. Use [../airflow/dags/README.md](../airflow/dags/README.md) instead.
- [website-and-owner-discovery-results.md](website-and-owner-discovery-results.md): measured answer to "can AI find a company's website and owner from just a name and address?" — 100 Osnabrück companies, register number on vs off, two Mistral models, tokens per business
