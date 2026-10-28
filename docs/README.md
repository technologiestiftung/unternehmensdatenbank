# Documentation

The target design and the order of the next pull requests: issue #18.

**How the data is stored**
- [data-model.md](data-model.md): the tables, how a value moves from a source to the app, and how
  the OffeneRegister Berlin set is loaded
- [target-columns.md](target-columns.md): the 20 columns the project has to fill, and where each comes from
- [core-queries-current.sql](core-queries-current.sql): queries that work against the current schema

**Rules that still hold, from the earlier example data** (Osnabrück, removed 2026-10-28). The
rules apply unchanged; the numbers in these files were measured on that data and Berlin replaces them.
- [website-verification.md](website-verification.md): why a page is judged to belong to a company
- [branch-classification.md](branch-classification.md): the Technologiefeld, from the words a company publishes
- [purpose-classification.md](purpose-classification.md): the same 77 fields, from the register purpose

**Running and shipping**
- [guards-and-tests.md](guards-and-tests.md): the free checks, what each defends, and where they run
- [datahub-emulation.md](datahub-emulation.md): the Berlin Data Hub, locally
- [release.md](release.md): the migration image and the release button
- [geospatial.md](geospatial.md): optional local Overpass and Nominatim for Berlin
- [berlin-gewerbedaten-pipeline.md](berlin-gewerbedaten-pipeline.md): the Berlin Gewerbedaten DAG
- [architecture-overview.md](architecture-overview.md): the whole setup on one page, for Notion
