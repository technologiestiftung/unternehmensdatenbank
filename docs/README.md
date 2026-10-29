# Documentation

The target design and the order of the next pull requests: issue #18.

**The data**
- [data-model.md](data-model.md): the tables, how a value reaches the app, loading OffeneRegister
- [target-columns.md](target-columns.md): the 20 target columns and where each comes from
- [core-queries-current.sql](core-queries-current.sql): queries against the current schema

**Rules** (their numbers were measured on the earlier Osnabrück data; the rules still hold)
- [website-verification.md](website-verification.md): when a page belongs to a company
- [branch-classification.md](branch-classification.md): the Technologiefeld, from a company's own words
- [purpose-classification.md](purpose-classification.md): the same 77 fields, from the register purpose

**Running and shipping**
- [guards-and-tests.md](guards-and-tests.md): the free checks and what each defends
- [datahub-emulation.md](datahub-emulation.md): the Berlin Data Hub, locally
- [release.md](release.md): the migration and pipelines images, the release button
- [architecture-overview.md](architecture-overview.md): the whole setup on one page
