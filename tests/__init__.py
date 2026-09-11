"""Tests for the unternehmensdatenbank pipelines.

`unit/` for tests that need nothing but the code. `integration/` for tests that read the artifacts
on disk or the database — those carry `pytest.mark.integration`, so `pytest -m "not integration"`
is the fast subset.

These `__init__.py` files are not ceremony. They make `tests.paths` and `tests.loaders` plain
package imports, they keep `unit/` in git while it is empty, and they let a future
`tests/unit/test_ground_truth_columns.py` coexist with the integration file of the same name, which
under pytest's default import mode would otherwise be a hard error.
"""
