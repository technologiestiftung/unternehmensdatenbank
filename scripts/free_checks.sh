#!/bin/sh
# The free checks: ruff, the unit tests and the seven rule guards. No database, no network, no
# model. The one list for every caller, so they cannot drift: `.no-mistakes.yaml` runs it in a
# python:3.12-slim container, `.github/workflows/checks.yml` on the CI runner, and the two Claude
# hooks read the guard lines below (the edit hook) or run the whole script (the commit hook).
# Expects the packages of airflow/requirements.txt plus ruff and pytest to be installed.
set -eu

python -m ruff check .
python -m pytest -q -p no:cacheprovider
python scripts/check_branch_classifier.py
python scripts/check_purpose_branch.py
python scripts/check_branch_observations.py
python scripts/check_website_discovery.py
python scripts/check_own_site_filter.py
python scripts/check_imprint_owner_extraction.py
