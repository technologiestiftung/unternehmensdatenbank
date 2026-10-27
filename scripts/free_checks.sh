#!/bin/sh
# The free checks: ruff, the unit tests and the five rule guards. No database, no network, no
# model. One list for both callers, so they cannot drift: `.no-mistakes.yaml` runs it in a
# python:3.12-slim container, `.github/workflows/checks.yml` on the CI runner.
# Expects the packages of airflow/requirements.txt plus ruff and pytest to be installed.
set -eu

ruff check .
python -m pytest -q -p no:cacheprovider -m 'not integration'
python scripts/check_branch_classifier.py
python scripts/check_purpose_branch.py
python scripts/check_combined_discovery.py
python scripts/check_osm_match.py
python scripts/check_branch_observations.py
