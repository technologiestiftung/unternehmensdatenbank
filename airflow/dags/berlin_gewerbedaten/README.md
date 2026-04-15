# Berlin Gewerbedaten DAG

This DAG package contains the Berlin Gewerbedaten importer adapted for this repository.

Pipeline steps:

1. fetch data from the Berlin Open Data WFS
2. enrich with official Berlin address data and Overpass OSM names
3. write `gewerbedaten.jsonl` to `/opt/airflow/data`
4. load the exported data into the local Postgres schema

Main entrypoint:

- [main.py](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/airflow/dags/berlin_gewerbedaten/main.py:1)
