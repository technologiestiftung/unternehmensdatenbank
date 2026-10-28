# airflow/data

Mounted into the Airflow containers at `/opt/airflow/data`. Local runtime files only; everything here
except this README is gitignored. On the Data Hub there is no shared disk, so nothing that has to
survive between task runs may depend on this folder (issue #18).

- `models/manager_ner/`: spaCy model versions trained by `manager_ner_train`, and `current.json`
