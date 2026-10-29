# airflow/data

Mounted at `/opt/airflow/data`; local runtime files only, gitignored except this README. The hub has
no shared disk, so nothing that must survive between task runs may live here.

- `models/manager_ner/`: spaCy models trained by `manager_ner_train`, and `current.json`
