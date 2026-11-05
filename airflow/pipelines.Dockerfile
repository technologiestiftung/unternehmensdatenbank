# The pipelines image. On the Data Hub every DAG task is a KubernetesPodOperator pod started from
# this image (airflow/dags/pipeline_task.py), because the hub's Airflow cannot install our packages.
# It holds the DAG packages and their requirements, not Airflow: a pod runs one task and exits.
#
#   docker run --rm -e PG_DSN=... <image> website_discovery.main:run_discover
#
# Configured by environment only (see docs/release.md): PG_DSN for the writer login and each DAG's
# own settings.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/opt/pipelines
# The models' working copies. Their defaults (/opt/airflow/data/models/...) are the Airflow image's
# folders, which do not exist here and cannot be made by the task user: the first training on play
# failed with "Permission denied: '/opt/airflow'" (2026-11-05). The durable copy is S3
# (enrichment.model_store), so a folder that ends with the container is enough.
ENV BRANCH_TEXTCAT_MODEL_DIR=/tmp/models/branch_textcat MANAGER_NER_MODEL_DIR=/tmp/models/manager_ner \
    WZ_TEXTCAT_MODEL_DIR=/tmp/models/wz_textcat
WORKDIR /opt/pipelines

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dags/ /opt/pipelines/

RUN useradd --uid 50000 --no-create-home pipelines
# Where run_task leaves a task's return value. Kubernetes mounts its own folder here; Docker
# (pipeline_runner "docker") reads the file from this one.
RUN mkdir -p /airflow/xcom && chown pipelines /airflow/xcom
USER pipelines

ENTRYPOINT ["python", "-m", "run_task"]
