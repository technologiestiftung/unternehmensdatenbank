ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.2
FROM ${AIRFLOW_BASE_IMAGE}

COPY airflow/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt
