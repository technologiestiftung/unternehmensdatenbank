ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.2
FROM ${AIRFLOW_BASE_IMAGE}

COPY airflow/requirements.txt /tmp/requirements.txt

# CPU-only torch from the PyTorch index, alone, so that index serves no other package (requirements-model.txt).
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch>=2.5,<3"
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY airflow/requirements-model.txt /tmp/requirements-model.txt
RUN pip install --no-cache-dir -r /tmp/requirements-model.txt

# The local classifier's weights (#183), baked in and loaded once offline (the DAGs are mounted, so only
# this file is copied for the build).
ENV HF_HOME=/opt/models/hf
USER root
RUN mkdir -p /opt/models && chown airflow /opt/models
USER airflow
COPY airflow/dags/enrichment/decide.py /tmp/decide.py
RUN python /tmp/decide.py
ENV HF_HUB_OFFLINE=1

