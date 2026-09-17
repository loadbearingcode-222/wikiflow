# Pinned to the exact Airflow patch release AND an explicit Python version so that
# the container and the local uv environment run the same interpreter (3.12).
FROM apache/airflow:3.3.1-python3.12

# curl is used by the healthchecks in docker-compose.yml.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Airflow publishes a constraints file per (airflow version, python version).
# Installing without it is the single most common cause of a broken Airflow image:
# pip is free to upgrade a transitive dependency and break the scheduler at runtime.
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.12
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" -r /tmp/requirements-airflow.txt

# dbt lives in its own virtualenv, deliberately.
# dbt-core and Airflow both pin jinja2, click and protobuf, and resolving them into a
# single environment is a recurring source of breakage. An isolated venv costs ~120MB
# and removes the conflict entirely; Airflow invokes the CLI by absolute path.
RUN python -m venv /home/airflow/dbt-venv     && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip     && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-duckdb>=1.11,<2.0" "dbt-postgres>=1.9,<2.0"

ENV DBT_VENV_BIN=/home/airflow/dbt-venv/bin

# src/ is mounted at runtime (see compose volumes) so edits do not need a rebuild;
# this just puts it on the path.
ENV PYTHONPATH=/opt/airflow/src
