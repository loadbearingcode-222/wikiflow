"""Rebuild the dbt marts whenever new bronze data lands.

Scheduling is Asset-driven rather than time-driven: this DAG runs when
`pageviews_ingest` publishes a partition, not on a cron that hopes ingestion finished.
That matters here because publication latency for the source dump varies by an hour and
a half (ADR 0001), so any fixed offset would be either wasteful or wrong.

`dbt build` is used rather than `dbt run` followed by `dbt test`. build interleaves them
model by model, so a failing test stops its dependents from being built on top of data
already known to be wrong.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from wikiflow_airflow.assets import BRONZE_COMPANY_PAGEVIEWS, MARTS_READY

DBT_DIR = "/opt/airflow/dbt"

# dbt lives in its own virtualenv in the image - see the Dockerfile for why. Invoked by
# absolute path so nothing depends on PATH ordering inside the task.
DBT = "/home/airflow/dbt-venv/bin/dbt"

COMMON_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"


@dag(
    dag_id="pageviews_transform",
    description="Build and test the dbt marts from bronze",
    schedule=[BRONZE_COMPANY_PAGEVIEWS],
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    # The marts are rebuilt from whatever bronze currently holds, so concurrent runs
    # would contend for the same DuckDB file with no benefit.
    max_active_runs=1,
    default_args={
        "owner": "wikiflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["wikiflow", "dbt", "marts"],
    doc_md=__doc__,
)
def pageviews_transform() -> None:
    # dbt_packages/ is gitignored, so a fresh container has no dbt_utils until this
    # runs. Cheap and idempotent - it is a no-op once the lock file is satisfied.
    install_packages = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT} deps {COMMON_FLAGS}",
    )

    # Models and their tests, interleaved. A failed test blocks its dependents.
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {COMMON_FLAGS} --target prod",
        outlets=[MARTS_READY],
    )

    # Generates the lineage graph and column-level docs. Not required for correctness,
    # but it is the artefact that makes the model layer legible to anyone else.
    generate_docs = BashOperator(
        task_id="dbt_docs_generate",
        # A separate target path is required, not tidy. `docs generate` rewrites
        # run_results.json, and in ITS copy every test reads status "success" - meaning
        # compiled, not passed. Sharing the default path erased dbt build's real test
        # verdicts, and the dashboard showed a reconciliation status with no verdict in it.
        bash_command=f"{DBT} docs generate {COMMON_FLAGS} --target prod --target-path target/docs",
    )

    install_packages >> build >> generate_docs


pageviews_transform()
