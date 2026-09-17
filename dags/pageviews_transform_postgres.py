"""Build the same dbt marts in Postgres - the server warehouse (ADR 0008).

Triggered by the same bronze Asset as `pageviews_transform`, and independent of it: if
Postgres is down, the DuckDB marts and the dashboard carry on, and this DAG fails on its
own. Two steps where the DuckDB DAG needs none, because Postgres cannot read Parquet in
object storage:

1. `load_bronze` copies every new or rewritten bronze partition (and Wikimedia's own
   projectviews totals, for the reconciliation test) into Postgres, using an ETag
   manifest to decide what changed - see wikiflow.postgres_load.
2. `dbt_build --target pg` builds and tests the identical models.

Whether the two warehouses then agree is checked by `python -m wikiflow.parity`.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from wikiflow.postgres_load import sync
from wikiflow_airflow.alerts import notify_failure
from wikiflow_airflow.assets import BRONZE_COMPANY_PAGEVIEWS, POSTGRES_MARTS_READY
from wikiflow_airflow.pools import DBT_PROJECT_POOL

DBT_DIR = "/opt/airflow/dbt"
DBT = "/home/airflow/dbt-venv/bin/dbt"

# A separate target and log path from the DuckDB DAG: the two share one project
# directory, and dbt writes run_results.json and its partial-parse cache per target
# path. Sharing it would let one warehouse's results overwrite the other's.
COMMON_FLAGS = (
    f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
    "--target pg --target-path target/postgres --log-path logs/postgres"
)


@dag(
    dag_id="pageviews_transform_postgres",
    description="Load bronze into Postgres and build the dbt marts there",
    schedule=[BRONZE_COMPANY_PAGEVIEWS],
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    # Each run loads whatever changed since the last; overlapping runs would race on
    # the same partitions for no benefit.
    max_active_runs=1,
    default_args={
        "owner": "wikiflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failure,
    },
    tags=["wikiflow", "dbt", "marts", "postgres"],
    doc_md=__doc__,
)
def pageviews_transform_postgres() -> None:
    @task
    def load_bronze() -> dict[str, int]:
        report = sync()
        return {
            "partitions_loaded": report.loaded,
            "rows_loaded": report.rows,
            "unchanged": report.unchanged,
            "missing_from_storage": report.missing_from_storage,
        }

    # No `dbt deps` here: the DuckDB DAG installs packages, and running deps from two
    # DAGs is exactly the race the dbt_project pool exists to prevent.
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {COMMON_FLAGS}",
        pool=DBT_PROJECT_POOL,
        outlets=[POSTGRES_MARTS_READY],
    )

    load_bronze() >> build


pageviews_transform_postgres()
