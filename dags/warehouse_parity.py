"""Do the DuckDB and Postgres marts still hold the same data? Checked daily.

`python -m wikiflow.parity` compares every mart row by row (ADR 0008). Run by hand, it
proves parity once. Scheduled, it catches the day a model change, a dialect macro or a
loader bug makes the two warehouses quietly diverge.

Two outcomes are deliberately different:

* **Not comparable yet** - the warehouses were built from different writes of bronze,
  because one transform DAG has not caught up. That is timing, not a defect: the task
  fails and RETRIES later.
* **Different** - built from the same bronze, but the data disagrees. That is a defect:
  the task fails immediately, without retrying, and alerts.

The task takes the `dbt_project` pool's single slot, so no dbt build in either warehouse
runs while it reads. Without that, it could read the DuckDB file mid-build (DuckDB permits
one writer, and a reader can collide with it) or catch Postgres half-rebuilt.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from wikiflow.parity import compare_warehouses, summarise
from wikiflow_airflow.alerts import notify_failure
from wikiflow_airflow.pools import DBT_PROJECT_POOL

DUCKDB_WAREHOUSE = Path("/opt/airflow/warehouse/wikiflow.duckdb")


@dag(
    dag_id="warehouse_parity",
    description="Compare every mart in DuckDB and Postgres, row by row",
    schedule="45 5 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "wikiflow",
        # Retries are for "not comparable yet" only; a real difference skips them.
        "retries": 4,
        "retry_delay": timedelta(minutes=20),
        "on_failure_callback": notify_failure,
    },
    tags=["wikiflow", "quality", "ops", "postgres"],
    doc_md=__doc__,
)
def warehouse_parity() -> None:
    @task(pool=DBT_PROJECT_POOL)
    def compare() -> dict[str, object]:
        report = compare_warehouses(DUCKDB_WAREHOUSE)
        print(summarise(report))

        if not report.comparable:
            raise RuntimeError(
                f"{report.hours_at_different_versions} hour(s) built from different bronze "
                "writes; one warehouse has not caught up. Retrying later."
            )
        if not report.identical:
            raise AirflowFailException(
                "the warehouses were built from the same bronze but their marts differ:\n"
                + summarise(report)
            )
        return {
            "marts": len(report.marts),
            "rows": sum(m.left_rows for m in report.marts),
            "identical": True,
        }

    compare()


warehouse_parity()
