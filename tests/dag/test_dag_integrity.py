"""DAG integrity checks.

These catch the failure mode unit tests cannot: a DAG that is syntactically fine but
breaks at parse time in the scheduler, or that is scheduled in a way which would
quietly corrupt data. They need Airflow importable, so they run in the container.

Marked `dag` rather than `unit` for that reason - `make test` skips them locally and
`make test-container` runs them, as does CI.
"""

from __future__ import annotations

from pathlib import Path

import pytest

DAG_FOLDER = Path("/opt/airflow/dags")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


@pytest.mark.dag
def test_no_import_errors(dagbag) -> None:
    """The single most valuable DAG test: does it parse at all?"""
    assert not dagbag.import_errors, f"DAG import failures: {dagbag.import_errors}"


@pytest.mark.dag
def test_expected_dags_are_present(dagbag) -> None:
    assert "pageviews_ingest" in dagbag.dags


@pytest.mark.dag
def test_every_dag_has_owner_tags_and_retries(dagbag) -> None:
    """Operational hygiene: an unowned, untagged DAG is unmaintainable."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.default_args.get("owner"), f"{dag_id} has no owner"
        assert dag.default_args.get("retries", 0) >= 1, f"{dag_id} has no retries"


@pytest.mark.dag
def test_catchup_is_explicit(dagbag) -> None:
    """Relying on the global default for catchup is how accidental 1000-run
    backfills happen. Every DAG here sets it deliberately."""
    for dag_id, dag in dagbag.dags.items():
        assert isinstance(dag.catchup, bool), f"{dag_id} has no explicit catchup"


@pytest.mark.dag
def test_ingest_dag_bounds_concurrency(dagbag) -> None:
    """Unbounded catchup against a free public service is abuse, and concurrent runs
    writing the same partition would race."""
    dag = dagbag.dags["pageviews_ingest"]

    assert dag.max_active_runs > 0
    assert dag.max_active_runs <= 4, "too many concurrent runs against dumps.wikimedia.org"


@pytest.mark.dag
def test_ingest_dag_is_hourly(dagbag) -> None:
    """The whole partition-as-watermark design assumes an hourly interval.

    HourlyDump rejects a non-hour-aligned interval, so a changed schedule would fail
    at runtime - this makes it fail at test time instead.
    """
    dag = dagbag.dags["pageviews_ingest"]
    assert str(dag.schedule_interval) in {"@hourly", "0 * * * *", "1:00:00"}, dag.schedule_interval


@pytest.mark.dag
def test_sensor_timeout_covers_observed_publication_lag(dagbag) -> None:
    """Observed lag reaches ~3h50m past the end of the covered interval (ADR 0001).

    A 1-hour timeout - the intuitive choice - would fail almost every run.
    """
    dag = dagbag.dags["pageviews_ingest"]
    sensor = dag.get_task("wait_for_publication")

    assert sensor.timeout >= 4 * 3600, "sensor timeout is shorter than observed lag"
    assert sensor.mode == "reschedule", "poke mode would hold a worker slot for hours"


@pytest.mark.dag
def test_ingest_dag_has_no_cycles(dagbag) -> None:
    from airflow.utils.dag_cycle_tester import check_cycle

    for dag in dagbag.dags.values():
        check_cycle(dag)


@pytest.mark.dag
def test_bronze_asset_is_published(dagbag) -> None:
    """Downstream dbt is triggered by an Asset, not by sensing on this DAG."""
    dag = dagbag.dags["pageviews_ingest"]
    outlets = [uri for task in dag.tasks for uri in getattr(task, "outlets", [])]

    assert outlets, "no task publishes an Asset; downstream models cannot be triggered"
