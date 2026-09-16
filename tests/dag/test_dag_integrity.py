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

    # Airflow 3.3 dropped the include_examples argument; example DAGs are
    # controlled by AIRFLOW__CORE__LOAD_EXAMPLES, which compose sets to false.
    return DagBag(dag_folder=str(DAG_FOLDER))


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

    # Airflow 3 exposes `schedule` and a resolved `timetable`; `schedule_interval` is
    # gone. Assert on the timetable, since that is what the scheduler actually uses -
    # `schedule` is only the string that was passed in.
    assert str(dag.schedule) == "@hourly"
    assert dag.timetable.expression == "0 * * * *", dag.timetable


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
    # dag.check_cycle() replaces the deprecated airflow.utils.dag_cycle_tester.
    for dag in dagbag.dags.values():
        dag.check_cycle()


@pytest.mark.dag
def test_bronze_asset_is_published(dagbag) -> None:
    """Downstream dbt is triggered by an Asset, not by sensing on this DAG."""
    dag = dagbag.dags["pageviews_ingest"]
    outlets = [uri for task in dag.tasks for uri in getattr(task, "outlets", [])]

    assert outlets, "no task publishes an Asset; downstream models cannot be triggered"


# ---------------------------------------------------------------- transform DAG


@pytest.mark.dag
def test_transform_dag_is_asset_triggered_not_scheduled(dagbag) -> None:
    """Transformation must react to ingestion, not run on a hopeful cron.

    Source publication latency varies by about 90 minutes (ADR 0001), so any fixed
    offset after the hour would either fire before the data exists or waste time
    waiting. An Asset dependency removes the guess entirely.
    """
    dag = dagbag.dags["pageviews_transform"]

    assert not isinstance(dag.schedule, str), (
        f"expected an Asset schedule, got the cron/preset {dag.schedule!r}"
    )
    uris = {getattr(asset, "uri", None) for asset in dag.schedule}
    assert "s3://wikiflow-lake/bronze/company_pageviews" in uris


@pytest.mark.dag
def test_ingest_and_transform_agree_on_the_asset_uri(dagbag) -> None:
    """The producing outlet and the consuming schedule must name the same URI.

    They are coupled by string, so a typo in either would not error - the transform
    DAG would simply never trigger, silently, forever.
    """
    ingest = dagbag.dags["pageviews_ingest"]
    transform = dagbag.dags["pageviews_transform"]

    produced = {getattr(outlet, "uri", None) for task in ingest.tasks for outlet in task.outlets}
    consumed = {getattr(asset, "uri", None) for asset in transform.schedule}

    assert consumed <= produced, (
        f"transform waits on {consumed - produced}, which no ingest task produces"
    )


@pytest.mark.dag
def test_transform_dag_runs_one_at_a_time(dagbag) -> None:
    """Concurrent dbt runs would contend for the same DuckDB file for no benefit."""
    assert dagbag.dags["pageviews_transform"].max_active_runs == 1


# ---------------------------------------------------------------- ops DAGs


@pytest.mark.dag
def test_all_expected_dags_exist(dagbag) -> None:
    assert set(dagbag.dags) == {
        "pageviews_ingest",
        "pageviews_transform",
        "pageviews_quality",
        "raw_retention",
    }


@pytest.mark.dag
def test_retention_is_a_separate_dag_from_ingestion(dagbag) -> None:
    """Retention must not be a cleanup task inside the ingest DAG.

    Backfill runs execute out of chronological order, so a cleanup task running inside
    a backfill of an old hour could delete data a later run still needs. Decoupling
    them makes retention depend on wall-clock time only, which is what the policy is.
    """
    ingest_tasks = set(dagbag.dags["pageviews_ingest"].task_dict)

    assert not any("reten" in t or "cleanup" in t or "purge" in t for t in ingest_tasks)
    assert "raw_retention" in dagbag.dags


@pytest.mark.dag
def test_retention_plans_before_it_deletes(dagbag) -> None:
    """The destructive task must run after the dry run that logs the manifest."""
    dag = dagbag.dags["raw_retention"]

    plan = dag.get_task("plan")
    apply_task = dag.get_task("apply")

    assert "apply" in plan.downstream_task_ids
    assert "plan" in apply_task.upstream_task_ids


@pytest.mark.dag
def test_ops_dags_alert_on_failure(dagbag) -> None:
    """An ops DAG that fails silently is worse than no ops DAG."""
    for dag_id in ("raw_retention", "pageviews_quality"):
        dag = dagbag.dags[dag_id]
        assert dag.default_args.get("on_failure_callback") is not None, dag_id


@pytest.mark.dag
def test_quality_dag_reports_all_checks_together(dagbag) -> None:
    """Failing on the first problem turns one incident into three.

    The report task must depend on every check, so a single run surfaces all of them.
    """
    dag = dagbag.dags["pageviews_quality"]
    report = dag.get_task("report")

    assert report.upstream_task_ids == {"check_gaps", "check_fresh", "check_volume"}


@pytest.mark.dag
def test_quality_freshness_threshold_exceeds_publication_lag(dagbag) -> None:
    """Measured worst-case lag is ~3h50m past the end of the covered interval.

    A threshold below that alerts during healthy operation, which trains people to
    ignore the alert.
    """
    # dags/ is not on sys.path under Airflow 3.3's bundle loader (which is why shared
    # code lives in src/wikiflow_airflow). A DAG module can still be imported directly
    # by adding the folder for the duration of this assertion.
    import sys

    sys.path.insert(0, str(DAG_FOLDER))
    try:
        import pageviews_quality as module
    finally:
        sys.path.remove(str(DAG_FOLDER))

    assert module.FRESHNESS_THRESHOLD.total_seconds() >= 4 * 3600


# ---------------------------------------------------------------- source rate limits


@pytest.mark.dag
def test_every_task_that_contacts_wikimedia_is_in_the_pool(dagbag) -> None:
    """The pool, not max_active_runs, is what bounds load on dumps.wikimedia.org.

    max_active_runs is per DAG and counts backfill, scheduled and manual runs
    separately; relying on it once let five downloads run at once against a
    3-connections-per-IP cap, and Wikimedia answered with 429s.
    """
    from wikiflow_airflow.pools import WIKIMEDIA_DUMPS_POOL

    dag = dagbag.dags["pageviews_ingest"]
    for task_id in ("wait_for_publication", "ingest"):
        assert dag.get_task(task_id).pool == WIKIMEDIA_DUMPS_POOL, task_id


@pytest.mark.dag
def test_pool_is_created_below_the_documented_connection_cap() -> None:
    """The pool is created by airflow-init; that command and the constants must agree.

    If they drift, tasks either queue forever against a pool that does not exist, or
    the cap silently rises above what Wikimedia allows.
    """
    import re

    from wikiflow_airflow.pools import (
        WIKIMEDIA_CONNECTION_CAP,
        WIKIMEDIA_DUMPS_POOL,
        WIKIMEDIA_DUMPS_SLOTS,
    )

    compose = Path("/opt/airflow/docker-compose.yml")
    if not compose.exists():
        pytest.skip("docker-compose.yml is not mounted into this container")

    match = re.search(r"airflow pools set (\S+) (\d+)", compose.read_text(encoding="utf-8"))
    assert match, "airflow-init no longer creates the pool"
    assert match.group(1) == WIKIMEDIA_DUMPS_POOL
    assert int(match.group(2)) == WIKIMEDIA_DUMPS_SLOTS
    assert WIKIMEDIA_DUMPS_SLOTS < WIKIMEDIA_CONNECTION_CAP


# ---------------------------------------------------------------- the image itself


@pytest.mark.dag
def test_dbt_is_installed_where_the_transform_dag_calls_it() -> None:
    """The transform DAG calls dbt by absolute path; that binary must exist and run.

    Parsing the DAG proves nothing about this - the DAG file is valid whether or not dbt
    is installed. This gap was real: the dbt virtualenv was added to the Dockerfile, the
    local image was never rebuilt, and the transform DAG failed with
    `/home/airflow/dbt-venv/bin/dbt: No such file or directory` the first time it ran.
    """
    import subprocess
    import sys

    sys.path.insert(0, str(DAG_FOLDER))
    try:
        import pageviews_transform as module
    finally:
        sys.path.remove(str(DAG_FOLDER))

    dbt = Path(module.DBT)
    assert dbt.exists(), f"{dbt} is missing - rebuild the image (`make up` builds)"

    result = subprocess.run([str(dbt), "--version"], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert "duckdb" in result.stdout, "the dbt-duckdb adapter is not installed"
