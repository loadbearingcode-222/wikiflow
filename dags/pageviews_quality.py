"""Warehouse health checks: is the data there, recent, and plausible?

The dbt tests answer a different question - whether the data is internally consistent
and reconciles with Wikimedia's published totals. They run on whatever arrived. This DAG
looks across the whole range instead, and catches the failures that produce no error:

* a missing hour, which the ingest DAG cannot report because a run that never happened
  raises nothing;
* ingestion having silently stopped, where everything present is correct and there is
  simply nothing new;
* an hour that arrived at the wrong size.

Checks are reported together rather than failing on the first problem, so one run tells
you everything that is wrong rather than only the first thing.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from wikiflow.quality import (
    DataQualityError,
    check_freshness,
    detect_gaps,
    detect_volume_anomalies,
)
from wikiflow_airflow.alerts import notify_failure

#: How far back to look for holes.
GAP_WINDOW = timedelta(days=3)

#: Source publication latency reaches nearly 4h past the end of the interval a file
#: covers (ADR 0001), and the sensor may wait beyond that. A threshold tighter than
#: this alerts on healthy operation, which is how alerts get ignored.
FRESHNESS_THRESHOLD = timedelta(hours=8)

#: Deliberately loose. Wikipedia traffic genuinely spikes on news events, and this
#: check exists to catch a truncated file or a half-written partition, not to detect
#: something interesting happening in the world.
SIGMA_THRESHOLD = 4.0


@dag(
    dag_id="pageviews_quality",
    description="Gap, freshness and volume checks across the bronze layer",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "wikiflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
    },
    tags=["wikiflow", "quality", "ops"],
    doc_md=__doc__,
)
def pageviews_quality() -> None:
    @task
    def check_gaps() -> dict[str, object]:
        end = pendulum.now("UTC").start_of("hour").subtract(hours=1)
        start = end - GAP_WINDOW
        report = detect_gaps(start, end)
        print(report.summary())
        return {"ok": report.is_complete, "detail": report.summary()}

    @task
    def check_fresh() -> dict[str, object]:
        report = check_freshness(max_age=FRESHNESS_THRESHOLD)
        print(report.summary())
        return {"ok": report.is_fresh, "detail": report.summary()}

    @task
    def check_volume() -> dict[str, object]:
        report = detect_volume_anomalies(sigma_threshold=SIGMA_THRESHOLD)
        print(report.summary())
        # Insufficient history is not a failure. Saying "skipped, and here is why" is
        # more useful than inventing a threshold from two data points.
        return {
            "ok": not report.anomalies or report.insufficient_history,
            "detail": report.summary(),
        }

    @task
    def report(*results: dict[str, object]) -> None:
        """Fail once, naming every check that failed.

        Failing on the first problem would mean fixing it, re-running, and discovering
        the next one - which turns one incident into three.
        """
        failures = [str(r["detail"]) for r in results if not r["ok"]]

        for result in results:
            status = "OK  " if result["ok"] else "FAIL"
            print(f"{status} {result['detail']}")

        if failures:
            raise DataQualityError(
                f"{len(failures)} of {len(results)} checks failed:\n  " + "\n  ".join(failures)
            )

        print(f"all {len(results)} checks passed")

    report(check_gaps(), check_fresh(), check_volume())


pageviews_quality()
