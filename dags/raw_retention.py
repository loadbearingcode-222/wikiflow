"""Delete raw dumps once they age out of the replay window.

Raw files cost 45-70 MB an hour - roughly 500 GB a year if kept forever. Keeping none
is worse, because the first parsing bug then costs a full re-download of every affected
hour. Seven days is the compromise (ADR 0003): long enough to fix a bug and reprocess
from object storage, which measures at about 4 seconds per hour.

**This is a separate DAG on purpose.** Retention as a cleanup task inside the ingest DAG
would misbehave during a backfill: backfill runs execute out of chronological order, so
a task that deletes "anything older than N days" could run in a backfill of an old hour
and delete data a later run still needed. Decoupling them means retention depends on
wall-clock time only, which is what the policy is actually about.

The sweep runs in dry-run mode first and logs its manifest, then deletes. Both steps
appear in the task log, so after the fact you can see exactly what was removed and
confirm it matches what was proposed.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from wikiflow.retention import sweep
from wikiflow_airflow.alerts import notify_failure


@dag(
    dag_id="raw_retention",
    description="Delete raw partitions older than the retention window",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "wikiflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": notify_failure,
    },
    tags=["wikiflow", "retention", "ops"],
    doc_md=__doc__,
)
def raw_retention() -> None:
    @task
    def plan() -> dict[str, int]:
        """Dry run. Logs exactly what the next task will delete."""
        report = sweep(dry_run=True)
        print(report.manifest())

        if report.unparseable_keys:
            # Not fatal, but it means the layout changed or something else is writing
            # into the raw bucket. Worth a look before the next sweep.
            print(
                f"WARNING: {len(report.unparseable_keys)} keys did not match the expected "
                "partition layout and will be left alone."
            )

        return {
            "partitions": report.partitions_expired,
            "objects": report.objects_deleted,
        }

    @task
    def apply(planned: dict[str, int]) -> dict[str, int]:
        """Delete for real, then report what actually went."""
        if planned["partitions"] == 0:
            print("nothing expired; skipping")
            return {"partitions": 0, "objects": 0}

        report = sweep(dry_run=False)
        print(report.manifest())

        # The two sweeps are separate listings, so a partition could appear between
        # them. A mismatch is not an error, but it should be visible rather than silent.
        if report.partitions_expired != planned["partitions"]:
            print(
                f"NOTE: planned {planned['partitions']} partitions, deleted "
                f"{report.partitions_expired}. The bucket changed between the two passes."
            )

        return {
            "partitions": report.partitions_expired,
            "objects": report.objects_deleted,
        }

    apply(plan())


raw_retention()
