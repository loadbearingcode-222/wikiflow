"""Hourly ingestion of one Wikimedia pageviews dump.

This DAG is deliberately thin. Every piece of logic lives in `wikiflow.*` as plain
importable functions, so it can be unit-tested without a scheduler; the DAG only
wires those functions to a schedule and to storage.

The incremental contract, in one place:

* There is **no stored watermark**. `data_interval_start` IS the watermark, because
  the source is immutable and partitioned by time.
* Every task writes exactly **one** partition and replaces it wholesale. There is no
  append path, so a re-run cannot double-count.
* Backfill uses this same code. There is no separate backfill script.

Load on dumps.wikimedia.org is bounded by the `wikimedia_dumps` pool, not by
`max_active_runs`. That distinction was learned the hard way: max_active_runs is per DAG
and counts backfill, scheduled and manual runs separately, so five downloads once ran at
once against a source that caps clients at 3 connections per IP - and got `429 Too Many
Requests` for it. See `wikiflow_airflow.pools`.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pendulum
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import dag, get_current_context, task

from wikiflow.audit import IngestionAudit, write_audit
from wikiflow.config import get_settings
from wikiflow.download import DownloadResult, download_dump
from wikiflow.extract import ExtractionResult, extract_hour
from wikiflow.redirects import resolve_filter
from wikiflow.reference import FilterCounts, load_current, publish
from wikiflow.reprocess import publish_bronze
from wikiflow.source import HourlyDump
from wikiflow.storage import overwrite_partition
from wikiflow_airflow.assets import BRONZE_COMPANY_PAGEVIEWS, BRONZE_DOMAIN_TOTALS
from wikiflow_airflow.pools import WIKIMEDIA_DUMPS_POOL

COMPANY_SEED = Path("/opt/airflow/seeds/companies.csv")

# Publication latency was measured at 1h20m-2h50m after the labelled hour, which is
# 2h20m-3h50m after the end of the interval the file actually covers (ADR 0001). Five
# hours leaves real headroom without waiting forever on a genuinely missing hour.
SENSOR_TIMEOUT = timedelta(hours=5)
POKE_INTERVAL = timedelta(minutes=10)


def _dump_is_published(url: str) -> bool:
    """HEAD the dump URL. True once it exists.

    A 404 is the normal state for a recent hour, not an error, so it must not raise:
    raising here would burn a retry instead of waiting.
    """
    settings = get_settings()
    try:
        response = httpx.head(
            url,
            headers={"User-Agent": settings.user_agent},
            timeout=30.0,
            follow_redirects=True,
        )
    except httpx.HTTPError:
        # Transient network trouble. Treat as "not yet" and poke again.
        return False
    return response.status_code == 200


log = logging.getLogger(__name__)


def _record_audit(
    *,
    dump: HourlyDump,
    started_at: datetime,
    status: str,
    error: str | None,
    downloaded: DownloadResult | None,
    extracted: ExtractionResult | None,
    filter_snapshot: str,
) -> None:
    """Persist one audit row. Never raises.

    Called from a `finally`, so an exception here would replace the real failure with
    an audit-writing failure - hiding the thing the record exists to explain.
    """
    try:
        context = get_current_context()
        task_instance = context.get("ti")
        record = IngestionAudit(
            run_id=str(context.get("run_id", "unknown")),
            dag_id="pageviews_ingest",
            task_id="ingest",
            filter_snapshot=filter_snapshot,
            try_number=int(getattr(task_instance, "try_number", 1) or 1),
            interval_start=dump.interval_start,
            partition_date=dump.partition_date,
            partition_hour=int(dump.partition_hour),
            source_url=dump.pageviews_url,
            status=status,
            error=error,
            compressed_bytes=downloaded.compressed_bytes if downloaded else 0,
            decompressed_bytes=downloaded.decompressed_bytes if downloaded else 0,
            line_count=downloaded.line_count if downloaded else 0,
            sha256=downloaded.sha256 if downloaded else "",
            rows_scanned=extracted.rows_scanned if extracted else 0,
            rows_malformed=extracted.rows_malformed if extracted else 0,
            company_rows=extracted.company_rows if extracted else 0,
            company_views=extracted.company_views if extracted else 0,
            domain_rows=extracted.domain_rows if extracted else 0,
            total_views=extracted.total_views if extracted else 0,
            started_at=started_at,
            finished_at=datetime.now(UTC),
        )
        key = write_audit(record, dump)
        log.info("audit record written: %s (status=%s)", key, status)
    except Exception:
        log.exception("could not write the audit record; continuing")


@dag(
    dag_id="pageviews_ingest",
    description="Ingest one hourly Wikimedia pageviews dump into two bronze grains",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=2,
    default_args={
        "owner": "wikiflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["wikiflow", "ingestion", "bronze"],
    doc_md=__doc__,
)
def pageviews_ingest() -> None:
    @task
    def resolve_dump(data_interval_start: datetime) -> dict[str, str]:
        """Turn the data interval into concrete URLs and partition keys."""
        dump = HourlyDump.for_interval(data_interval_start)
        return {
            "interval_start": dump.interval_start.isoformat(),
            "pageviews_url": dump.pageviews_url,
            "projectviews_url": dump.projectviews_url,
            "partition_path": dump.partition_path,
            "raw_key": dump.raw_key("pageviews"),
        }

    wait_for_publication = PythonSensor(
        task_id="wait_for_publication",
        python_callable=lambda **context: _dump_is_published(
            HourlyDump.for_interval(context["data_interval_start"]).pageviews_url
        ),
        # Reschedule mode releases the worker slot between pokes. Without it, a
        # backfill of N hours would hold N slots doing nothing but sleeping, and the
        # pipeline would deadlock against max_active_runs.
        #
        # A deferrable sensor would additionally avoid the scheduler round-trip per
        # poke. At one DAG and a 10-minute interval that saving is negligible, so the
        # simpler option wins; the triggerer is running if this ever needs upgrading.
        mode="reschedule",
        # Each poke is a HEAD request to dumps.wikimedia.org, so it counts against the
        # same 3-connection cap as the downloads. In reschedule mode the slot is only
        # held for the seconds a poke takes, not for the whole wait.
        pool=WIKIMEDIA_DUMPS_POOL,
        poke_interval=int(POKE_INTERVAL.total_seconds()),
        timeout=int(SENSOR_TIMEOUT.total_seconds()),
        # A missing hour should fail loudly rather than retry for another 5 hours.
        retries=0,
    )

    @task
    def ensure_company_filter() -> dict[str, object]:
        """Make sure a published company filter exists, and report its size.

        The filter is resolved daily by company_pages_refresh, not here: with redirects
        it takes a few hundred MediaWiki requests (wikiflow.reference). On a fresh stack
        nothing has been published yet, so the first run resolves and publishes it
        rather than failing - the same code the refresh DAG runs.
        """
        current = load_current()
        if current is None:
            log.info("no published company filter yet; resolving and publishing one")
            publish(resolve_filter(COMPANY_SEED))
            current = load_current()
            assert current is not None
        counts = FilterCounts.of(current.pages)
        return {
            "snapshot": current.snapshot_key,
            "articles": counts.articles,
            "rows": counts.total,
        }

    # Two sequential downloads per task, one connection at a time. The pool - not
    # max_active_runs - is what bounds concurrent connections across backfill,
    # scheduled and manual runs together. See wikiflow_airflow.pools.
    @task(pool=WIKIMEDIA_DUMPS_POOL)
    def ingest(spec: dict[str, str], filter_counts: dict[str, object]) -> dict[str, Any]:
        """Download, verify, land raw, extract both grains, publish bronze.

        Writes an audit record on the way out whether or not it succeeded. A failed
        attempt is exactly the case the audit table exists for, so recording only
        successes would leave the table useless during an incident.
        """
        settings = get_settings()
        dump = HourlyDump.for_interval(pendulum.parse(spec["interval_start"]))
        # Read here rather than passed through XCom: ~9,000 rows is too much to push
        # through the metadata database on every run.
        published = load_current()
        if published is None or not published.pages:
            raise ValueError("the published company filter is missing or empty")
        pages = published.pages

        started_at = datetime.now(UTC)
        downloaded: DownloadResult | None = None
        extracted: ExtractionResult | None = None
        status, error = "failed", None

        try:
            # Scratch only. Object storage is the durable location; anything written
            # to the worker's local disk disappears when the container restarts.
            with tempfile.TemporaryDirectory(prefix="wikiflow-ingest-") as tmpdir:
                workdir = Path(tmpdir)

                downloaded = download_dump(dump.pageviews_url, workdir / dump.pageviews_filename)

                # Land the bytes exactly as received, before any parsing.
                overwrite_partition(
                    downloaded.path,
                    bucket=settings.raw_bucket,
                    partition_prefix=f"pageviews/{dump.partition_path}/",
                    key=dump.raw_key("pageviews"),
                )

                projectviews = download_dump(
                    dump.projectviews_url,
                    workdir / dump.projectviews_filename,
                    compressed=False,
                )
                overwrite_partition(
                    projectviews.path,
                    bucket=settings.raw_bucket,
                    partition_prefix=f"projectviews/{dump.partition_path}/",
                    key=dump.raw_key("projectviews"),
                )

                extracted = extract_hour(
                    source=str(downloaded.path),
                    dump=dump,
                    company_pages=pages,
                    output_dir=workdir / "bronze",
                    expected_lines=downloaded.line_count,
                )

                publish_bronze(extracted, dump)

            status = "success"
            return {
                "partition": dump.partition_path,
                "compressed_bytes": downloaded.compressed_bytes,
                "sha256": downloaded.sha256,
                "rows_scanned": extracted.rows_scanned,
                "rows_malformed": extracted.rows_malformed,
                "company_rows": extracted.company_rows,
                "company_views": extracted.company_views,
                "domain_rows": extracted.domain_rows,
                "total_views": extracted.total_views,
                "filter_snapshot": published.snapshot_key,
            }
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            _record_audit(
                dump=dump,
                started_at=started_at,
                status=status,
                error=error,
                downloaded=downloaded,
                extracted=extracted,
                filter_snapshot=published.snapshot_key,
            )

    @task(outlets=[BRONZE_COMPANY_PAGEVIEWS, BRONZE_DOMAIN_TOTALS])
    def verify(stats: dict[str, Any]) -> dict[str, Any]:
        """Gate the partition before anything downstream can see it.

        Zero company rows is treated as a failure rather than a quiet success: real
        traffic to these articles never stops, so an empty narrow grain means the
        filter broke, not that the world lost interest.
        """
        if stats["company_rows"] == 0:
            raise ValueError(
                f"partition {stats['partition']} produced zero company rows. "
                "The filter or the Wikidata resolution is broken."
            )
        if stats["total_views"] == 0:
            raise ValueError(f"partition {stats['partition']} produced zero total views")
        return stats

    spec = resolve_dump()
    wait_for_publication >> spec
    verify(ingest(spec, ensure_company_filter()))


pageviews_ingest()
