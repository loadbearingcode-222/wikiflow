"""Rebuild bronze partitions from the raw layer, without touching Wikimedia.

    python -m wikiflow.reprocess --start 2026-09-14T00 --end 2026-09-15T23

This is what the 7-day raw window exists for (ADR 0003): when extraction changes - a
parsing fix, or a bigger company filter - every hour still inside the window can be
rebuilt from object storage instead of downloaded again. It reads the raw file,
re-verifies it, extracts with the CURRENT published filter, and replaces both bronze
partitions exactly as the ingest DAG does.

Each hour writes an audit record like any other attempt, under its own dag_id, so a
partition rebuilt this way is distinguishable from one the scheduler ingested.

Run it inside the stack (`docker compose exec airflow-scheduler python -m ...`), where
object storage is reachable by service name. It does not go through Airflow, so it is
not bounded by pools - which is fine here precisely because it never contacts Wikimedia.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from wikiflow.audit import IngestionAudit, write_audit
from wikiflow.config import get_settings
from wikiflow.download import _scan_content
from wikiflow.extract import ExtractionResult, extract_hour
from wikiflow.reference import load_current
from wikiflow.source import HourlyDump
from wikiflow.storage import object_exists, overwrite_partition, s3_client
from wikiflow.wikidata import CompanyPage

log = logging.getLogger(__name__)

DAG_ID = "reprocess_from_raw"


def publish_bronze(result: ExtractionResult, dump: HourlyDump) -> None:
    """Replace both bronze partitions for one hour. Shared with the ingest DAG."""
    lake = get_settings().lake_bucket
    for grain, path in (
        ("company_pageviews", result.company_path),
        ("domain_totals", result.domain_path),
    ):
        prefix = f"bronze/{grain}/{dump.partition_path}/"
        overwrite_partition(
            path, bucket=lake, partition_prefix=prefix, key=f"{prefix}part-0.parquet"
        )


def hours(start: datetime, end: datetime) -> Iterator[datetime]:
    """Every hour from start to end, both inclusive."""
    if end < start:
        raise ValueError(f"end {end:%Y-%m-%dT%H} is before start {start:%Y-%m-%dT%H}")
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


def reprocess_hour(dump: HourlyDump, pages: list[CompanyPage], *, run_id: str) -> ExtractionResult:
    settings = get_settings()
    started_at = datetime.now(UTC)
    status, error, extracted, lines, size = "failed", None, None, 0, 0

    try:
        with tempfile.TemporaryDirectory(prefix="wikiflow-reprocess-") as tmpdir:
            local = Path(tmpdir) / dump.pageviews_filename
            s3_client().download_file(settings.raw_bucket, dump.raw_key("pageviews"), str(local))
            # Re-verified, not trusted: the malformed-line gate needs the true line count,
            # and scanning to the end re-checks the gzip CRC as it goes.
            size, lines = _scan_content(local, compressed=True)
            extracted = extract_hour(
                source=str(local),
                dump=dump,
                company_pages=pages,
                output_dir=Path(tmpdir) / "bronze",
                expected_lines=lines,
            )
            publish_bronze(extracted, dump)
        status = "success"
        return extracted
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        try:
            write_audit(
                IngestionAudit(
                    run_id=run_id,
                    dag_id=DAG_ID,
                    task_id="reprocess",
                    try_number=1,
                    interval_start=dump.interval_start,
                    partition_date=dump.partition_date,
                    partition_hour=int(dump.partition_hour),
                    source_url=f"s3://{settings.raw_bucket}/{dump.raw_key('pageviews')}",
                    status=status,
                    error=error,
                    decompressed_bytes=size,
                    line_count=lines,
                    rows_scanned=extracted.rows_scanned if extracted else 0,
                    rows_malformed=extracted.rows_malformed if extracted else 0,
                    company_rows=extracted.company_rows if extracted else 0,
                    company_views=extracted.company_views if extracted else 0,
                    domain_rows=extracted.domain_rows if extracted else 0,
                    total_views=extracted.total_views if extracted else 0,
                    started_at=started_at,
                    finished_at=datetime.now(UTC),
                ),
                dump,
            )
        except Exception:
            log.exception("could not write the audit record; continuing")


def _parse_hour(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H").replace(tzinfo=UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=_parse_hour, required=True, help="UTC hour, 2026-09-14T00")
    parser.add_argument("--end", type=_parse_hour, required=True, help="UTC hour, inclusive")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    pages = load_current()
    if not pages:
        log.error("no published company filter - run the company_pages_refresh DAG first")
        return 2

    run_id = f"manual__{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    raw_bucket = get_settings().raw_bucket
    done, skipped, failed = 0, [], []

    for interval in hours(args.start, args.end):
        dump = HourlyDump.for_interval(interval)
        if not object_exists(raw_bucket, dump.raw_key("pageviews")):
            skipped.append(dump.partition_path)
            log.warning("%s: no raw file (never ingested, or past retention)", dump.partition_path)
            continue
        try:
            result = reprocess_hour(dump, pages, run_id=run_id)
        except Exception as exc:
            failed.append(dump.partition_path)
            log.error("%s: FAILED - %s: %s", dump.partition_path, type(exc).__name__, exc)
            continue
        done += 1
        log.info(
            "%s: %d company rows, %d company views, %d total views",
            dump.partition_path,
            result.company_rows,
            result.company_views,
            result.total_views,
        )

    log.info("reprocessed %d hour(s); %d skipped; %d failed", done, len(skipped), len(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
