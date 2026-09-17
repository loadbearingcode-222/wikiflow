"""End to end, both warehouses: bronze -> Postgres load -> dbt on DuckDB and Postgres -> parity.

Everything Phase 5 claims, checked in one place that CI runs:

1. The same dbt models build and pass every test on DuckDB and on Postgres, including
   the cross-warehouse url_decode test and the reconciliation test.
2. The two warehouses' marts are identical, row by row.
3. A REWRITTEN old hour reaches the marts through ordinary incremental builds - the gap
   that once needed a manual --full-refresh (ADR 0008).
4. The loader moves only what changed.
5. A load that fails half-way through a partition leaves Postgres exactly as it was.

Needs MinIO and the warehouse Postgres, so it runs inside the stack or in CI:

    docker compose run --rm --no-deps --entrypoint pytest airflow-scheduler \
        /opt/airflow/tests -m e2e

It works in its OWN buckets and its OWN database, and refuses to start if pointed at the
real ones - so running it next to a live pipeline cannot touch real data.

About the oracle: the committed fixture is a 4,379-line sample, not a real hour, so there
is no genuine Wikimedia projectviews file to reconcile it against. The oracle is computed
here in plain Python from the raw fixture - an implementation independent of the DuckDB
extraction it checks. The real-data reconciliation is the one the live pipeline runs.
"""

from __future__ import annotations

import gzip
import os
import shutil
import subprocess
import time
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import psycopg
import pytest

if TYPE_CHECKING:
    from wikiflow.config import Settings
    from wikiflow.wikidata import CompanyPage

pytestmark = pytest.mark.e2e

REPO = Path(__file__).resolve().parents[2]
DBT_DIR = REPO / "dbt"
FIXTURES = REPO / "tests" / "fixtures"
SAMPLE = FIXTURES / "pageviews-sample-20260914-120000.gz"

# On DIFFERENT dates, deliberately. The rule this replaced watermarked on view_date, which
# rebuilds every hour of the newest date anyway - so a rewritten hour on the same date as
# the newest one passes under the old, broken rule too. This test once did exactly that,
# until it was run against the old rule and did not fail.
HOUR_A = datetime(2026, 9, 13, 11, tzinfo=UTC)
HOUR_B = datetime(2026, 9, 14, 12, tzinfo=UTC)

ISOLATED = {
    "WIKIFLOW_LAKE_BUCKET": "wikiflow-e2e-lake",
    "WIKIFLOW_RAW_BUCKET": "wikiflow-e2e-raw",
    "WIKIFLOW_PG_DATABASE": "wikiflow_e2e",
}


def _unavailable(reason: str) -> None:
    if os.environ.get("WIKIFLOW_E2E_REQUIRED") == "1":
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """Point every setting at isolated buckets, database and DuckDB file, then clean them."""
    from wikiflow.config import get_settings

    workdir = tmp_path_factory.mktemp("e2e")
    previous = {key: os.environ.get(key) for key in [*ISOLATED, "WIKIFLOW_DUCKDB_PATH"]}
    os.environ.update(ISOLATED)
    os.environ["WIKIFLOW_DUCKDB_PATH"] = str(workdir / "e2e.duckdb")
    get_settings.cache_clear()
    settings = get_settings()

    assert settings.lake_bucket == ISOLATED["WIKIFLOW_LAKE_BUCKET"], "refusing to use real buckets"
    assert settings.pg_database == ISOLATED["WIKIFLOW_PG_DATABASE"], "refusing to use real database"

    try:
        _reset_buckets(settings.lake_bucket, settings.raw_bucket)
        _reset_database(settings)
    except Exception as exc:
        _unavailable(f"MinIO or the warehouse Postgres is not reachable: {exc}")

    yield workdir

    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    get_settings.cache_clear()


def _reset_buckets(*buckets: str) -> None:
    from wikiflow.storage import delete_prefix, s3_client

    client = s3_client()
    deadline = time.monotonic() + 60
    while True:  # MinIO has no healthcheck; it may still be starting
        try:
            existing = {b["Name"] for b in client.list_buckets()["Buckets"]}
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)
    for bucket in buckets:
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)
        for prefix in ("bronze/", "pageviews/", "projectviews/", "reference/", "ops/"):
            delete_prefix(bucket, prefix)


def _reset_database(settings: Settings) -> None:
    admin = settings.pg_conninfo.replace(f"dbname={settings.pg_database}", "dbname=postgres")
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute("drop database if exists wikiflow_e2e with (force)")
        connection.execute("create database wikiflow_e2e")


# --- building blocks ---------------------------------------------------------------------


def company_pages(exclude_qid: str | None = None) -> list[CompanyPage]:
    import csv

    from wikiflow.wikidata import CompanyPage

    with (FIXTURES / "company_pages.csv").open(newline="", encoding="utf-8") as handle:
        pages = [
            CompanyPage(
                qid=row["qid"],
                company_name=row["company_name"],
                sitelink_key=row["sitelink_key"],
                language=row["language"] or None,
                project=row["project"],
                page_title=row["page_title"],
                domain_code=row["domain_code"],
                access_method=row["access_method"],
            )
            for row in csv.DictReader(handle)
        ]
    return [p for p in pages if p.qid != exclude_qid]


def ingest_from_fixture(interval: datetime, pages: list[CompanyPage], workdir: Path) -> None:
    """What the ingest task does after downloading: land raw, extract, publish bronze."""
    from wikiflow.config import get_settings
    from wikiflow.extract import extract_hour
    from wikiflow.reprocess import publish_bronze
    from wikiflow.source import HourlyDump
    from wikiflow.storage import upload_file

    dump = HourlyDump.for_interval(interval)
    out = workdir / f"{interval:%H}-{time.monotonic_ns()}"
    out.mkdir()
    result = extract_hour(
        source=str(SAMPLE), dump=dump, company_pages=pages, output_dir=out, expected_lines=4379
    )
    publish_bronze(result, dump)

    # The reconciliation oracle, computed independently of DuckDB.
    totals: Counter[str] = Counter()
    with gzip.open(SAMPLE, "rt", encoding="utf-8") as handle:
        for line in handle:
            domain, _title, views, _size = line.rstrip("\n").split(" ")
            totals[domain] += int(views)
    oracle = out / dump.projectviews_filename
    oracle.write_text(
        "".join(f"{domain} - {views} 0\n" for domain, views in sorted(totals.items())),
        encoding="utf-8",
    )
    upload_file(oracle, get_settings().raw_bucket, dump.raw_key("projectviews"))


def dbt(target: str, workdir: Path, *extra: str) -> str:
    binary = os.environ.get("DBT_BIN") or shutil.which("dbt") or "/home/airflow/dbt-venv/bin/dbt"
    common = ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if not (DBT_DIR / "dbt_packages" / "dbt_utils").exists():
        subprocess.run([binary, "deps", *common], check=True, capture_output=True, text=True)
    # Own target and log paths: never overwrite the live pipeline's run_results.json.
    paths = ["--target-path", str(workdir / f"target-{target}"), "--log-path", str(workdir)]
    result = subprocess.run(
        [binary, "build", *common, "--target", target, *paths, *extra],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, f"dbt build --target {target} failed:\n{result.stdout[-4000:]}"
    return result.stdout


def fact_rows(duckdb_path: Path, interval: datetime, qid: str) -> int:
    connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT count(*) FROM main_marts.fct_company_pageviews_hourly
            WHERE view_date = ? AND view_hour = ? AND company_qid = ?
            """,
            [interval.date(), interval.hour, qid],
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return int(row[0])


# --- the scenario ------------------------------------------------------------------------


def test_both_warehouses_build_agree_and_follow_a_rewritten_hour(stack: Path) -> None:
    from wikiflow.parity import compare_warehouses, summarise
    from wikiflow.postgres_load import sync

    duckdb_path = Path(os.environ["WIKIFLOW_DUCKDB_PATH"])

    # 1. Two hours of bronze, then both warehouses from scratch.
    ingest_from_fixture(HOUR_A, company_pages(), stack)
    ingest_from_fixture(HOUR_B, company_pages(), stack)

    first = sync()
    assert (first.loaded, first.unchanged) == (6, 0), "2 hours x 3 datasets"

    for target in ("prod", "pg"):
        output = dbt(target, stack)
        assert "assert_domain_totals_reconcile_with_projectviews" in output
        assert "assert_url_decode_behaves_identically" in output

    report = compare_warehouses(duckdb_path)
    assert report.identical, summarise(report)
    apple_before = fact_rows(duckdb_path, HOUR_A, "Q312")
    assert apple_before > 0

    # 2. Rewrite the OLDER hour with a narrower filter, as a reprocess would.
    ingest_from_fixture(HOUR_A, company_pages(exclude_qid="Q312"), stack)

    second = sync()
    assert (second.loaded, second.unchanged) == (2, 4), "only hour A's two bronze grains changed"

    # Before the marts catch up, parity must say "not comparable", not "different".
    dbt("prod", stack)
    behind = compare_warehouses(duckdb_path)
    assert not behind.comparable, summarise(behind)

    # 3. Ordinary incremental builds - no --full-refresh - pick up the rewritten hour.
    dbt("pg", stack)
    assert fact_rows(duckdb_path, HOUR_A, "Q312") == 0
    assert fact_rows(duckdb_path, HOUR_B, "Q312") > 0, "the untouched hour must survive"

    report = compare_warehouses(duckdb_path)
    assert report.identical, summarise(report)


def test_a_load_that_fails_mid_partition_changes_nothing(
    stack: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The DELETE and half the COPY happen, then the load dies. Postgres must not notice.

    The connection runs in autocommit mode, with each partition in an explicit
    transaction. If that transaction did not actually wrap the DELETE, this is the case
    that would lose a whole hour.
    """
    from wikiflow.config import get_settings
    from wikiflow.postgres_load import DATASETS, MANIFEST, list_objects, load_partition, sync
    from wikiflow.postgres_load import partition_of as partition_for
    from wikiflow.storage import configure_duckdb_s3

    sync()  # whatever the previous test left, fully loaded
    dataset = DATASETS[1]  # bronze.domain_totals: ~320 rows an hour in the fixture
    dataset_name, key, _etag = next(iter(list_objects(dataset)))
    partition = partition_for(dataset_name, key)
    assert partition is not None

    def state(connection: psycopg.Connection) -> tuple[int, str]:
        rows = connection.execute(
            f"select count(*) from {dataset.name} where dt = %s and hour = %s",
            (partition.dt, partition.hour),
        ).fetchone()
        manifest = connection.execute(
            f"select etag from {MANIFEST} where dataset = %s and dt = %s and hour = %s",
            (dataset.name, partition.dt, partition.hour),
        ).fetchone()
        assert rows is not None and manifest is not None
        return int(rows[0]), str(manifest[0])

    written = 0
    original_write_row = psycopg.Copy.write_row

    def dies_part_way(self: psycopg.Copy, row: object) -> None:
        nonlocal written
        written += 1
        if written == 100:
            raise ConnectionError("simulated: connection lost mid-COPY")
        original_write_row(self, row)  # type: ignore[arg-type]

    duck = duckdb.connect()
    configure_duckdb_s3(duck)
    with psycopg.connect(get_settings().pg_conninfo, autocommit=True) as connection:
        before = state(connection)
        assert before[0] > 100

        monkeypatch.setattr(psycopg.Copy, "write_row", dies_part_way)
        with pytest.raises(ConnectionError, match="mid-COPY"):
            load_partition(connection, duck, dataset, partition, key, "etag-that-must-not-land")
        monkeypatch.undo()

        assert written == 100, "the failure must happen after the delete and some rows"
        assert state(connection) == before, "rows or manifest changed despite the failure"

    # And the next ordinary sync finds nothing to repair.
    assert sync().loaded == 0
