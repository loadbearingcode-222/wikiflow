"""Loading bronze into the server warehouse, incrementally.

DuckDB reads Parquet straight out of object storage, so the DuckDB warehouse never needs
a load step. Postgres cannot, so the same bronze partitions have to be copied in before
dbt can build the same marts there (ADR 0008).

**Which partitions are new?** This is a different incremental question from the one
ingestion answers. Ingestion is driven by time: the Airflow data interval names the hour,
so no state is needed. Here the source is object storage that ingestion keeps rewriting -
a re-run, a reprocess from raw or a recovered partition all REPLACE an existing hour.
"Every hour after the newest loaded" would miss all of those.

So the loader keeps a manifest in Postgres: the object's ETag for every partition it has
loaded. A partition is (re)loaded when its current ETag differs from the manifest's, or
the manifest has no row for it. S3 changes the ETag whenever the object's content
changes, so a rewritten hour is picked up and an untouched one costs nothing.

**Each partition loads in one transaction**: delete the hour, copy its rows in, record
the ETag. A failure part-way through rolls all three back, so the manifest never claims
a partition that is not fully there - the same delete-then-write rule as the lake, with
the database's atomicity instead of object-store ordering.

**Deletions are not propagated.** A partition missing from object storage is reported,
not deleted from Postgres. Raw projectviews files expire after 7 days by design, and a
lost bronze partition is a fault for the gap check to report - mirroring it would turn
one incident into two.
"""

from __future__ import annotations

import argparse
import logging
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date

import duckdb
import psycopg

from wikiflow.config import get_settings
from wikiflow.contracts import CompanyPageviewRecord, DomainTotalRecord, postgres_columns
from wikiflow.storage import configure_duckdb_s3, s3_client

log = logging.getLogger(__name__)

PARTITION = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/hour=(\d{1,2})/")


@dataclass(frozen=True, slots=True)
class Dataset:
    name: str
    """`schema.table` in Postgres, and the manifest's dataset key."""
    bucket_setting: str
    prefix: str
    columns: tuple[tuple[str, str], ...]
    """(name, Postgres type) in table order."""
    reader: str
    """DuckDB table function for one object; `{url}` is substituted."""


_PARTITION_COLUMNS = (
    ("dt", "date not null"),
    ("hour", "bigint not null"),
)

DATASETS: tuple[Dataset, ...] = (
    # Bronze tables follow the data contract, so a column added there is added here.
    Dataset(
        name="bronze.company_pageviews",
        bucket_setting="lake_bucket",
        prefix="bronze/company_pageviews/",
        columns=postgres_columns(CompanyPageviewRecord),
        reader="read_parquet('{url}', hive_partitioning = false)",
    ),
    Dataset(
        name="bronze.domain_totals",
        bucket_setting="lake_bucket",
        prefix="bronze/domain_totals/",
        columns=postgres_columns(DomainTotalRecord),
        reader="read_parquet('{url}', hive_partitioning = false)",
    ),
    # Wikimedia's own totals: the reconciliation oracle. Its partition keys live only in
    # the path, so they are supplied as literals rather than read from the file.
    Dataset(
        name="raw.projectviews",
        bucket_setting="raw_bucket",
        prefix="projectviews/",
        columns=(
            *_PARTITION_COLUMNS,
            ("domain_code", "text"),
            ("page_title", "text"),
            ("count_views", "bigint"),
            ("total_response_size", "bigint"),
        ),
        reader=(
            "read_csv('{url}', delim = ' ', header = false, quote = '', escape = '', "
            "names = ['domain_code', 'page_title', 'count_views', 'total_response_size'], "
            "types = ['VARCHAR', 'VARCHAR', 'BIGINT', 'BIGINT'], hive_partitioning = false)"
        ),
    ),
)

MANIFEST = "ops.load_manifest"


@dataclass(frozen=True, slots=True, order=True)
class Partition:
    dataset: str
    dt: date
    hour: int


@dataclass(frozen=True, slots=True)
class SyncPlan:
    to_load: dict[Partition, tuple[str, str]]
    """Partition -> (object key, ETag) for every partition new or changed since loaded."""
    unchanged: int
    missing_from_storage: list[Partition]


@dataclass(frozen=True, slots=True)
class SyncReport:
    loaded: int
    rows: int
    unchanged: int
    missing_from_storage: int


def partition_of(dataset: str, key: str) -> Partition | None:
    match = PARTITION.search(key)
    if match is None:
        return None
    return Partition(dataset, date.fromisoformat(match.group(1)), int(match.group(2)))


def plan_sync(
    objects: Iterable[tuple[str, str, str]],
    manifest: dict[Partition, str],
) -> SyncPlan:
    """Decide what to load. Pure, so the incremental rule is testable without storage.

    Args:
        objects: (dataset, key, etag) for every object currently in storage.
        manifest: Partition -> ETag as last loaded.

    Raises:
        ValueError: if one partition holds more than one object. Every writer replaces a
            partition with exactly one file, so two means something else wrote there,
            and loading either one alone would be wrong.
    """
    current: dict[Partition, tuple[str, str]] = {}
    for dataset, key, etag in objects:
        partition = partition_of(dataset, key)
        if partition is None:
            log.warning("ignoring %s: not in a dt=/hour= partition", key)
            continue
        if partition in current:
            raise ValueError(
                f"{partition} has more than one object ({current[partition][0]}, {key}); "
                "refusing to guess which is authoritative"
            )
        current[partition] = (key, etag)

    to_load = {p: obj for p, obj in current.items() if manifest.get(p) != obj[1]}
    return SyncPlan(
        to_load=dict(sorted(to_load.items())),
        unchanged=len(current) - len(to_load),
        missing_from_storage=sorted(p for p in manifest if p not in current),
    )


def ensure_schema(connection: psycopg.Connection) -> None:
    """Create the loaded tables and the manifest. Idempotent."""
    with connection.cursor() as cursor:
        for schema in {d.name.split(".")[0] for d in DATASETS} | {MANIFEST.split(".")[0]}:
            cursor.execute(f"create schema if not exists {schema}")
        for dataset in DATASETS:
            columns = ", ".join(f"{name} {kind}" for name, kind in dataset.columns)
            cursor.execute(f"create table if not exists {dataset.name} ({columns})")
            # Every load deletes by (dt, hour); without this index each delete is a scan.
            index = dataset.name.replace(".", "_") + "_partition"
            cursor.execute(f"create index if not exists {index} on {dataset.name} (dt, hour)")
        cursor.execute(
            f"""
            create table if not exists {MANIFEST} (
                dataset    text        not null,
                dt         date        not null,
                hour       bigint      not null,
                object_key text        not null,
                etag       text        not null,
                row_count  bigint      not null,
                loaded_at  timestamptz not null default now(),
                primary key (dataset, dt, hour)
            )
            """
        )
    connection.commit()


def read_manifest(connection: psycopg.Connection) -> dict[Partition, str]:
    with connection.cursor() as cursor:
        cursor.execute(f"select dataset, dt, hour, etag from {MANIFEST}")
        return {Partition(ds, dt, int(hour)): etag for ds, dt, hour, etag in cursor.fetchall()}


def list_objects(dataset: Dataset) -> Iterator[tuple[str, str, str]]:
    bucket = getattr(get_settings(), dataset.bucket_setting)
    paginator = s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=dataset.prefix):
        for item in page.get("Contents", []):
            yield dataset.name, item["Key"], item["ETag"].strip('"')


def _select_list(
    duck: duckdb.DuckDBPyConnection, dataset: Dataset, source: str, part: Partition
) -> str:
    """Columns in table order, each CAST to its declared type, NULL for any the file predates.

    Bronze gained match_type and canonical_title with redirect resolution (ADR 0007).
    A partition written before then is still valid; its rows simply lack both.

    The casts are not decoration. The first two domain_totals partitions ever written
    predate the fix that cast sums to BIGINT, so their `views` column is DOUBLE. Read
    together with the other 52, DuckDB silently unified the types and nothing noticed;
    copied file by file into Postgres, `3882.0` was rejected as a bigint. Casting here
    makes the file's own type irrelevant - and a view count with a real fractional part
    would still fail loudly, which is the right outcome.
    """
    present = {row[0] for row in duck.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}
    select = []
    for name, pg_type in dataset.columns:
        duck_type = _DUCKDB_TYPES[pg_type.removesuffix(" not null")]
        if name == "dt" and name not in present:
            expression = f"DATE '{part.dt.isoformat()}'"
        elif name == "hour" and name not in present:
            expression = str(part.hour)
        elif name in present:
            expression = f'"{name}"'
        else:
            expression = "NULL"
        select.append(f"CAST({expression} AS {duck_type}) AS {name}")
    return ", ".join(select)


_DUCKDB_TYPES = {
    "date": "DATE",
    "bigint": "BIGINT",
    "text": "VARCHAR",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
}


def load_partition(
    connection: psycopg.Connection,
    duck: duckdb.DuckDBPyConnection,
    dataset: Dataset,
    partition: Partition,
    key: str,
    etag: str,
) -> int:
    """Replace one partition in Postgres, atomically. Returns the rows loaded."""
    bucket = getattr(get_settings(), dataset.bucket_setting)
    url = f"s3://{bucket}/{key}".replace("'", "''")
    source = dataset.reader.format(url=url)
    rows = duck.execute(
        f"SELECT {_select_list(duck, dataset, source, partition)} FROM {source}"
    ).fetchall()

    names = ", ".join(name for name, _ in dataset.columns)
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            f"delete from {dataset.name} where dt = %s and hour = %s",
            (partition.dt, partition.hour),
        )
        with cursor.copy(f"copy {dataset.name} ({names}) from stdin") as copy:
            for row in rows:
                copy.write_row(row)
        cursor.execute(
            f"""
            insert into {MANIFEST} (dataset, dt, hour, object_key, etag, row_count)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (dataset, dt, hour) do update
               set object_key = excluded.object_key, etag = excluded.etag,
                   row_count = excluded.row_count, loaded_at = now()
            """,
            (dataset.name, partition.dt, partition.hour, key, etag, len(rows)),
        )
    return len(rows)


def sync(*, conninfo: str | None = None) -> SyncReport:
    """Bring Postgres up to date with object storage."""
    conninfo = conninfo or get_settings().pg_conninfo
    by_name = {d.name: d for d in DATASETS}

    duck = duckdb.connect()
    configure_duckdb_s3(duck)
    # autocommit: each partition gets its own explicit transaction in load_partition.
    with psycopg.connect(conninfo, autocommit=True) as connection:
        ensure_schema(connection)
        plan = plan_sync(
            (obj for dataset in DATASETS for obj in list_objects(dataset)),
            read_manifest(connection),
        )
        log.info(
            "sync plan: %d partition(s) to load, %d unchanged, %d no longer in storage",
            len(plan.to_load),
            plan.unchanged,
            len(plan.missing_from_storage),
        )
        rows = 0
        for partition, (key, etag) in plan.to_load.items():
            loaded = load_partition(
                connection, duck, by_name[partition.dataset], partition, key, etag
            )
            rows += loaded
            log.info(
                "loaded %s %s hour=%d: %d rows",
                partition.dataset,
                partition.dt,
                partition.hour,
                loaded,
            )
    duck.close()

    return SyncReport(
        loaded=len(plan.to_load),
        rows=rows,
        unchanged=plan.unchanged,
        missing_from_storage=len(plan.missing_from_storage),
    )


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Sync bronze and projectviews into Postgres").parse_args(
        argv
    )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(sync())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
