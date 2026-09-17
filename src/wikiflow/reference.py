"""The company filter as versioned reference data in object storage.

Resolving the filter used to happen inside every ingest run: one Wikidata call, about
2,200 pages. Redirect resolution makes that untenable - it is a few hundred MediaWiki
requests, and the ingest DAG runs 24 times a day, twice concurrently during a backfill.
The inputs change over weeks, not hours.

So the filter is resolved once a day by its own DAG and published here, and ingestion
reads the published copy:

    reference/company_pages/snapshots/20260917T020000Z.parquet   immutable, one per refresh
    reference/company_pages/current.parquet                      what ingestion reads

Every snapshot is kept, and each file names itself in a `snapshot_key` column (the copy at
`current.parquet` included). Ingestion records that key in the hour's audit record, so the
exact filter any hour was extracted with can be found - for hours ingested from
2026-09-17 on; earlier audit records predate the field.

Publishing refuses a snapshot that is much SMALLER than the current one. A Wikidata
outage that returned half the sitelinks, or a wiki that answered with nothing, would
otherwise shrink the filter silently, and every hour ingested afterwards would be an
undercount that no downstream check could tell from a quiet day. The same reasoning as
the retention guard in ADR 0006: a sudden large loss is more likely a fault than a fact.
"""

from __future__ import annotations

import csv
import tempfile
from dataclasses import astuple, dataclass, fields
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from wikiflow.config import get_settings
from wikiflow.storage import object_exists, s3_client, upload_file
from wikiflow.wikidata import CompanyPage

REFERENCE_PREFIX = "reference/company_pages"
CURRENT_KEY = f"{REFERENCE_PREFIX}/current.parquet"

#: The largest drop, in either article rows or all rows, a new snapshot may show against
#: the current one before publishing is refused.
MAX_SHRINK_RATIO = 0.10

PAGE_COLUMNS = tuple(f.name for f in fields(CompanyPage))


class ReferenceShrinkError(RuntimeError):
    """A new filter snapshot lost too many pages to be trusted."""


@dataclass(frozen=True, slots=True)
class FilterCounts:
    articles: int
    total: int

    @classmethod
    def of(cls, pages: list[CompanyPage]) -> FilterCounts:
        return cls(
            articles=sum(1 for p in pages if p.match_type == "sitelink"),
            total=len(pages),
        )


def snapshot_key(resolved_at: datetime) -> str:
    return f"{REFERENCE_PREFIX}/snapshots/{resolved_at.astimezone(UTC):%Y%m%dT%H%M%SZ}.parquet"


def check_not_shrunk(
    previous: FilterCounts | None, new: FilterCounts, *, max_ratio: float = MAX_SHRINK_RATIO
) -> None:
    """Raise if `new` has lost more than `max_ratio` of the articles or rows in `previous`."""
    if new.articles == 0:
        raise ReferenceShrinkError("refusing to publish a filter with no articles in it")
    if previous is None:
        return
    for label, before, after in (
        ("article rows", previous.articles, new.articles),
        ("rows", previous.total, new.total),
    ):
        if before and (before - after) / before > max_ratio:
            raise ReferenceShrinkError(
                f"refusing to publish: {label} fell from {before:,} to {after:,} "
                f"({(before - after) / before:.1%}, limit {max_ratio:.0%}). A resolution "
                "that lost this much is more likely a failed lookup than a real change; "
                "the current snapshot stays in use."
            )


@dataclass(frozen=True, slots=True)
class PublishedFilter:
    snapshot_key: str
    """The immutable snapshot this filter was read from. Empty for a file published
    before snapshots named themselves."""
    pages: list[CompanyPage]


def write_pages_parquet(pages: list[CompanyPage], path: Path, *, snapshot_key: str = "") -> None:
    """Write pages to Parquet, via CSV - DuckDB's fast path (see extract.py)."""
    with tempfile.TemporaryDirectory(prefix="wikiflow-reference-") as tmpdir:
        csv_path = Path(tmpdir) / "pages.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(PAGE_COLUMNS)
            writer.writerows(
                ["" if value is None else value for value in astuple(page)] for page in pages
            )

        columns = ", ".join(f"'{c}': 'VARCHAR'" for c in PAGE_COLUMNS)
        source = csv_path.as_posix().replace("'", "''")
        target = path.as_posix().replace("'", "''")
        connection = duckdb.connect()
        try:
            connection.execute(
                f"""
                COPY (
                    -- quote AND escape explicit: titles contain `"` (Hebrew uses it
                    -- as an abbreviation mark, e.g. אלפבת_בע"מ), and the auto-detected
                    -- escape rejects the doubled quote that csv.writer produces.
                    SELECT *, '{snapshot_key.replace("'", "''")}' AS snapshot_key
                    FROM read_csv('{source}', header = true, quote = '"',
                                  escape = '"', columns = {{{columns}}})
                    ORDER BY domain_code, page_title
                ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
        finally:
            connection.close()


def read_pages_parquet(path: Path) -> PublishedFilter:
    connection = duckdb.connect()
    try:
        present = {
            row[0]
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [path.as_posix()]
            ).fetchall()
        }
        rows = connection.execute(
            f"SELECT {', '.join(PAGE_COLUMNS)} FROM read_parquet(?)", [path.as_posix()]
        ).fetchall()
        key_row = (
            connection.execute(
                "SELECT any_value(snapshot_key) FROM read_parquet(?)", [path.as_posix()]
            ).fetchone()
            if "snapshot_key" in present
            else None
        )
    finally:
        connection.close()
    pages = []
    for row in rows:
        values = dict(zip(PAGE_COLUMNS, row, strict=True))
        values = {k: ("" if v is None else v) for k, v in values.items()}
        values["language"] = values["language"] or None
        pages.append(CompanyPage(**values))
    return PublishedFilter(snapshot_key=(key_row[0] or "") if key_row else "", pages=pages)


def load_current() -> PublishedFilter | None:
    """The published filter, or None if nothing has been published yet."""
    bucket = get_settings().lake_bucket
    if not object_exists(bucket, CURRENT_KEY):
        return None
    with tempfile.TemporaryDirectory(prefix="wikiflow-reference-") as tmpdir:
        local = Path(tmpdir) / "current.parquet"
        s3_client().download_file(bucket, CURRENT_KEY, str(local))
        return read_pages_parquet(local)


def publish(
    pages: list[CompanyPage],
    *,
    resolved_at: datetime | None = None,
    max_ratio: float = MAX_SHRINK_RATIO,
) -> str:
    """Publish a new snapshot and make it current. Returns the snapshot's key.

    The immutable snapshot is written first and `current` replaced second, so a failure
    between the two leaves the previous `current` intact rather than half-written.
    """
    resolved_at = resolved_at or datetime.now(UTC)
    current = load_current()
    check_not_shrunk(
        FilterCounts.of(current.pages) if current is not None else None,
        FilterCounts.of(pages),
        max_ratio=max_ratio,
    )

    bucket = get_settings().lake_bucket
    key = snapshot_key(resolved_at)
    with tempfile.TemporaryDirectory(prefix="wikiflow-reference-") as tmpdir:
        local = Path(tmpdir) / "snapshot.parquet"
        write_pages_parquet(pages, local, snapshot_key=key)
        upload_file(local, bucket, key)
        upload_file(local, bucket, CURRENT_KEY)
    return key
