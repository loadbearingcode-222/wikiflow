"""End-to-end extraction against committed fixtures. No network, no containers.

These are marked `integration` because they exercise DuckDB, real gzip decoding and
real Parquet output rather than pure functions - but they still run in under a second
and are safe to run anywhere.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from wikiflow.extract import ExtractionResult, extract_hour
from wikiflow.source import HourlyDump
from wikiflow.wikidata import CompanyPage

FIXTURES = Path(__file__).parents[1] / "fixtures"
SAMPLE = FIXTURES / "pageviews-sample-20260914-120000.gz"
COMPANY_PAGES = FIXTURES / "company_pages.csv"

# The hour the sample covers. The file is LABELLED 12:00; per ADR 0001 that means it
# holds 11:00-11:59.
INTERVAL = datetime(2026, 9, 14, 11, tzinfo=UTC)

# Measured from the fixture when it was built.
EXPECTED_LINES = 4379
EXPECTED_COMPANY_ROWS = 379
EXPECTED_COMPANY_VIEWS = 5138
EXPECTED_DOMAIN_ROWS = 320
EXPECTED_TOTAL_VIEWS = 15255


@pytest.fixture(scope="module")
def company_pages() -> list[CompanyPage]:
    with COMPANY_PAGES.open(newline="", encoding="utf-8") as handle:
        return [
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


@pytest.fixture
def result(tmp_path: Path, company_pages: list[CompanyPage]) -> ExtractionResult:
    return extract_hour(
        source=str(SAMPLE),
        dump=HourlyDump.for_interval(INTERVAL),
        company_pages=company_pages,
        output_dir=tmp_path,
        expected_lines=EXPECTED_LINES,
    )


@pytest.mark.integration
def test_extracts_expected_counts(result: ExtractionResult) -> None:
    assert result.rows_scanned == EXPECTED_LINES
    assert result.rows_malformed == 0
    assert result.company_rows == EXPECTED_COMPANY_ROWS
    assert result.company_views == EXPECTED_COMPANY_VIEWS
    assert result.domain_rows == EXPECTED_DOMAIN_ROWS
    assert result.total_views == EXPECTED_TOTAL_VIEWS


@pytest.mark.integration
def test_wide_grain_totals_match_the_raw_file(result: ExtractionResult) -> None:
    """The denominator must account for every view in the file, not a subset."""
    connection = duckdb.connect()
    fetched = connection.execute(
        f"SELECT sum(views) FROM '{result.domain_path.as_posix()}'"
    ).fetchone()
    assert fetched is not None
    total = fetched[0]

    assert total == EXPECTED_TOTAL_VIEWS


@pytest.mark.integration
def test_partition_columns_use_the_data_hour_not_the_filename_hour(
    result: ExtractionResult,
) -> None:
    """The fixture is named 12:00 but holds 11:00 data. Partitions must say 11."""
    connection = duckdb.connect()
    row = connection.execute(
        f"SELECT DISTINCT dt, hour FROM '{result.company_path.as_posix()}'"
    ).fetchall()

    assert row == [(datetime(2026, 9, 14).date(), 11)]


@pytest.mark.integration
def test_unicode_titles_survive_the_round_trip(result: ExtractionResult) -> None:
    """Arabic, Japanese and Chinese titles must not be mangled by encoding."""
    connection = duckdb.connect()
    titles = {
        r[0]
        for r in connection.execute(
            f"SELECT page_title FROM '{result.company_path.as_posix()}'"
        ).fetchall()
    }

    assert any(not title.isascii() for title in titles), "expected non-ASCII titles"
    # Spot-check a specific known article rather than only the general property.
    assert any("アップル" in t or "Apple" in t for t in titles)


@pytest.mark.integration
def test_extraction_is_deterministic(tmp_path: Path, company_pages: list[CompanyPage]) -> None:
    """Re-running the same hour must produce identical counts.

    This is the property that makes backfill and retry safe. It is asserted here at
    the extraction level; the partition-overwrite behaviour that makes it safe at the
    storage level is tested separately.
    """
    dump = HourlyDump.for_interval(INTERVAL)
    first = extract_hour(
        source=str(SAMPLE), dump=dump, company_pages=company_pages, output_dir=tmp_path / "a"
    )
    second = extract_hour(
        source=str(SAMPLE), dump=dump, company_pages=company_pages, output_dir=tmp_path / "b"
    )

    assert (first.rows_scanned, first.company_rows, first.company_views) == (
        second.rows_scanned,
        second.company_rows,
        second.company_views,
    )


@pytest.mark.integration
def test_no_company_rows_when_filter_is_empty(tmp_path: Path) -> None:
    """An empty filter must yield zero rows, not silently match everything."""
    result = extract_hour(
        source=str(SAMPLE),
        dump=HourlyDump.for_interval(INTERVAL),
        company_pages=[],
        output_dir=tmp_path,
    )

    assert result.company_rows == 0
    # The wide grain is unaffected by the filter.
    assert result.total_views == EXPECTED_TOTAL_VIEWS
