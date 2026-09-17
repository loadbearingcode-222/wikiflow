"""End-to-end extraction against committed fixtures. No network, no containers.

These are marked `integration` because they exercise DuckDB, real gzip decoding and
real Parquet output rather than pure functions - but they still run in under a second
and are safe to run anywhere.
"""

from __future__ import annotations

import csv
import gzip
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


@pytest.mark.integration
def test_wide_grain_carries_parsed_domain_attributes(result: ExtractionResult) -> None:
    """The wide grain must arrive pre-parsed, so dbt never re-implements the scheme.

    Re-implementing `.m` handling in SQL would mean two definitions of a rule where
    the same token means three different things, drifting apart over time.

    The codes checked here are chosen to cover all three meanings of `.m` at once.
    """
    connection = duckdb.connect()
    rows = connection.execute(
        f"""
        SELECT domain_code, project, language, access_method, is_parseable
        FROM '{result.domain_path.as_posix()}'
        WHERE domain_code IN ('en', 'en.m', 'commons.m', 'commons.m.m', 'it.m.q', 'm.wd')
        ORDER BY domain_code
        """
    ).fetchall()

    assert rows == [
        # `.m` as the wikimedia.org project suffix - DESKTOP, not mobile
        ("commons.m", "wikimedia", "commons", "desktop", True),
        # both meanings at once
        ("commons.m.m", "wikimedia", "commons", "mobile-web", True),
        ("en", "wikipedia", "en", "desktop", True),
        # `.m` as the mobile marker
        ("en.m", "wikipedia", "en", "mobile-web", True),
        ("it.m.q", "wikiquote", "it", "mobile-web", True),
        # prefix-less wiki: the mobile marker leads
        ("m.wd", "wikidata", None, "mobile-web", True),
    ]


@pytest.mark.integration
def test_unparseable_domains_are_kept_not_dropped(tmp_path: Path) -> None:
    """Dropping them would silently break reconciliation against projectviews.

    Real files contain a literal `""` domain code carrying several thousand views an
    hour, and Wikimedia counts it in their own per-project totals. Discarding it would
    make our sum smaller than theirs.

    Built synthetically rather than taken from the sample fixture: the sample happens
    to contain no malformed codes, and a test that silently passes because its input
    lacks the case it checks is worse than no test.
    """
    source = tmp_path / "malformed.gz"
    with gzip.open(source, "wt", encoding="utf-8", newline="\n") as handle:
        handle.write("en Apple_Inc. 100 0\n")
        handle.write('"" Some_Page 4321 0\n')

    result = extract_hour(
        source=str(source),
        dump=HourlyDump.for_interval(INTERVAL),
        company_pages=[],
        output_dir=tmp_path / "out",
    )

    connection = duckdb.connect()
    unparseable = connection.execute(
        f"SELECT domain_code, views FROM '{result.domain_path.as_posix()}' WHERE NOT is_parseable"
    ).fetchall()

    assert unparseable == [('""', 4321)]
    # The malformed row's views must still be counted in the total.
    assert result.total_views == 100 + 4321


@pytest.mark.integration
def test_too_many_unparseable_lines_refuses_the_hour_before_writing(tmp_path: Path) -> None:
    """Truncation must fail extraction, not quietly shrink the hour.

    Measured against a real file: cut off at 30%, read with ignore_errors, DuckDB
    returned 2.1M rows and no error. The gate turns that silence into a failure, and it
    does so before any partition exists.
    """
    from wikiflow.extract import MalformedRowsError

    source = tmp_path / "damaged.gz"
    with gzip.open(source, "wt", encoding="utf-8", newline="\n") as handle:
        for index in range(100):
            handle.write(f"en Page_{index} 1 0\n")
        handle.write("en this line has far too many fields 1 0\n")

    out = tmp_path / "out"
    with pytest.raises(MalformedRowsError, match="did not parse"):
        extract_hour(
            source=str(source),
            dump=HourlyDump.for_interval(INTERVAL),
            company_pages=[],
            output_dir=out,
            expected_lines=101,
        )

    assert not (out / "domain_totals.parquet").exists(), "nothing may be written"
    assert not (out / "company_pageviews.parquet").exists()


@pytest.mark.integration
def test_redirect_views_are_matched_labelled_and_attributed(tmp_path: Path) -> None:
    """A view requested under a redirect title reaches bronze, labelled with its article.

    Synthetic, like the test above: the redirect title includes a `"`, the character
    that broke loading the filter on real data (Hebrew abbreviation marks).
    """
    source = tmp_path / "redirects.gz"
    with gzip.open(source, "wt", encoding="utf-8", newline="\n") as handle:
        handle.write("en Apple_Computer 40 0\n")
        handle.write("en Apple_Inc. 100 0\n")
        handle.write('he אלפבת_בע"מ 3 0\n')

    def page(domain: str, title: str, match: str, canonical: str) -> CompanyPage:
        return CompanyPage(
            qid="Q312",
            company_name="Apple Inc.",
            sitelink_key=f"{domain}wiki",
            language=domain,
            project="wikipedia",
            page_title=title,
            domain_code=domain,
            access_method="desktop",
            match_type=match,
            canonical_title=canonical,
        )

    result = extract_hour(
        source=str(source),
        dump=HourlyDump.for_interval(INTERVAL),
        company_pages=[
            page("en", "Apple_Inc.", "sitelink", "Apple_Inc."),
            page("en", "Apple_Computer", "redirect", "Apple_Inc."),
            page("he", 'אלפבת_בע"מ', "redirect", "אפל"),
        ],
        output_dir=tmp_path / "out",
    )

    rows = (
        duckdb.connect()
        .execute(
            f"""
            SELECT page_title, match_type, canonical_title, count_views
            FROM '{result.company_path.as_posix()}' ORDER BY count_views DESC
            """
        )
        .fetchall()
    )
    assert rows == [
        ("Apple_Inc.", "sitelink", "Apple_Inc.", 100),
        ("Apple_Computer", "redirect", "Apple_Inc.", 40),
        ('אלפבת_בע"מ', "redirect", "אפל", 3),
    ]
    assert result.company_views == 143


@pytest.mark.unit
def test_malformed_ratio_boundary() -> None:
    from wikiflow.extract import MalformedRowsError, check_malformed

    # Exactly at the limit is tolerated; one line over is not.
    assert check_malformed(rows_scanned=99_999, expected_lines=100_000) == 1
    with pytest.raises(MalformedRowsError):
        check_malformed(rows_scanned=99_998, expected_lines=100_000)
    assert check_malformed(rows_scanned=7_451_970, expected_lines=7_451_970) == 0
