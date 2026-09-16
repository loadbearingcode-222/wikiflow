"""Turning one raw hourly dump into two Parquet partitions.

The file is ~8M rows and ~250 MB decompressed, but almost all of it is irrelevant to
the question being asked. Rather than store it all or throw it all away, one scan
produces two grains:

  narrow  every row about a tracked company, any language, any access method
          (~200-600 rows) - the fact table

  wide    sum(count_views) by domain_code (~1,400 rows) - the denominator, so a
          company's traffic can be expressed as a share rather than a bare number,
          and the reconciliation key against the projectviews file

Both are tiny, so both are kept indefinitely while raw files cycle out after 7 days.

The scan itself is the expensive part and happens regardless of how little is kept,
which is why DuckDB is doing it rather than Python: it reads the gzip, splits the
columns and aggregates in C++ across all cores.

A note on CSV quoting: `quote=''` is deliberate and load-bearing. Page titles legally
contain double-quote characters, and DuckDB's default CSV quoting would treat those
as field delimiters and silently merge or truncate rows.
"""

from __future__ import annotations

import csv
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from wikiflow.parsing import UnparseableDomainCode, parse_domain_code
from wikiflow.source import HourlyDump
from wikiflow.wikidata import CompanyPage

#: The most lines per hour that may fail to parse before extraction refuses the file.
#:
#: `ignore_errors=true` below lets DuckDB skip an unparseable line instead of aborting the
#: hour. The cost of that convenience was measured, not assumed: a download cut off 30%
#: of the way through, read with ignore_errors, returned 2,111,414 rows and NO error -
#: 72% of the hour silently gone. Without the option DuckDB raises at the cut-off line.
#:
#: Download verification rejects such a file first, but that would make it the only
#: defence. Across 40 real hours (~270M lines) exactly zero lines failed to parse, so
#: this can be strict: 1 in 100,000 tolerates a stray garbage line upstream, while any
#: truncation-scale loss fails loudly.
MAX_MALFORMED_RATIO = 1e-5


class MalformedRowsError(ValueError):
    """Too many lines failed to parse for the hour to be trusted."""


def check_malformed(
    *, rows_scanned: int, expected_lines: int, max_ratio: float = MAX_MALFORMED_RATIO
) -> int:
    """Return the malformed-line count, raising if it exceeds the tolerated ratio."""
    malformed = max(0, expected_lines - rows_scanned)
    if expected_lines and malformed / expected_lines > max_ratio:
        raise MalformedRowsError(
            f"{malformed:,} of {expected_lines:,} lines did not parse "
            f"({malformed / expected_lines:.4%}, limit {max_ratio:.4%}). Refusing to publish "
            "a partition that may be missing rows - a truncated file looks exactly like this."
        )
    return malformed


#: The four space-separated fields of a pageviews row.
COLUMNS = {
    "domain_code": "VARCHAR",
    "page_title": "VARCHAR",
    "count_views": "BIGINT",
    "total_response_size": "BIGINT",
}


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """Counts for the audit table, and the files produced."""

    rows_scanned: int
    """Rows DuckDB successfully parsed out of the dump."""

    rows_malformed: int
    """Lines present in the file that did not parse into four fields."""

    company_rows: int
    domain_rows: int
    company_views: int
    total_views: int

    company_path: Path
    domain_path: Path

    @property
    def company_share(self) -> float:
        """Tracked companies' share of all views in the hour."""
        return self.company_views / self.total_views if self.total_views else 0.0


def extract_hour(
    *,
    source: str,
    dump: HourlyDump,
    company_pages: list[CompanyPage],
    output_dir: Path,
    expected_lines: int | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> ExtractionResult:
    """Read one hourly dump and write both grains as Parquet.

    Args:
        source: Local path or `s3://` URL of the gzipped pageviews file.
        dump: Supplies the partition keys. Note these come from the data's real hour,
            not the filename's hour - see ADR 0001.
        company_pages: The filter, resolved from Wikidata.
        output_dir: Directory to write the two Parquet files into.
        expected_lines: Line count from download verification. When given, lines that
            fail to parse are counted, and the hour is refused if they exceed
            MAX_MALFORMED_RATIO - before anything is written.
        connection: An existing DuckDB connection, mainly for tests.
    """
    owns_connection = connection is None
    connection = connection or duckdb.connect()
    output_dir.mkdir(parents=True, exist_ok=True)

    company_path = output_dir / "company_pageviews.parquet"
    domain_path = output_dir / "domain_totals.parquet"

    try:
        _register_company_pages(connection, company_pages)

        column_spec = ", ".join(f"'{name}': '{type_}'" for name, type_ in COLUMNS.items())
        # DuckDB cannot bind prepared parameters inside CREATE VIEW or COPY, so the
        # source is inlined as an escaped SQL literal instead.
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW raw AS
            SELECT * FROM read_csv(
                {_sql_literal(source)},
                delim = ' ',
                header = false,
                quote = '',
                escape = '',
                columns = {{{column_spec}}},
                ignore_errors = true
            )
            """
        )

        rows_scanned = _scalar(connection, "SELECT count(*) FROM raw")

        # Checked here, before a single partition is written. The DAG's verify task runs
        # after bronze is published, which would be too late to keep a short hour out.
        malformed = (
            check_malformed(rows_scanned=rows_scanned, expected_lines=expected_lines)
            if expected_lines is not None
            else 0
        )

        # --- wide grain: the denominator, and the reconciliation key --------
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE domain_totals AS
            SELECT
                domain_code,
                -- Explicit casts: DuckDB's sum() over BIGINT yields DOUBLE, which
                -- would make every downstream view count a float and every equality
                -- comparison against the reconciliation oracle a float comparison.
                sum(count_views)::BIGINT         AS views,
                sum(total_response_size)::BIGINT AS response_bytes,
                count(*)::BIGINT                 AS distinct_pages
            FROM raw
            GROUP BY domain_code
            """
        )

        _enrich_domain_totals(connection)

        # --- narrow grain: rows about tracked companies ---------------------
        # An inner join against ~2,200 (domain_code, page_title) pairs. DuckDB builds
        # the hash table from the small side and probes it with the 8M-row scan.
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE company_pageviews AS
            SELECT
                c.qid,
                c.company_name,
                c.language,
                c.project,
                c.access_method,
                r.domain_code,
                r.page_title,
                r.count_views,
                r.total_response_size
            FROM raw r
            JOIN company_pages c
              ON r.domain_code = c.domain_code
             AND r.page_title  = c.page_title
            """
        )

        _write_partition(connection, "domain_totals", domain_path, dump, source)
        _write_partition(connection, "company_pageviews", company_path, dump, source)

        domain_rows, total_views = _row(
            connection, "SELECT count(*), coalesce(sum(views), 0) FROM domain_totals"
        )
        company_rows, company_views = _row(
            connection, "SELECT count(*), coalesce(sum(count_views), 0) FROM company_pageviews"
        )

        return ExtractionResult(
            rows_scanned=rows_scanned,
            rows_malformed=malformed,
            company_rows=company_rows,
            domain_rows=domain_rows,
            company_views=company_views,
            total_views=total_views,
            company_path=company_path,
            domain_path=domain_path,
        )
    finally:
        if owns_connection:
            connection.close()


def _enrich_domain_totals(connection: duckdb.DuckDBPyConnection) -> None:
    """Add (project, language, access_method) to the wide grain.

    The alternative was re-implementing the domain-code scheme as a dbt macro, which
    would mean two definitions of a genuinely tricky rule - `.m` alone means three
    different things - drifting apart over time. Instead the Python parser, which is
    tested against all 1,946 codes in a real hourly file, runs here and the result is
    carried in the data.

    This is cheap precisely because it runs on the AGGREGATED grain: ~1,955 distinct
    domain codes, not 7.45M rows.

    Unparseable codes are kept with `is_parseable = false` rather than dropped. A real
    file contains a literal `""` code carrying several thousand views an hour; dropping
    it would silently break the reconciliation against projectviews, which counts it.
    """
    codes = [
        row[0]
        for row in connection.execute("SELECT DISTINCT domain_code FROM domain_totals").fetchall()
    ]

    parsed: list[tuple[str, str | None, str | None, str | None, bool]] = []
    for code in codes:
        try:
            decoded = parse_domain_code(code)
        except UnparseableDomainCode:
            parsed.append((code, None, None, None, False))
        else:
            parsed.append((code, decoded.project, decoded.language, decoded.access_method, True))

    with tempfile.TemporaryDirectory(prefix="wikiflow-domains-") as tmpdir:
        csv_path = Path(tmpdir) / "domains.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["domain_code", "project", "language", "access_method", "is_parseable"])
            for code, project, language, access, ok in parsed:
                writer.writerow([code, project or "", language or "", access or "", int(ok)])

        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE parsed_domains AS
            SELECT
                domain_code,
                nullif(project, '')       AS project,
                nullif(language, '')      AS language,
                nullif(access_method, '') AS access_method,
                is_parseable::BOOLEAN     AS is_parseable
            FROM read_csv(
                {_sql_literal(csv_path.as_posix())},
                header = true,
                quote = '"',
                columns = {{
                    'domain_code': 'VARCHAR', 'project': 'VARCHAR', 'language': 'VARCHAR',
                    'access_method': 'VARCHAR', 'is_parseable': 'INTEGER'
                }}
            )
            """
        )

    # LEFT JOIN, not INNER: every domain row must survive, parseable or not.
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE domain_totals AS
        SELECT
            t.domain_code,
            p.project,
            p.language,
            p.access_method,
            coalesce(p.is_parseable, false) AS is_parseable,
            t.views,
            t.response_bytes,
            t.distinct_pages
        FROM domain_totals t
        LEFT JOIN parsed_domains p USING (domain_code)
        """
    )


COMPANY_PAGE_COLUMNS = (
    "domain_code",
    "page_title",
    "qid",
    "company_name",
    "language",
    "project",
    "access_method",
)


def _register_company_pages(
    connection: duckdb.DuckDBPyConnection, pages: list[CompanyPage]
) -> None:
    """Load the filter into DuckDB via a temporary CSV rather than INSERT statements.

    DuckDB is columnar, and row-at-a-time INSERT costs roughly 8ms per row no matter
    how it is batched - measured at ~19s for these 2,238 rows, which dwarfed the
    actual 7.4M-row scan. Routing through a file drops that to milliseconds, because
    the CSV reader is the path DuckDB actually optimises.

    This also matches production shape: the company dimension is materialised by the
    monthly Wikidata DAG, so the real pipeline reads it from storage anyway.
    """
    column_ddl = ", ".join(f"{name} VARCHAR" for name in COMPANY_PAGE_COLUMNS)
    connection.execute(f"CREATE OR REPLACE TEMP TABLE company_pages ({column_ddl})")

    if not pages:
        return

    with tempfile.TemporaryDirectory(prefix="wikiflow-filter-") as tmpdir:
        csv_path = Path(tmpdir) / "company_pages.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(COMPANY_PAGE_COLUMNS)
            writer.writerows(
                (
                    page.domain_code,
                    page.page_title,
                    page.qid,
                    page.company_name,
                    page.language or "",
                    page.project,
                    page.access_method,
                )
                for page in pages
            )

        connection.execute(
            f"""
            INSERT INTO company_pages
            SELECT {", ".join(COMPANY_PAGE_COLUMNS)}
            FROM read_csv(
                {_sql_literal(csv_path.as_posix())},
                header = true,
                columns = {{{", ".join(f"'{c}': 'VARCHAR'" for c in COMPANY_PAGE_COLUMNS)}}}
            )
            """
        )


def _write_partition(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    path: Path,
    dump: HourlyDump,
    source: str,
) -> None:
    """Write a grain to Parquet, stamped with partition keys and provenance.

    dt/hour are carried in the data as well as the path so that a file remains
    self-describing if it is ever moved or read outside its directory.
    """
    connection.execute(
        f"""
        COPY (
            SELECT
                DATE '{dump.partition_date}'          AS dt,
                {int(dump.partition_hour)}            AS hour,
                TIMESTAMP '{dump.interval_start:%Y-%m-%d %H:%M:%S}' AS interval_start_utc,
                {_sql_literal(source)}                AS source_url,
                now()                                 AS ingested_at,
                *
            FROM {table}
        ) TO {_sql_literal(path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def _row(connection: duckdb.DuckDBPyConnection, sql: str) -> tuple[Any, ...]:
    """fetchone() that refuses to return None.

    DuckDB types fetchone() as Optional because a query may yield no rows. Every call
    here is an aggregate, which always yields exactly one - so None means something is
    genuinely wrong and should surface rather than becoming a TypeError later.
    """
    result = connection.execute(sql).fetchone()
    if result is None:
        raise RuntimeError(f"expected exactly one row from: {sql}")
    return result


def _scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(_row(connection, sql)[0])


def _sql_literal(value: str) -> str:
    """Quote a string as a SQL literal, escaping embedded single quotes.

    Needed because DuckDB rejects prepared parameters in CREATE VIEW and COPY. Paths
    here come from our own config rather than user input, but escaping properly costs
    nothing and keeps a Windows path or an apostrophe from breaking the statement.
    """
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
