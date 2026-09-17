"""Read-side queries for the dashboard.

Kept out of the Streamlit file for the same reason pipeline logic is kept out of DAG
files: so it can be tested against a known warehouse without starting a UI.

Two rules shape these queries.

**Only claim what can be checked.** The dashboard reports reconciliation status by
reading dbt's own `run_results.json` - the recorded outcome of the test that compares
our totals with Wikimedia's - rather than by asserting it. If dbt has not run, the
dashboard says so instead of showing a reassuring number.

**Marts only, plus the audit trail.** The staging models are views over object storage;
querying them from a UI would put S3 latency behind every widget. Everything shown comes
from materialised mart tables, except pipeline health, which reads the audit Parquet
directly because that is where it lives (ADR 0006).

Results are plain lists of dicts so this module does not depend on pandas; the
dashboard converts at the edge.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

MARTS = "main_marts"

#: The singular test whose outcome the dashboard reports.
RECONCILIATION_TEST = "assert_domain_totals_reconcile_with_projectviews"

#: dbt commands whose run_results.json contains real test outcomes.
VERDICT_COMMANDS = frozenset({"build", "test"})

Row = dict[str, Any]


def _rows(
    connection: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None
) -> list[Row]:
    cursor = connection.execute(sql, params or [])
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, values, strict=True)) for values in cursor.fetchall()]


@dataclass(frozen=True, slots=True)
class WarehouseSummary:
    hours_loaded: int
    first_hour: datetime | None
    last_hour: datetime | None
    domain_hours: int
    """Rows in the wide grain - each one reconciled against projectviews."""
    total_views: int
    company_views: int
    fact_rows: int

    @property
    def company_share(self) -> float:
        return self.company_views / self.total_views if self.total_views else 0.0


def summary(connection: duckdb.DuckDBPyConnection) -> WarehouseSummary:
    hours = _rows(
        connection,
        f"""
        SELECT count(*) AS n, min(ts) AS first_hour, max(ts) AS last_hour
        FROM (SELECT DISTINCT interval_start_utc AS ts FROM {MARTS}.agg_company_share_of_traffic)
        """,
    )[0]
    wiki = _rows(
        connection,
        f"SELECT coalesce(sum(hours_observed), 0) AS domain_hours, "
        f"coalesce(sum(lifetime_views), 0) AS total_views FROM {MARTS}.dim_wiki",
    )[0]
    fact = _rows(
        connection,
        f"SELECT count(*) AS n, coalesce(sum(views), 0) AS views "
        f"FROM {MARTS}.fct_company_pageviews_hourly",
    )[0]
    return WarehouseSummary(
        hours_loaded=int(hours["n"]),
        first_hour=hours["first_hour"],
        last_hour=hours["last_hour"],
        domain_hours=int(wiki["domain_hours"]),
        total_views=int(wiki["total_views"]),
        company_views=int(fact["views"]),
        fact_rows=int(fact["n"]),
    )


def companies(connection: duckdb.DuckDBPyConnection) -> list[Row]:
    return _rows(
        connection,
        f"""
        SELECT company_qid, company_name, language_editions, lifetime_views
        FROM {MARTS}.dim_company
        ORDER BY lifetime_views DESC
        """,
    )


def company_hourly(connection: duckdb.DuckDBPyConnection, qids: list[str]) -> list[Row]:
    """Views and share of all traffic, per company per hour."""
    if not qids:
        return []
    return _rows(
        connection,
        f"""
        SELECT interval_start_utc, company_qid, company_name,
               company_views, total_views, share_of_traffic
        FROM {MARTS}.agg_company_share_of_traffic
        WHERE company_qid IN (SELECT unnest(?::VARCHAR[]))
        ORDER BY interval_start_utc, company_name
        """,
        [qids],
    )


def language_breakdown(
    connection: duckdb.DuckDBPyConnection, qid: str, *, limit: int = 12
) -> list[Row]:
    """Top language editions for one company, with the long tail folded into 'Other'.

    Folded rather than truncated, so the bars still account for every view: a top-N that
    silently drops the tail overstates how concentrated attention is.
    """
    ranked = _rows(
        connection,
        f"""
        SELECT wiki_language AS language, sum(views)::BIGINT AS views
        FROM {MARTS}.fct_company_pageviews_hourly
        WHERE company_qid = ?
        GROUP BY wiki_language
        ORDER BY views DESC, language
        """,
        [qid],
    )
    head, tail = ranked[:limit], ranked[limit:]
    if tail:
        head.append(
            {
                "language": f"Other ({len(tail)} editions)",
                "views": sum(int(row["views"]) for row in tail),
            }
        )
    return head


def access_split(connection: duckdb.DuckDBPyConnection, qids: list[str]) -> list[Row]:
    """Share of each company's views from the mobile web, and via redirects."""
    if not qids:
        return []
    return _rows(
        connection,
        f"""
        SELECT company_qid, max(company_name) AS company_name,
               sum(views)::BIGINT AS views,
               sum(CASE WHEN access_method = 'mobile-web' THEN views ELSE 0 END)::DOUBLE
                   / nullif(sum(views), 0) AS mobile_share,
               sum(CASE WHEN match_type = 'redirect' THEN views ELSE 0 END)::DOUBLE
                   / nullif(sum(views), 0) AS redirect_share
        FROM {MARTS}.fct_company_pageviews_hourly
        WHERE company_qid IN (SELECT unnest(?::VARCHAR[]))
        GROUP BY company_qid
        ORDER BY mobile_share DESC
        """,
        [qids],
    )


def audit_attempts(
    connection: duckdb.DuckDBPyConnection, source: str, *, limit: int = 50
) -> list[Row]:
    """Most recent ingestion attempts, failures included.

    Reads with `union_by_name`. Audit files written before the schema was declared
    (see `wikiflow.audit.AUDIT_SCHEMA`) typed `error` by content - INTEGER for a
    success, VARCHAR for a failure - and those historical files still exist. The
    WRITER must be strict; a reader of history has to tolerate what history contains.
    """
    return _rows(
        connection,
        f"""
        SELECT started_at, interval_start, dag_id, try_number, status,
               CAST(error AS VARCHAR) AS error,
               rows_scanned, company_rows, total_views, duration_seconds, run_id
        FROM read_parquet('{source}', hive_partitioning = false, union_by_name = true)
        ORDER BY started_at DESC
        LIMIT {int(limit)}
        """,
    )


@dataclass(frozen=True, slots=True)
class ReconciliationStatus:
    status: str
    """`pass`, `fail`, `error`, or `not run`."""
    mismatches: int | None
    generated_at: str | None

    @property
    def passed(self) -> bool:
        return self.status == "pass"


def reconciliation_status(run_results: Path) -> ReconciliationStatus:
    """What dbt last recorded for the reconciliation test.

    Reads the artefact rather than re-running the comparison, so the dashboard reports
    the pipeline's own verdict - and reports its absence honestly.
    """
    if not run_results.exists():
        return ReconciliationStatus(status="not run", mismatches=None, generated_at=None)

    payload = json.loads(run_results.read_text(encoding="utf-8"))
    generated_at = payload.get("metadata", {}).get("generated_at")

    # Only `build` and `test` produce test VERDICTS. Other commands - `docs generate`,
    # `compile`, `run` - write the same file with every test at status "success", which
    # means compiled. Reading that as a verdict would report a check that never ran.
    command = payload.get("args", {}).get("which")
    if command not in VERDICT_COMMANDS:
        return ReconciliationStatus(status="not run", mismatches=None, generated_at=generated_at)

    for result in payload.get("results", []):
        if result.get("unique_id", "").endswith(f".{RECONCILIATION_TEST}"):
            return ReconciliationStatus(
                status=str(result.get("status", "error")),
                mismatches=result.get("failures"),
                generated_at=generated_at,
            )

    # dbt ran, but not this test - e.g. `dbt run` without tests, or a --select.
    return ReconciliationStatus(status="not run", mismatches=None, generated_at=generated_at)
