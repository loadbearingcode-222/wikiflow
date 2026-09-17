"""Asset definitions shared between the ingest and transform DAGs.

Assets are how the two DAGs are coupled: ingestion declares what it produced,
transformation declares what it consumes, and Airflow schedules the second when the
first completes. No cross-DAG sensor, and no cron offset guessing at when ingestion
will have finished - which matters here, because source publication latency varies by
about an hour and a half (ADR 0001).

Assets are identified by URI, so these constants exist to keep one spelling of each
URI rather than to create the asset; two DAGs naming the same URI already refer to the
same asset.
"""

from __future__ import annotations

from airflow.sdk import Asset

#: Written by pageviews_ingest, consumed by pageviews_transform.
BRONZE_COMPANY_PAGEVIEWS = Asset(
    "s3://wikiflow-lake/bronze/company_pageviews",
    extra={"grain": "narrow", "description": "Tracked company articles, one partition per hour."},
)

#: The denominator grain. Produced by the same task, so in practice both appear or
#: neither does; kept separate because they are genuinely different datasets.
BRONZE_DOMAIN_TOTALS = Asset(
    "s3://wikiflow-lake/bronze/domain_totals",
    extra={"grain": "wide", "description": "Per-domain hourly totals, the share denominator."},
)

#: Published once dbt has rebuilt the marts and every test has passed.
MARTS_READY = Asset(
    "duckdb://wikiflow/marts",
    extra={"description": "dbt marts rebuilt and all tests passing."},
)

#: The company filter: tracked articles and their redirects. Published daily by
#: company_pages_refresh and read by every ingest run (see wikiflow.reference).
REFERENCE_COMPANY_PAGES = Asset(
    "s3://wikiflow-lake/reference/company_pages",
    extra={"description": "Company articles and redirects the hourly filter matches."},
)

#: The same marts, built in the server warehouse by pageviews_transform_postgres.
#:
#: The URI names the fact table because it has to: the Postgres provider validates
#: `postgres://` URIs as host:port/database/schema/table and raises otherwise. Because
#: this module is shared, a shorter URI here once failed every DAG importing it -
#: ingestion included - not just the one that used it.
POSTGRES_MARTS_READY = Asset(
    "postgres://warehouse-postgres:5432/wikiflow/public_marts/fct_company_pageviews_hourly",
    extra={"description": "dbt marts rebuilt in Postgres and all tests passing."},
)
