"""Resolve the company filter - articles and their redirects - and publish it.

The hourly ingest matches dump rows against this filter. Resolving it means one
Wikidata call for the articles, then one MediaWiki call per wiki (~315 in total) for
the redirects to them, which is why it runs daily here rather than in every ingest run.

If the new filter has lost more than 10% of the current one's rows, publishing refuses
and the current filter stays in use (wikiflow.reference). A failed refresh therefore
costs nothing but staleness, which for article titles is measured in weeks.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

from wikiflow.redirects import resolve_filter
from wikiflow.reference import FilterCounts, publish
from wikiflow_airflow.alerts import notify_failure
from wikiflow_airflow.assets import REFERENCE_COMPANY_PAGES

COMPANY_SEED = Path("/opt/airflow/seeds/companies.csv")


@dag(
    dag_id="company_pages_refresh",
    description="Resolve tracked companies into articles and redirects, and publish the filter",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "wikiflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=15),
        "on_failure_callback": notify_failure,
    },
    tags=["wikiflow", "reference", "ops"],
    doc_md=__doc__,
)
def company_pages_refresh() -> None:
    # One task, not resolve >> publish: passing ~9,000 rows between tasks would go
    # through XCom in the metadata database, for no gain in retry granularity.
    @task(outlets=[REFERENCE_COMPANY_PAGES])
    def resolve_and_publish() -> dict[str, object]:
        pages = resolve_filter(COMPANY_SEED)
        key = publish(pages)
        counts = FilterCounts.of(pages)
        return {"snapshot": key, "articles": counts.articles, "rows": counts.total}

    resolve_and_publish()


company_pages_refresh()
