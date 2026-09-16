"""Tests for the dashboard's read-side queries, against a tiny hand-built warehouse."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from wikiflow import metrics


@pytest.fixture
def warehouse() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA main_marts")
    connection.execute(
        """
        CREATE TABLE main_marts.fct_company_pageviews_hourly AS SELECT * FROM (VALUES
          ('Q312', 'Apple Inc.', 'en', 'desktop',    100),
          ('Q312', 'Apple Inc.', 'en', 'mobile-web', 300),
          ('Q312', 'Apple Inc.', 'de', 'mobile-web',  50),
          ('Q312', 'Apple Inc.', 'fr', 'desktop',     20),
          ('Q312', 'Apple Inc.', 'ja', 'desktop',     10),
          ('Q95',  'Google',     'en', 'desktop',    200)
        ) AS t(company_qid, company_name, wiki_language, access_method, views)
        """
    )
    connection.execute(
        """
        CREATE TABLE main_marts.agg_company_share_of_traffic AS SELECT * FROM (VALUES
          (TIMESTAMP '2026-09-14 00:00', 'Q312', 'Apple Inc.', 480, 1000, 0.48),
          (TIMESTAMP '2026-09-14 01:00', 'Q312', 'Apple Inc.',   0, 1000, 0.00),
          (TIMESTAMP '2026-09-14 00:00', 'Q95',  'Google',     200, 1000, 0.20)
        ) AS t(interval_start_utc, company_qid, company_name, company_views,
               total_views, share_of_traffic)
        """
    )
    connection.execute(
        """
        CREATE TABLE main_marts.dim_wiki AS SELECT * FROM (VALUES
          ('en', 2, 1500), ('de', 2, 500)
        ) AS t(domain_code, hours_observed, lifetime_views)
        """
    )
    connection.execute(
        """
        CREATE TABLE main_marts.dim_company AS SELECT * FROM (VALUES
          ('Q312', 'Apple Inc.', 4, 480), ('Q95', 'Google', 1, 200)
        ) AS t(company_qid, company_name, language_editions, lifetime_views)
        """
    )
    return connection


@pytest.mark.unit
def test_summary_counts_distinct_hours_and_reconciled_rows(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    summary = metrics.summary(warehouse)

    assert summary.hours_loaded == 2
    assert summary.first_hour == datetime(2026, 9, 14, 0)
    assert summary.last_hour == datetime(2026, 9, 14, 1)
    assert summary.domain_hours == 4
    assert summary.total_views == 2000
    assert summary.company_views == 680
    assert summary.company_share == pytest.approx(0.34)


@pytest.mark.unit
def test_company_hourly_filters_to_the_selection(warehouse: duckdb.DuckDBPyConnection) -> None:
    rows = metrics.company_hourly(warehouse, ["Q95"])

    assert [r["company_name"] for r in rows] == ["Google"]
    assert metrics.company_hourly(warehouse, []) == []


@pytest.mark.unit
def test_language_tail_is_folded_into_other_not_dropped(
    warehouse: duckdb.DuckDBPyConnection,
) -> None:
    """A top-N that drops the tail overstates how concentrated attention is."""
    rows = metrics.language_breakdown(warehouse, "Q312", limit=2)

    assert rows[:2] == [{"language": "en", "views": 400}, {"language": "de", "views": 50}]
    assert rows[2] == {"language": "Other (2 editions)", "views": 30}
    assert sum(r["views"] for r in rows) == 480, "every view must still be accounted for"


@pytest.mark.unit
def test_no_other_bucket_when_nothing_is_folded(warehouse: duckdb.DuckDBPyConnection) -> None:
    rows = metrics.language_breakdown(warehouse, "Q312", limit=10)

    assert not any(r["language"].startswith("Other") for r in rows)


@pytest.mark.unit
def test_mobile_share_per_company(warehouse: duckdb.DuckDBPyConnection) -> None:
    rows = {r["company_qid"]: r for r in metrics.access_split(warehouse, ["Q312", "Q95"])}

    assert rows["Q312"]["mobile_share"] == pytest.approx(350 / 480)
    assert rows["Q95"]["mobile_share"] == 0.0


def write_run_results(path: Path, results: list[dict[str, object]], command: str = "build") -> Path:
    path.write_text(
        json.dumps(
            {
                "metadata": {"generated_at": "2026-09-16T16:23:30Z"},
                "args": {"which": command},
                "results": results,
            }
        ),
        encoding="utf-8",
    )
    return path


@pytest.mark.unit
def test_reconciliation_status_reads_dbts_own_verdict(tmp_path: Path) -> None:
    path = write_run_results(
        tmp_path / "run_results.json",
        [
            {"unique_id": "test.wikiflow.not_null_x", "status": "pass", "failures": 0},
            {
                "unique_id": f"test.wikiflow.{metrics.RECONCILIATION_TEST}",
                "status": "fail",
                "failures": 3,
            },
        ],
    )

    status = metrics.reconciliation_status(path)

    assert status.status == "fail"
    assert status.mismatches == 3
    assert not status.passed


@pytest.mark.unit
def test_missing_artefact_is_reported_not_assumed(tmp_path: Path) -> None:
    """No run_results means no evidence. The dashboard must not show a green tick."""
    status = metrics.reconciliation_status(tmp_path / "absent.json")

    assert status.status == "not run"
    assert not status.passed


@pytest.mark.unit
def test_dbt_run_without_the_test_is_not_a_pass(tmp_path: Path) -> None:
    path = write_run_results(
        tmp_path / "run_results.json",
        [{"unique_id": "model.wikiflow.dim_company", "status": "success", "failures": None}],
    )

    assert metrics.reconciliation_status(path).status == "not run"


@pytest.mark.unit
def test_docs_generate_output_is_not_mistaken_for_a_verdict(tmp_path: Path) -> None:
    """Reproduces a real bug.

    `dbt docs generate` ran after `dbt build` and overwrote run_results.json. In its copy
    the reconciliation test had status "success" - meaning compiled - and no failure
    count. The dashboard displayed that as the reconciliation result.
    """
    path = write_run_results(
        tmp_path / "run_results.json",
        [
            {
                "unique_id": f"test.wikiflow.{metrics.RECONCILIATION_TEST}",
                "status": "success",
                "failures": None,
            }
        ],
        command="generate",
    )

    status = metrics.reconciliation_status(path)

    assert status.status == "not run"
    assert not status.passed
