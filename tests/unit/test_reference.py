"""The published company filter: the shrink guard and the Parquet round-trip."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from wikiflow.reference import (
    FilterCounts,
    ReferenceShrinkError,
    check_not_shrunk,
    read_pages_parquet,
    snapshot_key,
    write_pages_parquet,
)
from wikiflow.wikidata import CompanyPage


@pytest.mark.unit
def test_first_publication_has_nothing_to_compare_against() -> None:
    check_not_shrunk(None, FilterCounts(articles=2240, total=8742))


@pytest.mark.unit
def test_an_empty_filter_is_never_published() -> None:
    with pytest.raises(ReferenceShrinkError, match="no articles"):
        check_not_shrunk(None, FilterCounts(articles=0, total=0))


@pytest.mark.unit
def test_normal_churn_is_accepted() -> None:
    check_not_shrunk(
        FilterCounts(articles=2240, total=8742), FilterCounts(articles=2236, total=8700)
    )


@pytest.mark.unit
def test_losing_the_redirects_is_refused_even_if_articles_survive() -> None:
    """A MediaWiki outage leaves the Wikidata articles intact and drops only redirects."""
    with pytest.raises(ReferenceShrinkError, match="rows fell from 8,742 to 2,240"):
        check_not_shrunk(
            FilterCounts(articles=2240, total=8742), FilterCounts(articles=2240, total=2240)
        )


@pytest.mark.unit
def test_losing_articles_is_refused() -> None:
    with pytest.raises(ReferenceShrinkError, match="article rows"):
        check_not_shrunk(
            FilterCounts(articles=2240, total=8742), FilterCounts(articles=1100, total=8000)
        )


@pytest.mark.unit
def test_snapshot_keys_are_utc_and_sort_chronologically() -> None:
    local = datetime(2026, 9, 17, 9, 30, tzinfo=timezone(timedelta(hours=7)))
    assert snapshot_key(local).endswith("/snapshots/20260917T023000Z.parquet")
    assert snapshot_key(datetime(2026, 9, 17, 2, tzinfo=UTC)) < snapshot_key(
        datetime(2026, 9, 18, 1, tzinfo=UTC)
    )


@pytest.mark.unit
def test_parquet_round_trip_preserves_awkward_titles(tmp_path: Path) -> None:
    """Titles containing `"` broke the CSV step on real data (Hebrew abbreviation marks)."""
    pages = [
        CompanyPage(
            qid="Q20800404",
            company_name="Alphabet Inc.",
            sitelink_key="hewiki",
            language="he",
            project="wikipedia",
            page_title='אלפבת_בע"מ',
            domain_code="he",
            access_method="desktop",
            match_type="redirect",
            canonical_title="אלפאבית_(חברה)",
            site_host="he.wikipedia.org",
        ),
        CompanyPage(
            qid="Q312",
            company_name="Apple Inc.",
            sitelink_key="commonswiki",
            language=None,
            project="wikimedia",
            page_title="Category:Apple,_Inc.",
            domain_code="commons.m",
            access_method="desktop",
            canonical_title="Category:Apple,_Inc.",
            site_host="commons.wikimedia.org",
        ),
    ]
    path = tmp_path / "filter.parquet"
    write_pages_parquet(pages, path)
    assert sorted(read_pages_parquet(path), key=lambda p: p.qid) == sorted(
        pages, key=lambda p: p.qid
    )
