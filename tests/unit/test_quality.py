"""Tests for the warehouse health checks.

The anomaly tests matter most. A seasonality-aware check is easy to write in a way that
looks right and is useless - either it flags every evening because traffic is higher
than the morning, or it flags nothing because the threshold absorbed the signal. These
tests pin both ends: a real dip IS caught, and the normal daily cycle is NOT.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest

from wikiflow import quality
from wikiflow.quality import check_freshness, detect_gaps, detect_volume_anomalies

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


@pytest.fixture
def fake_keys(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {"keys": []}

    def fake_list_keys(bucket: str, prefix: str) -> Iterator[str]:
        return (k for k in state["keys"] if k.startswith(prefix))

    monkeypatch.setattr(quality, "list_keys", fake_list_keys)
    return state


def bronze_keys(hours: list[tuple[str, int]]) -> list[str]:
    return [
        f"bronze/company_pageviews/dt={day}/hour={hour:02d}/part-0.parquet" for day, hour in hours
    ]


# ---------------------------------------------------------------- gaps


@pytest.mark.unit
def test_no_gaps_when_every_hour_is_present(fake_keys: dict[str, Any]) -> None:
    fake_keys["keys"] = bronze_keys([("2026-09-14", h) for h in range(3)])

    report = detect_gaps(datetime(2026, 9, 14, 0, tzinfo=UTC), datetime(2026, 9, 14, 2, tzinfo=UTC))

    assert report.is_complete
    assert report.present == 3
    assert "no gaps" in report.summary()


@pytest.mark.unit
def test_missing_hour_is_reported(fake_keys: dict[str, Any]) -> None:
    """A run that never happened raises nothing; only this check can see the hole."""
    fake_keys["keys"] = bronze_keys([("2026-09-14", 0), ("2026-09-14", 2)])

    report = detect_gaps(datetime(2026, 9, 14, 0, tzinfo=UTC), datetime(2026, 9, 14, 2, tzinfo=UTC))

    assert not report.is_complete
    assert report.missing == (datetime(2026, 9, 14, 1, tzinfo=UTC),)
    assert "2026-09-14 01:00" in report.summary()


@pytest.mark.unit
def test_gap_range_is_inclusive_of_both_ends(fake_keys: dict[str, Any]) -> None:
    fake_keys["keys"] = []

    report = detect_gaps(datetime(2026, 9, 14, 0, tzinfo=UTC), datetime(2026, 9, 14, 2, tzinfo=UTC))

    assert report.expected == 3


# ---------------------------------------------------------------- freshness


@pytest.mark.unit
def test_recent_partition_is_fresh(fake_keys: dict[str, Any]) -> None:
    fake_keys["keys"] = bronze_keys([("2026-09-16", 8)])

    report = check_freshness(max_age=timedelta(hours=8), now=NOW)

    assert report.is_fresh
    assert report.age == timedelta(hours=4)


@pytest.mark.unit
def test_stale_partition_is_not_fresh(fake_keys: dict[str, Any]) -> None:
    fake_keys["keys"] = bronze_keys([("2026-09-15", 0)])

    report = check_freshness(max_age=timedelta(hours=8), now=NOW)

    assert not report.is_fresh


@pytest.mark.unit
def test_threshold_tolerates_normal_publication_lag(fake_keys: dict[str, Any]) -> None:
    """Latency reaches ~4h past the end of the covered interval (ADR 0001).

    A threshold tighter than that would alert during healthy operation, which is how
    alerts get ignored.
    """
    fake_keys["keys"] = bronze_keys([("2026-09-16", 7)])  # 5h old

    assert check_freshness(max_age=timedelta(hours=8), now=NOW).is_fresh


@pytest.mark.unit
def test_empty_lake_is_not_fresh(fake_keys: dict[str, Any]) -> None:
    fake_keys["keys"] = []

    report = check_freshness(max_age=timedelta(hours=8), now=NOW)

    assert not report.is_fresh
    assert "no partitions found" in report.summary()


# ---------------------------------------------------------------- volume anomalies


def write_hourly(tmp_path: Path, rows: list[tuple[str, int, int]]) -> str:
    """Write (date, hour, views) as parquet shaped like bronze/domain_totals."""
    connection = duckdb.connect()
    values = ", ".join(f"(DATE '{d}', {h}, {v}::BIGINT)" for d, h, v in rows)
    target = tmp_path / "domain_totals.parquet"
    connection.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS t(dt, hour, views)) "
        f"TO '{target.as_posix()}' (FORMAT PARQUET)"
    )
    connection.close()
    return target.as_posix()


@pytest.mark.unit
def test_normal_daily_cycle_is_not_flagged(tmp_path: Path) -> None:
    """The whole point of comparing like-for-like hours.

    Traffic at 18:00 is roughly double 03:00 every single day. An hour-over-hour check
    would flag that twice a day forever; this one must not.
    """
    rows = []
    for day in range(5):
        for hour, views in ((3, 8_000_000), (18, 16_000_000)):
            rows.append((f"2026-09-{10 + day:02d}", hour, views))

    report = detect_volume_anomalies(source=write_hourly(tmp_path, rows), min_history=3)

    assert report.anomalies == ()
    assert not report.insufficient_history


@pytest.mark.unit
def test_a_genuinely_short_hour_is_flagged(tmp_path: Path) -> None:
    """A truncated download that still passed verification looks exactly like this."""
    rows = [(f"2026-09-{10 + day:02d}", 12, 12_000_000 + day * 50_000) for day in range(5)]
    rows.append(("2026-09-15", 12, 3_000_000))  # a quarter of normal

    report = detect_volume_anomalies(source=write_hourly(tmp_path, rows), min_history=3)

    assert len(report.anomalies) == 1
    flagged = report.anomalies[0]
    assert flagged.interval_start == datetime(2026, 9, 15, 12, tzinfo=UTC)
    assert flagged.deviation < 0, "a short hour should deviate downward"
    assert "3,000,000 views" in report.summary()


@pytest.mark.unit
def test_insufficient_history_is_reported_not_guessed(tmp_path: Path) -> None:
    """A standard deviation over two samples is not a signal.

    Reporting 'skipped, and here is why' is more honest than inventing a threshold,
    and avoids a false alarm on every hour of a fresh deployment.
    """
    rows = [("2026-09-14", 12, 10_000_000), ("2026-09-15", 12, 30_000_000)]

    report = detect_volume_anomalies(source=write_hourly(tmp_path, rows), min_history=3)

    assert report.insufficient_history
    assert report.anomalies == ()
    assert "insufficient history" in report.summary()


@pytest.mark.unit
def test_identical_hours_do_not_divide_by_zero(tmp_path: Path) -> None:
    """Zero variance is possible with synthetic or very stable data."""
    rows = [(f"2026-09-{10 + day:02d}", 12, 10_000_000) for day in range(5)]

    report = detect_volume_anomalies(source=write_hourly(tmp_path, rows), min_history=3)

    assert report.anomalies == ()


# ---------------------------------------------------------------- warehouse lag


@pytest.mark.unit
def test_postgres_within_threshold_is_current() -> None:
    from wikiflow.quality import WarehouseLagReport

    report = WarehouseLagReport(
        warehouse="postgres",
        bronze_latest=datetime(2026, 9, 17, 5, tzinfo=UTC),
        warehouse_latest=datetime(2026, 9, 17, 4, tzinfo=UTC),
        threshold=timedelta(hours=2),
    )
    assert report.is_current
    assert report.lag == timedelta(hours=1)


@pytest.mark.unit
def test_postgres_behind_by_more_than_threshold_fails() -> None:
    from wikiflow.quality import WarehouseLagReport

    report = WarehouseLagReport(
        warehouse="postgres",
        bronze_latest=datetime(2026, 9, 17, 5, tzinfo=UTC),
        warehouse_latest=datetime(2026, 9, 16, 22, tzinfo=UTC),
        threshold=timedelta(hours=2),
    )
    assert not report.is_current
    assert "lag 7:00:00" in report.summary()


@pytest.mark.unit
def test_empty_marts_with_data_in_bronze_is_not_current() -> None:
    """The case a lag in hours cannot express: the warehouse has nothing at all."""
    from wikiflow.quality import WarehouseLagReport

    report = WarehouseLagReport(
        warehouse="postgres",
        bronze_latest=datetime(2026, 9, 17, 5, tzinfo=UTC),
        warehouse_latest=None,
        threshold=timedelta(hours=2),
    )
    assert not report.is_current
    assert "EMPTY" in report.summary()
