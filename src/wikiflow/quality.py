"""Checks that answer "is the warehouse actually healthy right now?"

The dbt tests assert that the data is internally consistent and reconciles with
Wikimedia's own totals. These checks are about a different question: whether the data
is *there*, *recent*, and *plausible*.

Four checks, each catching a failure the others cannot:

* **Gaps** — a missing hour. The pipeline cannot report this itself, because a run that
  never happened produces no failure. Only something that looks across the range can
  see the hole.
* **Freshness** — ingestion silently stopped. Everything present is correct; there is
  just nothing new. A row-count check passes happily through this.
* **Volume anomaly** — an hour arrived but is the wrong size. This is the subtle one,
  and it is why the comparison is against *the same hour on previous days* rather than
  against the previous hour. Wikipedia traffic swings about 1.6x across a day, so a
  naive hour-over-hour check fires every morning and every evening, and people learn to
  ignore it.
* **Warehouse lag** — the Postgres marts stopped keeping up with bronze (ADR 0008). A
  failing transform DAG alerts on its own, but a DAG that is paused, or never triggered,
  fails nothing.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import duckdb

from wikiflow.config import get_settings
from wikiflow.storage import configure_duckdb_s3, list_keys

_PARTITION = re.compile(r"dt=(?P<dt>\d{4}-\d{2}-\d{2})/hour=(?P<hour>\d{2})/")

BRONZE_GRAINS = ("company_pageviews", "domain_totals")


class DataQualityError(RuntimeError):
    """A check failed in a way that should stop downstream consumers."""


@dataclass(frozen=True, slots=True)
class GapReport:
    expected: int
    present: int
    missing: tuple[datetime, ...]

    @property
    def is_complete(self) -> bool:
        return not self.missing

    def summary(self) -> str:
        if self.is_complete:
            return f"no gaps: {self.present}/{self.expected} hours present"
        shown = ", ".join(f"{ts:%Y-%m-%d %H:00}" for ts in self.missing[:12])
        more = "" if len(self.missing) <= 12 else f" (+{len(self.missing) - 12} more)"
        return f"{len(self.missing)}/{self.expected} hours MISSING: {shown}{more}"


@dataclass(frozen=True, slots=True)
class FreshnessReport:
    latest: datetime | None
    age: timedelta | None
    threshold: timedelta

    @property
    def is_fresh(self) -> bool:
        return self.age is not None and self.age <= self.threshold

    def summary(self) -> str:
        if self.latest is None:
            return "no partitions found at all"
        return (
            f"latest partition {self.latest:%Y-%m-%d %H:00} UTC, "
            f"age {self.age}, threshold {self.threshold}"
        )


@dataclass(frozen=True, slots=True)
class Anomaly:
    interval_start: datetime
    views: int
    expected: float
    sigma: float
    deviation: float


@dataclass(frozen=True, slots=True)
class AnomalyReport:
    checked: int
    anomalies: tuple[Anomaly, ...]
    insufficient_history: bool

    def summary(self) -> str:
        if self.insufficient_history:
            return (
                "insufficient history for a seasonality-aware check "
                "(needs several days of the same hour); skipped rather than guessed"
            )
        if not self.anomalies:
            return f"no volume anomalies across {self.checked} hours"
        worst = max(self.anomalies, key=lambda a: abs(a.deviation))
        return (
            f"{len(self.anomalies)}/{self.checked} hours anomalous; worst "
            f"{worst.interval_start:%Y-%m-%d %H:00} at {worst.deviation:+.1f} sigma "
            f"({worst.views:,} views vs expected {worst.expected:,.0f})"
        )


@dataclass(frozen=True, slots=True)
class WarehouseLagReport:
    warehouse: str
    bronze_latest: datetime | None
    warehouse_latest: datetime | None
    threshold: timedelta

    @property
    def lag(self) -> timedelta | None:
        """None when the warehouse has nothing at all while bronze does."""
        if self.bronze_latest is None:
            return timedelta(0)
        if self.warehouse_latest is None:
            return None
        return max(timedelta(0), self.bronze_latest - self.warehouse_latest)

    @property
    def is_current(self) -> bool:
        return self.lag is not None and self.lag <= self.threshold

    def summary(self) -> str:
        if self.bronze_latest is None:
            return f"{self.warehouse}: bronze is empty; nothing to lag behind"
        if self.warehouse_latest is None:
            return (
                f"{self.warehouse}: marts are EMPTY while bronze reaches "
                f"{self.bronze_latest:%Y-%m-%d %H:00}"
            )
        return (
            f"{self.warehouse}: newest hour {self.warehouse_latest:%Y-%m-%d %H:00}, bronze "
            f"{self.bronze_latest:%Y-%m-%d %H:00}, lag {self.lag}, threshold {self.threshold}"
        )


def check_postgres_lag(*, max_lag: timedelta, conninfo: str | None = None) -> WarehouseLagReport:
    """How far do the Postgres marts trail the newest bronze partition?

    Measured against bronze rather than the clock, so a stall in ingestion - already
    reported by the freshness check - is not reported a second time as warehouse lag.
    A marts table that does not exist yet counts as empty, not as an error.
    """
    import psycopg

    present = _partitions_present("company_pageviews")
    bronze_latest = max(present) if present else None

    warehouse_latest: datetime | None = None
    with psycopg.connect(conninfo or get_settings().pg_conninfo) as connection:
        exists = connection.execute(
            "select to_regclass('public_marts.fct_company_pageviews_hourly') is not null"
        ).fetchone()
        if exists and exists[0]:
            row = connection.execute(
                "select max(interval_start_utc) from public_marts.fct_company_pageviews_hourly"
            ).fetchone()
            if row and row[0] is not None:
                warehouse_latest = row[0].replace(tzinfo=UTC)

    return WarehouseLagReport(
        warehouse="postgres",
        bronze_latest=bronze_latest,
        warehouse_latest=warehouse_latest,
        threshold=max_lag,
    )


def _partitions_present(grain: str) -> set[datetime]:
    settings = get_settings()
    found: set[datetime] = set()
    for key in list_keys(settings.lake_bucket, f"bronze/{grain}/"):
        match = _PARTITION.search(key)
        if match is None:
            continue
        found.add(
            datetime.fromisoformat(match.group("dt")).replace(
                hour=int(match.group("hour")), tzinfo=UTC
            )
        )
    return found


def detect_gaps(start: datetime, end: datetime, *, grain: str = "company_pageviews") -> GapReport:
    """Find hours with no partition between `start` and `end`, inclusive of both."""
    present = _partitions_present(grain)

    expected: list[datetime] = []
    cursor = start
    while cursor <= end:
        expected.append(cursor)
        cursor += timedelta(hours=1)

    missing = tuple(ts for ts in expected if ts not in present)
    return GapReport(
        expected=len(expected),
        present=len(expected) - len(missing),
        missing=missing,
    )


def check_freshness(
    *, max_age: timedelta, now: datetime | None = None, grain: str = "company_pageviews"
) -> FreshnessReport:
    """How old is the newest partition?

    The threshold must account for source publication latency, which reaches nearly
    4 hours past the end of the interval a file covers (ADR 0001). A threshold tighter
    than that alerts on healthy operation.
    """
    present = _partitions_present(grain)
    reference = now or datetime.now(UTC)

    if not present:
        return FreshnessReport(latest=None, age=None, threshold=max_age)

    latest = max(present)
    return FreshnessReport(latest=latest, age=reference - latest, threshold=max_age)


def detect_volume_anomalies(
    *,
    sigma_threshold: float = 4.0,
    min_history: int = 3,
    connection: duckdb.DuckDBPyConnection | None = None,
    source: str | None = None,
) -> AnomalyReport:
    """Flag hours whose total views deviate from the same hour on other days.

    Compares each hour against the mean and standard deviation of the SAME hour-of-day
    across every other day present. That is what makes the check survive the daily
    cycle; comparing against adjacent hours would flag every dawn and dusk.

    `min_history` is the number of other days required before an hour is judged at all.
    With less than that, the report says so instead of inventing a threshold from two
    points - a standard deviation over two samples is not a useful signal.

    `source` overrides where to read from, so the logic can be tested against local
    parquet without object storage.
    """
    settings = get_settings()
    owns = connection is None
    connection = connection or duckdb.connect()
    if owns:
        configure_duckdb_s3(connection)

    location = source or (f"s3://{settings.lake_bucket}/bronze/domain_totals/dt=*/hour=*/*.parquet")

    try:
        rows = connection.execute(
            f"""
            SELECT dt, hour, sum(views)::BIGINT AS views
            FROM read_parquet('{location}', hive_partitioning = false)
            GROUP BY dt, hour
            ORDER BY dt, hour
            """
        ).fetchall()
    finally:
        if owns:
            connection.close()

    by_hour_of_day: dict[int, list[tuple[datetime, int]]] = {}
    for day, hour, views in rows:
        ts = datetime(day.year, day.month, day.day, int(hour), tzinfo=UTC)
        by_hour_of_day.setdefault(int(hour), []).append((ts, int(views)))

    anomalies: list[Anomaly] = []
    checked = 0
    judged_any = False

    for samples in by_hour_of_day.values():
        if len(samples) <= min_history:
            continue
        judged_any = True
        for ts, views in samples:
            others = [v for other_ts, v in samples if other_ts != ts]
            mean = statistics.fmean(others)
            spread = statistics.stdev(others)
            checked += 1
            if spread == 0:
                continue
            deviation = (views - mean) / spread
            if abs(deviation) > sigma_threshold:
                anomalies.append(
                    Anomaly(
                        interval_start=ts,
                        views=views,
                        expected=mean,
                        sigma=spread,
                        deviation=deviation,
                    )
                )

    return AnomalyReport(
        checked=checked,
        anomalies=tuple(sorted(anomalies, key=lambda a: a.interval_start)),
        insufficient_history=not judged_any,
    )
