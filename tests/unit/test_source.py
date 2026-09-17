"""Tests for the interval -> URL mapping.

The whole point of this suite is to pin down the one-hour shift established in
docs/adr/0001. Several tests below are written specifically so that they FAIL if
`LABEL_OFFSET` is ever set back to zero - because that change would otherwise
produce a pipeline that runs perfectly and attributes every row to the wrong hour.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from wikiflow.source import HourlyDump, dump_url_for, projectviews_url_for


def utc(year: int, month: int, day: int, hour: int) -> datetime:
    return datetime(year, month, day, hour, tzinfo=UTC)


# ---------------------------------------------------------------- the core shift


@pytest.mark.unit
def test_filename_label_is_one_hour_after_the_interval() -> None:
    """ADR 0001: the filename hour is the END of the period it covers."""
    dump = HourlyDump.for_interval(utc(2026, 9, 15, 14))

    assert dump.interval_start == utc(2026, 9, 15, 14)
    assert dump.label == utc(2026, 9, 15, 15)
    assert dump.pageviews_filename == "pageviews-20260915-150000.gz"


@pytest.mark.unit
def test_data_for_1400_is_not_fetched_from_the_1400_file() -> None:
    """Regression guard against 'simplifying away' the offset.

    This is the exact bug ADR 0001 exists to prevent, so it gets its own test with
    an explanatory name rather than being folded into the case above.
    """
    url = dump_url_for(utc(2026, 9, 15, 14))

    assert "pageviews-20260915-150000.gz" in url
    assert "pageviews-20260915-140000.gz" not in url


@pytest.mark.unit
def test_partition_keys_come_from_the_interval_not_the_label() -> None:
    """Partitions describe the data's real hour, not the filename's hour."""
    dump = HourlyDump.for_interval(utc(2026, 9, 15, 14))

    assert dump.partition_date == "2026-09-15"
    assert dump.partition_hour == "14"
    assert dump.partition_path == "dt=2026-09-15/hour=14"
    # The label is 15:00, and it must not leak into the partition.
    assert "hour=15" not in dump.partition_path


# ---------------------------------------------------------------- rollovers


@pytest.mark.unit
def test_last_hour_of_day_reads_from_the_next_days_file() -> None:
    dump = HourlyDump.for_interval(utc(2026, 9, 15, 23))

    assert dump.pageviews_filename == "pageviews-20260916-000000.gz"
    assert dump.partition_path == "dt=2026-09-15/hour=23"


@pytest.mark.unit
def test_last_hour_of_month_reads_from_the_next_months_directory() -> None:
    dump = HourlyDump.for_interval(utc(2026, 9, 30, 23))

    assert dump.pageviews_url == (
        "https://dumps.wikimedia.org/other/pageviews/2026/2026-10/pageviews-20261001-000000.gz"
    )
    assert dump.partition_path == "dt=2026-09-30/hour=23"


@pytest.mark.unit
def test_last_hour_of_year_reads_from_the_next_years_directory() -> None:
    """The nastiest case: directory, filename and partition disagree on year."""
    dump = HourlyDump.for_interval(utc(2026, 12, 31, 23))

    assert dump.pageviews_url == (
        "https://dumps.wikimedia.org/other/pageviews/2027/2027-01/pageviews-20270101-000000.gz"
    )
    assert dump.partition_date == "2026-12-31"
    assert dump.partition_hour == "23"


@pytest.mark.unit
def test_leap_day_is_handled() -> None:
    dump = HourlyDump.for_interval(utc(2028, 2, 28, 23))
    assert dump.pageviews_filename == "pageviews-20280229-000000.gz"


# ---------------------------------------------------------------- projectviews


@pytest.mark.unit
def test_projectviews_is_uncompressed_and_shares_the_label() -> None:
    """projectviews files have no .gz extension - a real difference, easy to miss."""
    dump = HourlyDump.for_interval(utc(2026, 9, 15, 14))

    assert dump.projectviews_filename == "projectviews-20260915-150000"
    assert not dump.projectviews_filename.endswith(".gz")
    assert projectviews_url_for(utc(2026, 9, 15, 14)).endswith("projectviews-20260915-150000")


@pytest.mark.unit
def test_both_datasets_share_one_partition() -> None:
    """The reconciliation test in Phase 2 depends on this alignment."""
    dump = HourlyDump.for_interval(utc(2026, 9, 15, 14))

    assert dump.raw_key("pageviews") == (
        "pageviews/dt=2026-09-15/hour=14/pageviews-20260915-150000.gz"
    )
    assert dump.raw_key("projectviews") == (
        "projectviews/dt=2026-09-15/hour=14/projectviews-20260915-150000"
    )


# ---------------------------------------------------------------- input validation


@pytest.mark.unit
def test_naive_datetime_is_rejected() -> None:
    """A naive datetime would be read as local time and fetch the wrong hour."""
    with pytest.raises(ValueError, match="timezone-aware"):
        HourlyDump.for_interval(datetime(2026, 9, 15, 14))


@pytest.mark.unit
def test_non_utc_input_is_converted_not_rejected() -> None:
    """Airflow may hand over an aware datetime in another zone; convert it."""
    tokyo = timezone(timedelta(hours=9))
    dump = HourlyDump.for_interval(datetime(2026, 9, 15, 23, tzinfo=tokyo))

    # 23:00 in UTC+9 is 14:00 UTC, so this is the same hour as the core test above.
    assert dump.interval_start == utc(2026, 9, 15, 14)
    assert dump.pageviews_filename == "pageviews-20260915-150000.gz"


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad",
    [
        datetime(2026, 9, 15, 14, 30, tzinfo=UTC),
        datetime(2026, 9, 15, 14, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 15, 14, 0, 0, 500, tzinfo=UTC),
    ],
)
def test_non_hour_boundary_is_rejected(bad: datetime) -> None:
    """A misaligned interval means the schedule is not hourly - fail loudly."""
    with pytest.raises(ValueError, match="hour boundary"):
        HourlyDump.for_interval(bad)


@pytest.mark.unit
def test_dump_is_immutable() -> None:
    dump = HourlyDump.for_interval(utc(2026, 9, 15, 14))
    with pytest.raises(AttributeError):
        dump.label = utc(2026, 9, 15, 16)  # type: ignore[misc]


# ---------------------------------------------------------------- live source shape


@pytest.mark.unit
def test_generated_url_matches_the_shape_observed_on_the_live_server() -> None:
    """Locks in the real layout confirmed against dumps.wikimedia.org on 2026-09-16.

    Deliberately a string equality rather than a pattern: if the upstream layout ever
    changes, this should fail rather than quietly keep passing.
    """
    assert dump_url_for(utc(2026, 9, 14, 11)) == (
        "https://dumps.wikimedia.org/other/pageviews/2026/2026-09/pageviews-20260914-120000.gz"
    )


@pytest.mark.unit
def test_reprocess_hours_are_inclusive_and_cross_midnight() -> None:
    from wikiflow.reprocess import hours

    got = list(hours(datetime(2026, 9, 14, 22, tzinfo=UTC), datetime(2026, 9, 15, 1, tzinfo=UTC)))
    assert [h.hour for h in got] == [22, 23, 0, 1]
    with pytest.raises(ValueError, match="before start"):
        list(hours(datetime(2026, 9, 15, tzinfo=UTC), datetime(2026, 9, 14, tzinfo=UTC)))
