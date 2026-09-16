"""Mapping from an Airflow data interval to the Wikimedia dump that covers it.

This module exists to contain one dangerous off-by-one in exactly one place.

The filename hour is the END of the aggregation period, not the start (proven
empirically in docs/adr/0001). So the file covering the interval 14:00-14:59 is
named `pageviews-...-150000.gz`. Two different hours are therefore in play at once:

    interval_start  the hour the data actually describes  -> partition keys
    label           the hour in the filename              -> URL

Mixing them up shifts every row by an hour, silently, in a way no schema test would
catch. `HourlyDump` derives both from a single input so they cannot drift apart, and
callers never compute either by hand.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"

# See docs/adr/0001. Changing this shifts the entire warehouse by an hour.
LABEL_OFFSET = timedelta(hours=1)


def _validate(interval_start: datetime) -> datetime:
    """Normalise to UTC, rejecting inputs that would produce a wrong-but-plausible URL."""
    if interval_start.tzinfo is None:
        raise ValueError(
            "interval_start must be timezone-aware. A naive datetime would be "
            "interpreted as local time and silently fetch the wrong hour."
        )

    normalised = interval_start.astimezone(UTC)

    if (normalised.minute, normalised.second, normalised.microsecond) != (0, 0, 0):
        raise ValueError(
            f"interval_start must be on an exact hour boundary, got {normalised.isoformat()}. "
            "A non-aligned value means the DAG schedule is not hourly, which this source "
            "does not support."
        )

    return normalised


@dataclass(frozen=True)
class HourlyDump:
    """Everything derived from one hourly data interval.

    Construct with `HourlyDump.for_interval(...)` rather than directly, so the
    interval/label relationship is always established the same way.
    """

    interval_start: datetime
    """The hour the data describes. All partition keys derive from this."""

    label: datetime
    """The hour in the filename: interval_start + 1h. Only the URL derives from this."""

    @classmethod
    def for_interval(cls, interval_start: datetime) -> HourlyDump:
        normalised = _validate(interval_start)
        return cls(interval_start=normalised, label=normalised + LABEL_OFFSET)

    # --- source URLs (derived from `label`) ---------------------------------

    @property
    def _directory(self) -> str:
        # Directories follow the LABEL, so the last hour of a month lives in the
        # next month's directory. See ADR 0001 consequence 2.
        return f"{BASE_URL}/{self.label:%Y}/{self.label:%Y-%m}"

    @property
    def pageviews_filename(self) -> str:
        return f"pageviews-{self.label:%Y%m%d}-{self.label:%H}0000.gz"

    @property
    def projectviews_filename(self) -> str:
        # Note: no .gz - projectviews files are published uncompressed.
        return f"projectviews-{self.label:%Y%m%d}-{self.label:%H}0000"

    @property
    def pageviews_url(self) -> str:
        return f"{self._directory}/{self.pageviews_filename}"

    @property
    def projectviews_url(self) -> str:
        return f"{self._directory}/{self.projectviews_filename}"

    # --- partition keys (derived from `interval_start`) ---------------------

    @property
    def partition_date(self) -> str:
        return f"{self.interval_start:%Y-%m-%d}"

    @property
    def partition_hour(self) -> str:
        return f"{self.interval_start:%H}"

    @property
    def partition_path(self) -> str:
        """Hive-style partition path, used identically in raw and bronze layers."""
        return f"dt={self.partition_date}/hour={self.partition_hour}"

    def raw_key(self, dataset: str) -> str:
        """Object key for a raw file, e.g. `pageviews/dt=2026-09-15/hour=14/pageviews-...gz`."""
        filename = {
            "pageviews": self.pageviews_filename,
            "projectviews": self.projectviews_filename,
        }[dataset]
        return f"{dataset}/{self.partition_path}/{filename}"


def dump_url_for(interval_start: datetime) -> str:
    """Convenience wrapper: the pageviews URL covering `interval_start`."""
    return HourlyDump.for_interval(interval_start).pageviews_url


def projectviews_url_for(interval_start: datetime) -> str:
    """Convenience wrapper: the projectviews URL covering `interval_start`."""
    return HourlyDump.for_interval(interval_start).projectviews_url
