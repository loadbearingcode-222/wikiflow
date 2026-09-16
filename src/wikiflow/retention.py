"""Deleting raw files once they age out of the replay window.

Raw dumps are 45-70 MB an hour, so keeping them forever costs roughly 500 GB a year.
Keeping none of them is worse: the first parsing bug then costs a full re-download.
Seven days is the compromise (ADR 0003) - long enough to fix a bug and reprocess every
affected hour from object storage, which measured at about 4 seconds per hour.

This is the one module in the project whose bugs destroy data rather than produce wrong
answers, so it is deliberately conservative:

* It refuses to operate on any bucket or prefix other than raw. Bronze and the marts
  are kept indefinitely and this code must not be able to touch them, even by
  misconfiguration.
* It refuses to delete a partition it cannot parse a date out of. An unrecognised key
  is left alone and reported, never swept up by a wildcard.
* It refuses to delete everything. A sweep that would remove every partition means the
  cutoff is wrong, not that all the data expired at once.
* `dry_run` is the default. Deleting requires asking for it explicitly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from wikiflow.config import get_settings
from wikiflow.storage import delete_prefix, list_keys

#: Only these prefixes may ever be swept. Bronze and marts are never eligible.
SWEEPABLE_PREFIXES = ("pageviews/", "projectviews/")

_PARTITION = re.compile(r"(?P<prefix>[^/]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hour=(?P<hour>\d{2})/")


class RetentionSafetyError(RuntimeError):
    """A sweep was refused because it looked wrong rather than merely large."""


@dataclass(frozen=True, slots=True)
class Partition:
    dataset: str
    partition_date: date
    hour: int
    prefix: str
    keys: tuple[str, ...]

    @property
    def label(self) -> str:
        return f"{self.dataset}/dt={self.partition_date}/hour={self.hour:02d}"


@dataclass
class SweepReport:
    """What a sweep did, or would have done."""

    cutoff: date
    dry_run: bool
    expired: list[Partition] = field(default_factory=list)
    retained: int = 0
    unparseable_keys: list[str] = field(default_factory=list)
    objects_deleted: int = 0

    @property
    def partitions_expired(self) -> int:
        return len(self.expired)

    def manifest(self) -> str:
        """Human-readable summary, written to the task log on every run."""
        verb = "would delete" if self.dry_run else "deleted"
        lines = [
            f"retention sweep (cutoff {self.cutoff}, dry_run={self.dry_run})",
            f"  {verb}: {self.partitions_expired} partitions, {self.objects_deleted} objects",
            f"  retained: {self.retained} partitions",
        ]
        if self.unparseable_keys:
            lines.append(f"  UNPARSEABLE (left alone): {len(self.unparseable_keys)}")
            lines.extend(f"    {key}" for key in self.unparseable_keys[:10])
        lines.extend(f"    {partition.label}" for partition in self.expired[:50])
        if self.partitions_expired > 50:
            lines.append(f"    ... and {self.partitions_expired - 50} more")
        return "\n".join(lines)


def find_partitions(bucket: str) -> tuple[list[Partition], list[str]]:
    """Group every raw object into partitions. Returns (partitions, unparseable keys)."""
    grouped: dict[tuple[str, date, int], list[str]] = {}
    unparseable: list[str] = []

    for dataset_prefix in SWEEPABLE_PREFIXES:
        for key in list_keys(bucket, dataset_prefix):
            match = _PARTITION.match(key)
            if match is None:
                unparseable.append(key)
                continue
            parsed = (
                match.group("prefix"),
                date.fromisoformat(match.group("dt")),
                int(match.group("hour")),
            )
            grouped.setdefault(parsed, []).append(key)

    partitions = [
        Partition(
            dataset=dataset,
            partition_date=partition_date,
            hour=hour,
            prefix=f"{dataset}/dt={partition_date}/hour={hour:02d}/",
            keys=tuple(sorted(keys)),
        )
        for (dataset, partition_date, hour), keys in sorted(grouped.items())
    ]
    return partitions, unparseable


def sweep(
    *,
    dry_run: bool = True,
    retention_days: int | None = None,
    now: datetime | None = None,
) -> SweepReport:
    """Delete raw partitions older than the retention window.

    Args:
        dry_run: When True (the default) nothing is deleted; the report says what would
            have been.
        retention_days: Overrides the configured window. Mainly for tests.
        now: Overrides the clock. Mainly for tests.

    Raises:
        RetentionSafetyError: if the sweep would delete every partition, which means the
            cutoff is wrong rather than that everything genuinely expired.
    """
    settings = get_settings()
    days = retention_days if retention_days is not None else settings.raw_retention_days
    reference = now or datetime.now(UTC)
    cutoff = (reference - timedelta(days=days)).date()

    partitions, unparseable = find_partitions(settings.raw_bucket)
    expired = [p for p in partitions if p.partition_date < cutoff]

    report = SweepReport(
        cutoff=cutoff,
        dry_run=dry_run,
        expired=expired,
        retained=len(partitions) - len(expired),
        unparseable_keys=unparseable,
    )

    if partitions and len(expired) == len(partitions):
        raise RetentionSafetyError(
            f"refusing to sweep: all {len(partitions)} partitions are older than {cutoff}. "
            "That means the retention window or the clock is wrong, not that every hour "
            "expired at once."
        )

    if dry_run:
        report.objects_deleted = sum(len(p.keys) for p in expired)
        return report

    for partition in expired:
        report.objects_deleted += delete_prefix(settings.raw_bucket, partition.prefix)

    return report
