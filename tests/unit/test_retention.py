"""Tests for the retention sweep.

This is the only module in the project whose failure mode is destroyed data rather than
wrong answers, so the tests here are mostly about what it REFUSES to do.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

import pytest

from wikiflow import retention
from wikiflow.retention import RetentionSafetyError, find_partitions, sweep


def keys_for(hours: list[tuple[str, int]]) -> list[str]:
    """Build raw object keys for a list of (date, hour) pairs."""
    out = []
    for day, hour in hours:
        out.append(f"pageviews/dt={day}/hour={hour:02d}/pageviews-{day.replace('-', '')}.gz")
        out.append(f"projectviews/dt={day}/hour={hour:02d}/projectviews-{day.replace('-', '')}")
    return out


@pytest.fixture
def fake_storage(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Replace S3 access with an in-memory store, recording deletions."""
    state: dict[str, Any] = {"keys": [], "deleted": []}

    def fake_list_keys(bucket: str, prefix: str) -> Iterator[str]:
        return (k for k in state["keys"] if k.startswith(prefix))

    def fake_delete_prefix(bucket: str, prefix: str) -> int:
        matched = [k for k in state["keys"] if k.startswith(prefix)]
        state["deleted"].append(prefix)
        state["keys"] = [k for k in state["keys"] if k not in matched]
        return len(matched)

    monkeypatch.setattr(retention, "list_keys", fake_list_keys)
    monkeypatch.setattr(retention, "delete_prefix", fake_delete_prefix)
    return state


NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


@pytest.mark.unit
def test_groups_objects_into_partitions(fake_storage: dict[str, Any]) -> None:
    fake_storage["keys"] = keys_for([("2026-09-14", 8), ("2026-09-14", 9)])

    partitions, unparseable = find_partitions("wikiflow-raw")

    assert unparseable == []
    # Two datasets x two hours.
    assert len(partitions) == 4
    assert {p.label for p in partitions} == {
        "pageviews/dt=2026-09-14/hour=08",
        "pageviews/dt=2026-09-14/hour=09",
        "projectviews/dt=2026-09-14/hour=08",
        "projectviews/dt=2026-09-14/hour=09",
    }


@pytest.mark.unit
def test_deletes_only_partitions_older_than_the_window(fake_storage: dict[str, Any]) -> None:
    # 10 days old -> expired; 2 days old -> retained.
    fake_storage["keys"] = keys_for([("2026-09-06", 0), ("2026-09-14", 0)])

    report = sweep(dry_run=False, retention_days=7, now=NOW)

    assert report.cutoff == date(2026, 9, 9)
    assert {p.partition_date for p in report.expired} == {date(2026, 9, 6)}
    assert report.retained == 2
    assert all("dt=2026-09-06" in prefix for prefix in fake_storage["deleted"])


@pytest.mark.unit
def test_dry_run_deletes_nothing(fake_storage: dict[str, Any]) -> None:
    fake_storage["keys"] = keys_for([("2026-09-01", 0), ("2026-09-14", 0)])

    report = sweep(dry_run=True, retention_days=7, now=NOW)

    assert report.partitions_expired == 2
    assert report.objects_deleted == 2, "dry run should still report what it would remove"
    assert fake_storage["deleted"] == []
    assert len(fake_storage["keys"]) == 4, "no keys should have been removed"


@pytest.mark.unit
def test_dry_run_is_the_default(fake_storage: dict[str, Any]) -> None:
    """Deleting must be opt-in. A mistaken call should be harmless."""
    fake_storage["keys"] = keys_for([("2026-09-01", 0), ("2026-09-14", 0)])

    sweep(retention_days=7, now=NOW)

    assert fake_storage["deleted"] == []


@pytest.mark.unit
def test_refuses_to_delete_every_partition(fake_storage: dict[str, Any]) -> None:
    """Everything expiring at once means the clock or window is wrong.

    Without this guard, a misconfigured retention_days=0 would silently empty the raw
    bucket and the failure would only surface the next time a reprocess was needed.
    """
    fake_storage["keys"] = keys_for([("2026-09-01", 0), ("2026-09-02", 0)])

    with pytest.raises(RetentionSafetyError, match="all 4 partitions"):
        sweep(dry_run=False, retention_days=1, now=NOW)

    assert fake_storage["deleted"] == []


@pytest.mark.unit
def test_unparseable_keys_are_reported_not_deleted(fake_storage: dict[str, Any]) -> None:
    """An unrecognised layout is a reason to stop and look, not to wildcard-delete."""
    fake_storage["keys"] = [
        *keys_for([("2026-09-14", 0)]),
        "pageviews/some-unexpected-layout.gz",
        "pageviews/dt=not-a-date/hour=99/x.gz",
    ]

    report = sweep(dry_run=False, retention_days=7, now=NOW)

    assert len(report.unparseable_keys) == 2
    assert fake_storage["deleted"] == []
    assert "pageviews/some-unexpected-layout.gz" in fake_storage["keys"]


@pytest.mark.unit
def test_never_looks_at_bronze_or_marts(fake_storage: dict[str, Any]) -> None:
    """Retention applies to raw only. Bronze is kept indefinitely (ADR 0003).

    The prefixes are separate buckets in practice, but the guard is asserted here so
    that pointing this at the lake bucket by mistake still cannot match anything.
    """
    fake_storage["keys"] = [
        "bronze/company_pageviews/dt=2020-01-01/hour=00/part-0.parquet",
        "bronze/domain_totals/dt=2020-01-01/hour=00/part-0.parquet",
        "ops/ingestion_audit/dt=2020-01-01/hour=00/run-x.parquet",
    ]

    report = sweep(dry_run=False, retention_days=7, now=NOW)

    assert report.partitions_expired == 0
    assert report.unparseable_keys == []
    assert fake_storage["deleted"] == []
    assert len(fake_storage["keys"]) == 3


@pytest.mark.unit
def test_empty_bucket_is_not_an_error(fake_storage: dict[str, Any]) -> None:
    """A fresh deployment has nothing to sweep; that is not a safety violation."""
    report = sweep(dry_run=False, retention_days=7, now=NOW)

    assert report.partitions_expired == 0
    assert report.retained == 0


@pytest.mark.unit
def test_manifest_names_what_it_touched(fake_storage: dict[str, Any]) -> None:
    """The manifest is the audit trail for a destructive action; it must be specific."""
    fake_storage["keys"] = keys_for([("2026-09-01", 3), ("2026-09-14", 0)])

    manifest = sweep(dry_run=True, retention_days=7, now=NOW).manifest()

    assert "would delete" in manifest
    assert "pageviews/dt=2026-09-01/hour=03" in manifest
    assert "cutoff 2026-09-09" in manifest
