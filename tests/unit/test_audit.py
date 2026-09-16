"""Tests for audit records.

The schema test reproduces a real bug. Types used to be inferred, and DuckDB types a
bare NULL as INTEGER - so a successful run wrote `error INTEGER` and a failed run wrote
`error VARCHAR`. Each file was fine alone. Reading them together broke as soon as the
first failure was recorded, which is exactly when the audit table is needed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from wikiflow.audit import AUDIT_SCHEMA, IngestionAudit, audit_key, write_audit_parquet
from wikiflow.source import HourlyDump

DUMP = HourlyDump.for_interval(datetime(2026, 9, 14, 4, tzinfo=UTC))
STARTED = datetime(2026, 9, 16, 8, 0, tzinfo=UTC)


def record(**overrides: object) -> IngestionAudit:
    fields: dict[str, object] = {
        "run_id": "backfill__2026-09-14T04:00:00+00:00",
        "dag_id": "pageviews_ingest",
        "task_id": "ingest",
        "try_number": 1,
        "interval_start": DUMP.interval_start,
        "partition_date": DUMP.partition_date,
        "partition_hour": int(DUMP.partition_hour),
        "source_url": DUMP.pageviews_url,
        "status": "success",
        "started_at": STARTED,
        "finished_at": STARTED + timedelta(seconds=20),
    }
    fields.update(overrides)
    return IngestionAudit(**fields)  # type: ignore[arg-type]


@pytest.mark.unit
def test_success_and_failure_files_share_one_schema(tmp_path: Path) -> None:
    success = record(status="success", error=None, rows_scanned=7_451_970, total_views=19_322_047)
    failure = record(
        run_id="manual__retry",
        status="failed",
        error="HTTPStatusError: Client error '429 Too Many Requests'",
    )
    write_audit_parquet(success, tmp_path / "a.parquet")
    write_audit_parquet(failure, tmp_path / "b.parquet")

    connection = duckdb.connect()
    glob = (tmp_path / "*.parquet").as_posix()

    # Deliberately WITHOUT union_by_name: that option is what hid the bug.
    rows = connection.execute(
        f"SELECT status, error FROM read_parquet('{glob}') ORDER BY status"
    ).fetchall()

    assert rows == [
        ("failed", "HTTPStatusError: Client error '429 Too Many Requests'"),
        ("success", None),
    ]


@pytest.mark.unit
def test_every_column_has_its_declared_type(tmp_path: Path) -> None:
    """Checked on an all-defaults record, where inference would have been worst."""
    path = tmp_path / "a.parquet"
    write_audit_parquet(record(error=None), path)

    described = duckdb.connect().execute(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()
    actual = {name: kind for name, kind, *_ in described}

    normalise = {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE"}
    expected = {name: normalise.get(kind, kind) for name, kind in AUDIT_SCHEMA.items()}
    assert actual == expected
    assert [name for name, *_ in described] == list(AUDIT_SCHEMA), "column order drifted"


@pytest.mark.unit
def test_derived_fields_are_recorded(tmp_path: Path) -> None:
    path = tmp_path / "a.parquet"
    write_audit_parquet(record(company_views=5_138, total_views=19_322_047), path)

    duration, share = duckdb.connect().execute(
        f"SELECT duration_seconds, company_share FROM '{path.as_posix()}'"
    ).fetchone() or (None, None)

    assert duration == 20.0
    assert share == pytest.approx(5_138 / 19_322_047)


@pytest.mark.unit
def test_every_attempt_gets_its_own_key_so_history_accumulates() -> None:
    """Audit is append-only per ATTEMPT, not per run.

    Airflow retries share a run id. Keying on run id alone let attempt 3 overwrite
    attempts 1 and 2 - observed in the real audit data before this was fixed.
    """
    run_id = "backfill__2026-09-14T05:00:00+00:00"

    keys = {audit_key(DUMP, run_id, attempt) for attempt in (1, 2, 3)}
    assert len(keys) == 3, "retries of the same run must not collide"

    other_run = audit_key(DUMP, "manual__2026-09-16T08:07:42.314736+00:00", 1)
    assert other_run not in keys

    first = audit_key(DUMP, run_id, 1)
    assert first.startswith("ops/ingestion_audit/dt=2026-09-14/hour=04/")
    assert ":" not in first and "+" not in first, "run ids must be made key-safe"
