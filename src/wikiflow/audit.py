"""Recording what each ingestion run actually did.

Every run writes one row: where it read from, how many bytes and lines arrived, what
the checksum was, how many rows survived each stage, how long it took, and whether it
succeeded. Without this, "the pipeline is healthy" is an opinion.

Two design points worth stating, because both differ from how the data partitions work.

**Audit is append-only, data is overwrite.** A partition is replaced wholesale on
re-run (ADR 0002) so that re-running cannot double-count. Audit rows are the opposite:
each attempt writes its own file keyed by run id, so a partition that failed twice and
succeeded on the third try keeps all three records. Overwriting them would erase
exactly the history you want during an incident.

**It lives in object storage, not the warehouse.** DuckDB permits a single writer, so
having the ingest DAG write audit rows into the same file dbt builds marts in would
mean lock contention between two DAGs that are deliberately decoupled. Parquet in S3
has no such constraint, and dbt can still read it as a source.
"""

from __future__ import annotations

import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from wikiflow.config import get_settings
from wikiflow.source import HourlyDump
from wikiflow.storage import upload_file

AUDIT_PREFIX = "ops/ingestion_audit"


@dataclass(frozen=True, slots=True)
class IngestionAudit:
    """One attempt at ingesting one hour."""

    run_id: str
    dag_id: str
    task_id: str
    try_number: int

    # What was targeted
    interval_start: datetime
    partition_date: str
    partition_hour: int
    source_url: str

    # Outcome
    status: str
    """`success` or `failed`. Recorded either way - a run that failed silently is the
    case this table exists to make visible."""

    error: str | None = None

    # Download verification (see wikiflow.download)
    compressed_bytes: int = 0
    decompressed_bytes: int = 0
    line_count: int = 0
    sha256: str = ""

    # Extraction (see wikiflow.extract)
    rows_scanned: int = 0
    rows_malformed: int = 0
    company_rows: int = 0
    company_views: int = 0
    domain_rows: int = 0
    total_views: int = 0

    # Timing
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def company_share(self) -> float:
        return self.company_views / self.total_views if self.total_views else 0.0

    def as_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["duration_seconds"] = self.duration_seconds
        row["company_share"] = self.company_share
        return row


def audit_key(dump: HourlyDump, run_id: str) -> str:
    """Object key for one audit record.

    Keyed by run id rather than by partition, so retries accumulate instead of
    overwriting each other.
    """
    safe_run_id = "".join(c if c.isalnum() or c in "-_" else "-" for c in run_id)
    return f"{AUDIT_PREFIX}/{dump.partition_path}/run-{safe_run_id}.parquet"


def write_audit(record: IngestionAudit, dump: HourlyDump) -> str:
    """Persist one audit row as Parquet. Returns the object key written."""
    settings = get_settings()
    key = audit_key(dump, record.run_id)

    with tempfile.TemporaryDirectory(prefix="wikiflow-audit-") as tmpdir:
        path = Path(tmpdir) / "audit.parquet"
        connection = duckdb.connect()
        try:
            row = record.as_row()
            # DuckDB rejects prepared parameters inside COPY, so the row goes into a
            # temp table first and COPY reads from that. One INSERT of one row is
            # immaterial here - unlike the 2,238-row filter in extract.py, which had
            # to avoid INSERT entirely.
            placeholders = ", ".join("?" for _ in row)
            connection.execute(
                f"CREATE TEMP TABLE audit_row AS SELECT * FROM (VALUES ({placeholders})) "
                f"AS t({', '.join(row)})",
                list(row.values()),
            )
            connection.execute(
                f"COPY audit_row TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            connection.close()

        upload_file(path, settings.lake_bucket, key)

    return key
