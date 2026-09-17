"""Recording what each ingestion run actually did.

Every run writes one row: where it read from, how many bytes and lines arrived, what
the checksum was, how many rows survived each stage, how long it took, and whether it
succeeded. Without this, "the pipeline is healthy" is an opinion.

Two design points worth stating, because both differ from how the data partitions work.

**Audit is append-only, data is overwrite.** A partition is replaced wholesale on
re-run (ADR 0002) so that re-running cannot double-count. Audit rows are the opposite:
each attempt writes its own file keyed by run id and attempt number, so a partition that
failed twice and succeeded on the third try keeps all three records. Overwriting them
would erase exactly the history you want during an incident.

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

    filter_snapshot: str = ""
    """The company-filter snapshot extraction used (wikiflow.reference). Recorded so the
    exact filter behind any hour can be recovered after the filter has since changed."""

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


def audit_key(dump: HourlyDump, run_id: str, try_number: int) -> str:
    """Object key for one audit record.

    Keyed by run id AND attempt number. Run id alone is not enough: Airflow retries of
    the same run share a run id, so attempt 2 would overwrite attempt 1 - which is
    exactly the history the audit table exists to keep. (That is not hypothetical: the
    first version keyed on run id only, and a partition that failed three times kept
    only its third record.)
    """
    safe_run_id = "".join(c if c.isalnum() or c in "-_" else "-" for c in run_id)
    return f"{AUDIT_PREFIX}/{dump.partition_path}/run-{safe_run_id}-try{try_number}.parquet"


#: The declared schema of every audit file, in column order.
#:
#: Declared rather than inferred, and this is a fix rather than a preference. With
#: inference, DuckDB types a bare NULL as INTEGER, so a successful run (error=None)
#: wrote `error INTEGER` while a failed run wrote `error VARCHAR`. Each file was valid on
#: its own; the glob across them broke the moment the first failure landed - which is
#: precisely when someone needs to read the audit table.
AUDIT_SCHEMA: dict[str, str] = {
    "run_id": "VARCHAR",
    "dag_id": "VARCHAR",
    "task_id": "VARCHAR",
    "try_number": "INTEGER",
    "interval_start": "TIMESTAMPTZ",
    "partition_date": "DATE",
    "partition_hour": "INTEGER",
    "source_url": "VARCHAR",
    "status": "VARCHAR",
    "error": "VARCHAR",
    "filter_snapshot": "VARCHAR",
    "compressed_bytes": "BIGINT",
    "decompressed_bytes": "BIGINT",
    "line_count": "BIGINT",
    "sha256": "VARCHAR",
    "rows_scanned": "BIGINT",
    "rows_malformed": "BIGINT",
    "company_rows": "BIGINT",
    "company_views": "BIGINT",
    "domain_rows": "BIGINT",
    "total_views": "BIGINT",
    "started_at": "TIMESTAMPTZ",
    "finished_at": "TIMESTAMPTZ",
    "duration_seconds": "DOUBLE",
    "company_share": "DOUBLE",
}


def write_audit_parquet(record: IngestionAudit, path: Path) -> None:
    """Write one audit row to a local Parquet file with the declared schema."""
    row = record.as_row()
    if set(row) != set(AUDIT_SCHEMA):
        raise ValueError(
            "audit record fields and AUDIT_SCHEMA have drifted apart: "
            f"missing from schema {sorted(set(row) - set(AUDIT_SCHEMA))}, "
            f"missing from record {sorted(set(AUDIT_SCHEMA) - set(row))}"
        )

    connection = duckdb.connect()
    try:
        # DuckDB rejects prepared parameters inside COPY, so the row goes into a temp
        # table first. Each placeholder is CAST to its declared type, so a None becomes
        # a NULL of the RIGHT type instead of an INTEGER NULL.
        casts = ", ".join(f"CAST(? AS {kind}) AS {name}" for name, kind in AUDIT_SCHEMA.items())
        connection.execute(
            f"CREATE TEMP TABLE audit_row AS SELECT {casts}",
            [row[name] for name in AUDIT_SCHEMA],
        )
        connection.execute(
            f"COPY audit_row TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        connection.close()


def write_audit(record: IngestionAudit, dump: HourlyDump) -> str:
    """Persist one audit row to object storage. Returns the object key written."""
    settings = get_settings()
    key = audit_key(dump, record.run_id, record.try_number)

    with tempfile.TemporaryDirectory(prefix="wikiflow-audit-") as tmpdir:
        path = Path(tmpdir) / "audit.parquet"
        write_audit_parquet(record, path)
        upload_file(path, settings.lake_bucket, key)

    return key
