# ADR 0002 — No stored watermark; the partition is the state

- **Status:** Accepted
- **Date:** 2026-09-16
- **Decision:** Incremental state lives in the data's own location. There is no cursor
  table, no `updated_at` filter, and no change-data-capture.

## Context

Incremental ingestion needs an answer to "what is new?". The usual answers each carry
a cost:

| Approach | Failure it introduces |
|---|---|
| Stored high-watermark | A crash window between "data committed" and "watermark advanced" - one ordering duplicates, the other loses rows |
| `updated_at >` filter | Late-arriving rows land behind the watermark and are never seen; deletes are invisible |
| Snapshot diffing | Requires keeping the previous snapshot, and cost scales with the whole table |
| CDC | Correct, but needs a replication slot and a streaming stack |

The Wikimedia dumps avoid the question entirely. Files are **immutable** and
**partitioned by time** at the source, so the Airflow data interval already identifies
exactly one file.

## Decision

`data_interval_start` **is** the watermark.

1. Every task writes exactly one partition and replaces it wholesale
   (`storage.overwrite_partition` - delete-then-write). There is no append path, so a
   re-run cannot double-count regardless of how a previous attempt failed.
2. Backfill runs the same code as live ingestion. No separate backfill script exists.
3. A gap is a missing directory, which is detectable, rather than a silent absence.

## Consequences

- **Re-running is free.** Verified in the running stack: two full runs of
  2026-09-14 11:00 produced one object per partition, 379 company rows and 5,138
  company views both times.
- **No state to corrupt or migrate.** Nothing has to be reset when the schema changes.
- **This does not generalise.** A mutable source - an OLTP table, an API returning
  current state - cannot use this pattern and needs a real watermark or CDC. The
  pattern is available here because the source was chosen for it.
- `max_active_runs` must stay bounded, since two runs writing the same partition would
  race. It is asserted in the DAG integrity tests.
