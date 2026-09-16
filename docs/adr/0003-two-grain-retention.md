# ADR 0003 — Two grains per hour, and a 7-day raw retention window

- **Status:** Accepted
- **Date:** 2026-09-16
- **Decision:** Each hour produces a narrow company grain and a wide per-domain grain,
  both kept indefinitely. Raw files are deleted after 7 days.

## Context

The tracked companies are a vanishingly small slice of the data: 379 rows out of
7.45M, and 5,138 views out of 19,322,047 - about **0.027%**.

Keeping only those rows would make the warehouse tiny but the analysis impossible.
"Apple got 12,000 views this hour" is unanswerable as a question about attention
without a denominator; it could be a spike or a quiet Tuesday.

Keeping every raw file is the other extreme: at 45-70 MB per hour that is roughly
**500 GB per year**, which is not reasonable on a laptop.

## Decision

One scan, two outputs:

| Grain | Content | Rows/hour | Size |
|---|---|---|---|
| narrow | rows matching a tracked company page | ~379 | ~7 KB |
| wide | `sum(count_views)` by `domain_code` | ~1,955 | ~16 KB |

Plus a retention policy:

| Layer | Retention | Steady state |
|---|---|---|
| Raw `.gz` | 7 days | ~10 GB |
| Bronze Parquet | indefinite | ~2 GB/year |

## Consequences

- **Share-of-traffic becomes expressible**, which is the actually interesting metric.
- **The wide grain doubles as a data-quality oracle.** It matches the `projectviews`
  file exactly - verified across all 1,955 domain codes with zero mismatches and an
  identical total. Phase 2 asserts equality, not a tolerance.
- **The 7-day window is a replay budget.** Within it, a parsing or filter bug can be
  fixed and affected hours reprocessed without re-downloading. Past it, recovery means
  re-fetching from Wikimedia, which still works - the dumps are archival - but slowly.
- **Retention is a separate DAG**, never a cleanup task inside the ingest DAG. Coupling
  deletion to ingestion misbehaves during backfill, when runs execute out of order.
- The scan cost is unchanged by how little is kept: all 7.45M rows are still parsed
  every hour. Filtering saves storage, not compute.
