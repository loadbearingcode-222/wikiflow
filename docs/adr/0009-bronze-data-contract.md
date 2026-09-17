# ADR 0009 — A data contract for bronze

- **Status:** Accepted
- **Date:** 2026-09-17
- **Decision:** Declare the shape of both bronze grains once, as strict Pydantic models,
  and validate every row of a partition against them before it is published. Derive the
  Postgres loader's tables from the same models.

## Context

Bronze is where this pipeline's output becomes everyone else's input: two warehouses, the
dashboard, and anyone reading the Parquet directly. Its shape used to be implied by a
`SELECT` in `extract.py` and restated by hand in the Postgres loader. Twice in Phase 5
that implied shape was wrong, and nothing noticed:

- **Two partitions had a DOUBLE `views` column** (`3882.0`). They predate the fix that cast
  sums to BIGINT. DuckDB unified the types when reading all partitions together, so
  reconciliation passed. It surfaced only when Postgres rejected `3882.0` as a bigint
  (ADR 0008).
- **Adding `match_type` and `canonical_title`** (ADR 0007) meant editing extraction, the
  loader's column list and the dbt source separately, with no check that they agreed.

Neither was a logic error. Both were a declared shape that existed nowhere.

## Decision

`wikiflow.contracts` defines `CompanyPageviewRecord` and `DomainTotalRecord`: column
names, order, types, and the rules a row must satisfy.

- **Strict types.** `strict=True`, so `3882.0` in an integer column fails rather than being
  coerced to `3882`. Lax validation would have passed exactly the defect that motivated
  this.
- **Rules that matter downstream, not decoration:**
  - view counts are at least 1, since a dump row only exists because something was viewed;
  - `access_method` and `match_type` are closed sets;
  - a QID looks like a QID;
  - a row's `interval_start_utc` lies inside its own `dt`/`hour` partition, because a row
    filed under the wrong hour would reconcile nowhere;
  - a domain code marked parseable must actually carry its parsed attributes.
- **Columns before rows.** A missing, extra or reordered column is reported as a contract
  change in one message, not as thousands of row errors.
- **Enforced before publication.** `extract_hour` validates both files after writing them
  locally and before returning. Ingestion, and reprocessing from raw, only publish what
  passed.
- **One declaration, two uses.** The Postgres loader builds its bronze tables from the
  contract (`postgres_columns`). The derived tables were checked against the hand-written
  ones they replaced, and matched exactly.

## Why per row, and why only here

A partition is small: a few hundred company rows and about 1,950 domain rows an hour.
Validating one took **24–62 ms** on real partitions. The ~7 million raw rows an hour are
never validated this way; they are checked in aggregate, by the malformed-line gate and by
exact reconciliation with Wikimedia's own totals. The contract guards the boundary, not
the scan.

Every real bronze partition in object storage was validated against the contract when it
was introduced, and all passed. They had already been rewritten by the reprocess in
ADR 0007, including the two DOUBLE partitions.

## Consequences

- A change to bronze's shape is now a change to one file, and extraction refuses to
  publish anything that does not match it. That is deliberate friction.
- The dbt source still describes bronze separately. dbt tests on staging overlap with some
  contract rules; that overlap is accepted, because the contract stops bad data *before*
  publication and dbt tests only report it *after*.
- `raw.projectviews` has no contract. It is Wikimedia's file, not ours, and is used only as
  the reconciliation oracle.
