# ADR 0008 — The same marts in a server warehouse

- **Status:** Accepted
- **Date:** 2026-09-17
- **Decision:** Build the unchanged dbt models in Postgres as a second target, in a DAG
  independent of the DuckDB one. Load bronze in through an ETag manifest, and prove the
  two warehouses hold identical data by comparing every mart row by row.

## Context

DuckDB is the right warehouse for this project (ADR 0005), and it stays the primary one:
the dashboard reads it. It is also single-writer and embedded, which is where it runs out
at scale (README, *At 100× the volume*). The claim that "the model layer would move with
an adapter change" was untested. This ADR tests it.

Postgres stands in for any server warehouse. The question is not "is Postgres better"
but "what does moving the model layer actually take, and how do you know it worked?"

## What it took

### 1. A load step DuckDB never needed

DuckDB reads Parquet straight out of object storage, so the DuckDB warehouse has no load
step at all. Postgres cannot, so bronze has to be copied in first. This is the largest
real difference between the two targets, and none of it is SQL dialect.

Deciding *what to load* is a different incremental problem from ingestion's. Ingestion is
driven by time, so the data interval is the watermark (ADR 0002). The loader's source is
object storage that the pipeline keeps **rewriting**: a re-run, a reprocess from raw
(ADR 0007) or a recovered partition all replace an hour that already exists. A
high-watermark ("load everything after the newest hour loaded") would never look at those
hours again.

So `wikiflow.postgres_load` keeps a manifest, `ops.load_manifest`, recording the S3
**ETag** of every partition it loaded. A partition is loaded when its ETag differs or is
absent. That rule was proven on real data: bronze was partly loaded, then all 54 hours
were reprocessed. The next sync reloaded all 162 partitions (3 datasets × 54 hours),
because every ETag had changed, and the sync after that loaded **0 of 162**.

Each partition loads in **one transaction**: delete the hour, `COPY` the rows, upsert the
manifest row. A failure rolls back all three, so the manifest can never claim a partition
that is only half there. It is the lake's delete-then-write rule, with the database's
atomicity doing the work that write ordering does in object storage.

Deletions are **not** mirrored. Raw projectviews expire after 7 days by design, and a
lost bronze partition is an incident for the gap check. Propagating it would turn one
incident into two.

| | Measured |
|---|---|
| First full load | 162 partitions, 234,167 rows, **4.7 s** |
| Re-sync with nothing changed | 0 loaded, 162 unchanged |
| The same full load from the Windows host, through Docker's port forwarding | 2 min 51 s |

The last row is why the load runs inside the stack: the same code took 171 s instead
of 4.7 s, only because of where it ran relative to storage.

### 2. Two dialect differences, both dispatched

The models needed only two changes, each an `adapter.dispatch` macro in
`macros/cross_warehouse.sql`:

- **`::double`** is DuckDB syntax. `cast(... as double precision)` is standard and an
  8-byte float on both. `float` would **not** do: DuckDB reads it as 4-byte.
- **`url_decode`** is built into DuckDB and absent from Postgres, so a PL/pgSQL function
  is created by an `on-run-start` hook.

Writing the Postgres function exposed a bug in the **DuckDB** side. DuckDB's `url_decode`
*raises* when the decoded bytes are not valid UTF-8, so a single title like `%E3` would
have failed the whole build. Both versions now return such a title unchanged. A dbt test,
`assert_url_decode_behaves_identically`, pins 14 edge cases to the same expected output
on every target. They include `+` (not a space), `%zz`, a truncated `%4`, `%%41` and a
4-byte emoji.

Everything else was portable as written: `bool_and`, `DATE` literals, `is distinct from`,
full outer joins, the `delete+insert` incremental strategy, and every `dbt_utils` test.
**All 34 models and tests passed on Postgres on the first build**, including the exact
reconciliation against Wikimedia's projectviews, which Postgres checks against its own
loaded copy.

### 3. Independence between the two DAGs

`pageviews_transform_postgres` is triggered by the same bronze Asset as the DuckDB
transform, and depends on nothing else. If Postgres is down, the DuckDB marts and the
dashboard carry on, and only this DAG fails.

The two DAGs do share one dbt project directory, which creates two races:

- **Results.** dbt writes `run_results.json` and its partial-parse cache per target path.
  The Postgres DAG uses `--target-path target/postgres`, so one warehouse's test results
  can never overwrite the other's. That already happened once with `docs generate`
  (Phase 4).
- **Packages.** `dbt deps` rewrites `dbt_packages/` in place while another build may be
  parsing macros from it. A `dbt_project` pool with **one slot** covers every dbt task in
  both DAGs. The Postgres DAG does not run `deps` at all.

## Proving it worked: `python -m wikiflow.parity`

"The models build on both" shows that the SQL compiles. It does not show the data
matches. The parity check uses DuckDB as the comparison engine. It attaches its own
warehouse read-only, attaches Postgres read-only, and runs `EXCEPT ALL` in both
directions for every mart, selecting columns by name. It compares every column exactly,
floats included: both warehouses compute shares as IEEE-754 doubles from identical
integers, so any tolerance would only hide a real divergence.

```
mart                                 duckdb   postgres  only duck  only pg  result
dim_company                               7          7          0        0  identical
dim_wiki                              1,994      1,994          0        0  identical
fct_company_pageviews_hourly         23,847     23,847          0        0  identical
agg_company_share_of_traffic            378        378          0        0  identical
agg_company_daily                        21         21          0        0  identical

all marts identical: 5 marts, 26,247 rows compared
```

A check that has only ever said "identical" has not been tested. So one view count in
Postgres was incremented by 1, and parity reported `fct_company_pageviews_hourly` as
different: one row only in each warehouse.

Undoing that change by hand then went wrong, which is worth recording. The revert
targeted "the first row" by physical row id (`ctid`), but an UPDATE writes a new row
version with a new `ctid`. So the revert altered a *different* row, leaving two rows
wrong instead of none. Parity caught that too. The fix was not another hand edit but a
`--full-refresh` from bronze, after which parity reported identical again. Derived tables
should be rebuilt, not patched.

## Found along the way

- **Two bronze partitions had the wrong column type.** The first two `domain_totals`
  partitions ever written (2026-09-14 10:00 and 11:00) predate the fix that cast sums to
  BIGINT, so their `views` column was DOUBLE. Read together with the other 52 files,
  DuckDB silently unified the types, and reconciliation passed because the values were
  whole numbers. Copied file by file into Postgres, `3882.0` was rejected as a bigint.
  The loader now casts every column to its declared type, and reprocessing rewrote both
  partitions.
- **An invalid Asset URI broke four DAGs, not just the new one.** The Postgres provider
  validates `postgres://` URIs as `host:port/database/schema/table`. The first URI
  omitted the schema and table. `assets.py` is shared, so the four DAGs that import it,
  ingestion among them, failed to import in the running scheduler for about three
  minutes. The DAG integrity
  test caught it at once, but only after the stack had already been restarted on the new
  code. The order is now: container tests, then restart.

## Consequences

- One more service (`warehouse-postgres`, host port 5433) and one more DAG to operate.
  It is deliberately **not** the Postgres instance that holds Airflow's metadata. A
  warehouse sharing it would let an analytical query slow the scheduler, or a scheduler
  upgrade take the warehouse down with it.
- Postgres holds a copy of bronze. That duplication is the price of a warehouse that
  cannot read the lake directly, and exactly what a lakehouse-capable engine avoids.
- A new model must build on both targets. Dialect-specific SQL now belongs in a
  dispatched macro, and parity will report any model where the two disagree.
- The dashboard still reads DuckDB, and its queries in `wikiflow.metrics` use DuckDB
  syntax (`unnest(?::VARCHAR[])`, `::DOUBLE`). Pointing it at Postgres would mean
  porting those queries too, not just changing the connection.

## Addendum — closing the gaps a review found

A review of this ADR against the code found three places where the design claimed more
than the automation delivered. All three are fixed.

### Rewritten hours now reach the marts without a full refresh

The loader noticed a rewritten hour, but the fact table did not. It watermarked on
`view_date`, so a reprocessed hour on an earlier date landed in bronze and in Postgres'
copy of bronze, and changed no mart in either warehouse. Parity passed that day only
because both warehouses had been fully refreshed by hand.

`fct_company_pageviews_hourly` now carries each hour's `ingested_at` and selects rows
written after the newest one it holds. It uses the same rule as the loader: *changed*,
not *newer in time*. The watermark overlaps by one hour, because two ingest runs overlap.
An hour extracted slightly earlier can finish uploading after a build has already moved
past it, and re-selecting a recent hour is free under `delete+insert`.

Proven on real data: 2026-09-14 05:00 was rebuilt from raw. Ordinary incremental builds
on both warehouses then replaced exactly that hour (new `ingested_at`, still 417 rows),
left 04:00 untouched, and the loader moved 2 of 162 partitions. Parity stayed identical.

### CI now covers the Postgres path

Until now, CI never ran the loader against a database, never built `--target pg`, and
never ran parity. `tests/e2e/test_two_warehouses.py` does all of that in its own buckets
and database, refusing to run against the real ones, and CI starts MinIO and Postgres for
it. It covers:

- the build and every test on both targets;
- row-for-row parity;
- a rewritten hour on an *earlier date* flowing through incremental builds;
- the loader moving only the 2 changed partitions;
- a load failing half-way through a partition.

The test itself needed a fix, found by running it against the rule it replaces. With both
fixture hours on the same date, **it passed under the old `view_date` watermark too**,
because that rule rebuilds every hour of the newest date anyway. The rewritten hour now
sits on an earlier date. Against the old rule the test fails; against the new rule it
passes.

### Parity runs daily, and "behind" is not "wrong"

`warehouse_parity` runs the comparison daily. First it checks whether the two fact tables
were built from the same writes of bronze: the same hours, at the same `ingested_at`.
If not, one warehouse simply has not caught up. That outcome is *not comparable yet*,
and the task retries in 20 minutes. If both were built from the same bronze and still
differ, that is a defect: the task fails without retrying. It holds the `dbt_project`
pool's slot while reading, so no build in either warehouse runs underneath it.

`pageviews_quality` gained a fourth check: how far the Postgres marts trail the newest
bronze hour, failing past two hours. A transform DAG that fails alerts on its own. One
that is paused, or never triggered, fails nothing, and only this check would notice.

### A failed load, exercised

The claim above that "a failure rolls back all three" is now tested, not asserted. The
loader's connection is in autocommit mode, so if the explicit transaction did not really
wrap the DELETE, a failure would lose a whole hour. On the live warehouse, reloading
2026-09-15 09:00 was made to die after copying 999 of 1,949 rows, after the DELETE had
run. Afterwards the partition held its 1,949 rows and 16,180,602 views, and the manifest
kept its ETag and load time. The next sync found nothing to repair. The same scenario runs
in CI. Full write-up: `docs/FAILURE_DRILLS.md`, drill 5.
