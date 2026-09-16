# wikiflow

An hourly incremental data pipeline over Wikimedia pageviews, built to answer one question:

> How does public attention to the five largest tech companies move hour by hour, across
> every language edition of Wikipedia — and what share of all Wikipedia traffic is it?

**Status:** Phases 0-1 complete - the pipeline ingests a real hour end to end, idempotently. See [Roadmap](#roadmap).

---

## Why this project exists

Most data engineering portfolio projects ingest a static CSV on a schedule. That skips the
part that is actually hard in production: **knowing which data is new**, and being able to
re-run without corrupting what you already have.

This pipeline is built around that problem specifically.

Wikimedia publishes one immutable gzipped file per hour, roughly 8 million rows and 50–70 MB
each. Because the data is partitioned by time at the source, the pipeline needs **no stored
watermark, no `updated_at` column, and no change-data-capture** — the Airflow data interval
*is* the watermark. That is the cleanest version of a pattern most production pipelines
should use, and it makes correctness properties provable rather than hoped for:

- **Re-running any hour is a no-op.** Each task writes exactly one partition and overwrites it
  wholesale. There is no append path, so there is no way to double-count.
- **Backfilling is the same code as live ingestion.** No separate backfill script exists,
  because none is needed.
- **A gap is detectable.** A missing partition is a missing directory, not a silent absence.

---

## Design decisions

The reasoning behind each is in [`docs/adr/`](docs/adr/). The short version:

| Decision | Why | What was rejected |
|---|---|---|
| Partition-as-watermark | The source is time-partitioned and immutable, so state can live in the data's own location instead of a cursor table | A stored high-watermark, which would add a crash window between "data committed" and "watermark advanced" |
| DuckDB as the warehouse | Parses ~8M rows/hour in seconds, zero operational overhead, and the whole warehouse is a single file a reviewer can open | Postgres — genuinely slow at this row count; planned later as a second implementation behind the same interface |
| MinIO for object storage | The pipeline addresses storage as `s3://` and would move to real AWS by changing one endpoint URL, at zero cost | The local filesystem, which would not exercise any of the object-storage semantics that matter |
| LocalExecutor, not Celery | Drops the Redis and worker containers; an hourly pipeline does not need distributed task execution | CeleryExecutor, which doubles the memory footprint to demonstrate parallelism this workload never uses |
| Two grains per hour | Filtering to five companies alone would leave no denominator — you could report a view count but not whether it was unusual | Ingesting only the filtered rows |
| Entity resolution via Wikidata | `Apple Inc.` is `Apple_Inc.` in English, `Apple` in French, `アップル` in Japanese. A hardcoded title list is wrong on arrival | Hardcoding English titles |
| No Airflow imports in `src/` | Keeps the logic testable without a scheduler and the unit suite runnable on Windows in milliseconds | Putting logic directly in DAG files, the usual shape, which makes it untestable in isolation |

---

## Architecture

```
dumps.wikimedia.org  (hourly .gz, immutable)
      │
      ▼   availability sensor, reschedule mode (10-min poke, 5h timeout)
   download → verify → upload raw bytes
      │
      ▼   RAW  (immutable, 7-day retention)
   s3://wikiflow-raw/pageviews/dt=YYYY-MM-DD/hour=HH/
      │
      ▼   DuckDB reads the .gz from MinIO — ONE scan, TWO outputs
   BRONZE  (kept indefinitely)
     ├── company_pageviews     379 rows/hr    the detailed fact grain
     └── domain_totals       1,955 rows/hr    the denominator + reconciliation key
      │
      ▼   dbt-duckdb
   SILVER → GOLD   dim_company, dim_project, fct_company_pageviews_hourly,
                   agg_company_share_of_traffic
      │
      ▼
   Streamlit dashboard + freshness panel
```

### Verified behaviour

Measured against the live source, not asserted:

| | |
|---|---|
| Throughput | 7,451,970 rows scanned and both grains written in **3.5s** (2.1M rows/s) |
| Reconciliation | wide grain matches `projectviews` on **all 1,955 domain codes**, zero mismatches, identical total (19,322,047) |
| Cross-check | dump `en` equals the AQS API value for the same hour, to the view |
| Idempotency | two full runs of one hour → 1 object per partition, 379 company rows, 5,138 views, both times |
| Integrity | 7,451,970 lines parsed, **0 malformed** |
| Compression | 62 MB gzip → 236 MB (3.6x); bronze output 23 KB total |

### Storage lifecycle

Nothing is deleted implicitly; retention is an explicit policy enforced by its own DAG.

| Layer | Per hour | Retention | Steady state |
|---|---|---|---|
| Raw `.gz` | 45–70 MB | 7 days | ~10 GB |
| Bronze Parquet | ~100–300 KB | indefinite | ~2 GB/year |
| Gold marts | small | indefinite | < 1 GB/year |

The 7-day raw window is the **replay budget**: a bug in parsing or filtering can be fixed and
all affected hours reprocessed without re-downloading anything.

---

## Running it

**Prerequisites:** Docker Desktop with at least 8 GB allocated, and [uv](https://docs.astral.sh/uv/).
Airflow itself is never installed locally — it only runs in containers.

```bash
uv sync --all-groups     # local dev environment (Python 3.12)
make up                  # build and start the stack
```

On Windows, use `.\make.ps1 <target>` instead of `make <target>` — GNU Make is not installed
by default there. Both runners expose identical targets; run either with no arguments to list
them.

| | |
|---|---|
| Airflow UI | http://localhost:8080 (`airflow` / `airflow`) |
| MinIO console | http://localhost:9001 |

### Tests

```bash
make test             # unit tests, local, no containers (fast)
make test-container   # DAG integrity + integration tests, in the container
make check            # lint + types + unit tests, i.e. what CI runs
```

The split is deliberate. `src/wikiflow/` has no Airflow imports, so its tests run locally in
milliseconds — including on Windows, where Airflow does not install cleanly. Tests that
genuinely need a scheduler run in the container instead, which is also what CI does.

---

## Roadmap

- [x] **Phase 0** — Foundation: containerized stack, quality gates, CI
- [x] **Phase 1** — Ingestion: sensor, verified download, two-grain extraction, idempotent partitions
- [ ] **Phase 2** — Modeling: dbt staging and marts, star schema, reconciliation test
- [ ] **Phase 3** — Operations: audit table, quality gates, retention DAG, runbook
- [ ] **Phase 4** — Presentation: dashboard, benchmarks, failure drills
- [ ] **Phase 5** — Depth: Postgres warehouse behind the same interface, redirect resolution

---

## What this is not

Worth stating plainly, since the architecture above could imply more than is here:

- It is single-node. There is no horizontal scaling story and none is claimed.
- There are no SLAs, no on-call rotation, and alerting is a webhook rather than a real
  paging integration.
- Retention is time-based only; there is no tiered archival to cold storage.
- The company view counts **undercount** by design until redirect resolution lands in Phase 5
  — pageviews are recorded against the title requested, so redirects to a canonical article
  are counted separately and are not yet folded in.

---

## Data source and attribution

Pageviews data is published by the Wikimedia Foundation under
[CC0](https://creativecommons.org/publicdomain/zero/1.0/) at
[dumps.wikimedia.org](https://dumps.wikimedia.org/other/pageviews/). Requests identify
themselves with a descriptive `User-Agent` as Wikimedia asks of automated clients.

---

## Development workflow

Single-developer project, but `main` is never committed to directly: work goes through
a pull request with green CI, so `main` stays a sequence of completed, verified changes
and each PR carries the evidence for its change. See
[docs/WORKFLOW.md](docs/WORKFLOW.md), and run `make hooks` once after cloning.
