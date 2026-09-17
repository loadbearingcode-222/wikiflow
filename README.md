# wikiflow

An hourly incremental data pipeline over Wikimedia pageviews, built to answer one question:

> How does public attention to the largest tech companies move hour by hour, across every
> language edition of Wikipedia — and what share of all Wikimedia traffic is it?

Airflow 3 · DuckDB · Postgres · dbt · MinIO (S3) · Streamlit · Docker Compose

**Status:** Phases 0–5 complete. Every hour loaded reconciles exactly against Wikimedia's
own published totals, and the same marts build identically in two warehouses.

---

## The evidence

Every figure here was measured on the running pipeline, not asserted. Where a number
comes from, the linked document shows how.

| | |
|---|---|
| **Correctness** | Our per-domain totals, computed independently from ~7M rows an hour, match Wikimedia's published `projectviews` **exactly** — 105,160 domain-hours, 988,785,027 views, zero mismatches |
| **Portability** | The same dbt models build in DuckDB and Postgres, and every mart is **identical row for row** — 26,247 rows compared exactly, floats included ([ADR 0008](docs/adr/0008-a-second-warehouse.md)) |
| **Completeness** | 2026-09-14 and 09-15: 48 of 48 hours. 2026-09-16: 6 of 24 so far, because the stack was offline, and the gap check reports it rather than hiding it |
| **Coverage** | Views arriving through redirects are counted: +2.7% overall, from +0.9% (Facebook) to +7.2% (Alphabet) ([ADR 0007](docs/adr/0007-redirects-and-the-published-filter.md)) |
| **Integrity** | 0 unparseable lines across ~270 million; a truncated file is refused before anything is written |
| **Idempotency** | Re-running an hour leaves the same rows and the same totals; a lost partition was restored identically in **21 s** ([drills](docs/FAILURE_DRILLS.md)). All 54 hours were rebuilt from raw, without re-downloading, in 3 min 35 s |
| **Throughput** | DuckDB extracts both grains from 7,635,493 rows in **3.66 s using 201 MB**; the pandas equivalent takes 8.57 s and **1,922 MB** — identical output, median of 3 ([benchmark](scripts/benchmark_extraction.py)) |
| **Operation** | 17–21 s to ingest an hour; an Airflow pool keeps the pipeline under the source's 3-connection limit |

---

## Why this project exists

Most data engineering portfolio projects ingest a static CSV on a schedule. That skips the
part that is actually hard in production: **knowing which data is new**, and being able to
re-run without corrupting what is already there.

Wikimedia publishes one immutable gzipped file per hour — about 7 million rows, 45–70 MB.
Because the source is partitioned by time, the pipeline needs **no stored watermark, no
`updated_at` column and no change-data-capture**: the Airflow data interval *is* the
watermark. That makes the important properties provable rather than hoped for:

- **Re-running any hour is safe.** Each task replaces exactly one partition wholesale. There
  is no append path, so there is no way to double-count.
- **Backfill is the same code as live ingestion.** There is no separate backfill logic.
- **A gap is detectable.** A missing partition is a missing directory, not a silent absence.

---

## When things broke

A pipeline that has only ever succeeded hasn't been tested. [`docs/FAILURE_DRILLS.md`](docs/FAILURE_DRILLS.md)
covers one real incident and three deliberate drills, with measured results:

- **Incident — rate-limited by the source.** Up to five concurrent downloads against a
  3-connections-per-IP limit produced `429 Too Many Requests`. Misdiagnosed twice before
  the audit trail — which records failed attempts, not just final states — made the cause
  obvious. Fixed with an Airflow pool and `Retry-After`-honouring retries.
- **A truncated download.** Read with `ignore_errors`, a file cut off at 30% returned
  2.1 million rows and **no error** — 72% of the hour silently gone. Now refused.
- **A lost partition.** Detected by the gap check, restored identically. The drill also
  proved the documented recovery command wrong; the runbook is fixed.
- **Retention misconfigured to expire everything.** Refused; 98 of 98 raw partitions kept.

---

## Dashboard

A Streamlit dashboard at <http://localhost:8501> shows attention over time, share of all
Wikimedia traffic, the language breakdown per company, the mobile/desktop split, and
pipeline health read from the audit trail.

It is built to avoid specific misreadings. Company colours follow the entity, so filtering
never repaints the others, and the palette is validated for colour-blind separation by
script. Views and share of traffic are a toggle, never two y-axes on one chart. Lines break
at missing hours instead of drawing across them. Every chart has a table view. And the
reconciliation tile reports **dbt's own recorded verdict** — or "not run" if there isn't
one — rather than asserting a result.

One thing the data shows that deserves care: German Wikipedia's article on the company is
titled plain "Apple", unlike English Wikipedia's "Apple Inc.", so it also attracts readers
looking for something else. On 2026-09-14 at 19:00 UTC that article took 13,965 views
(desktop and mobile together), against a median of 47 an hour across the 49 hours
loaded. The dashboard labels the spike; it doesn't guess at a cause.

---

## Design decisions

The reasoning behind each is in [`docs/adr/`](docs/adr/).

| Decision | Why | What was rejected |
|---|---|---|
| Partition-as-watermark | The source is time-partitioned and immutable, so state can live in the data's own location | A stored high-watermark, with its crash window between "data committed" and "watermark advanced" |
| DuckDB as the warehouse | Handles ~7M rows an hour in seconds and ~200 MB, with no server; the warehouse is one file | Postgres — a server to operate for a single-user analytical warehouse |
| MinIO for object storage | The pipeline addresses `s3://`; moving to AWS is one endpoint URL | The local filesystem |
| Two grains per hour | Filtering to the companies alone leaves no denominator — a count, but no way to tell if it was unusual | Ingesting only the filtered rows |
| Entity resolution via Wikidata | `Apple Inc.` is `Apple_Inc.` in English, `Apple` in French, `アップル` in Japanese | A hardcoded list of titles |
| Redirects in the filter, labelled | A reader who asks for `Apple_Computer` reads *Apple Inc.*, but the dump counts the view under the title requested | Leaving the undercount, or merging redirects so the effect cannot be measured |
| The filter is published daily as a snapshot | Resolving redirects takes 315 requests; ingestion runs 24 times a day | Resolving inside every ingest run |
| A shrinking filter is refused | A MediaWiki outage loses the redirects and keeps the articles, which looks healthy by article count | Publishing whatever the lookup returned |
| Load Postgres by ETag manifest | Reprocessing *rewrites* old hours, so "newer than the last hour loaded" misses them | A high-watermark on time |
| Prove parity row by row | "It builds on both" shows the SQL compiles, not that the data matches | Comparing row counts |
| No Airflow imports in `src/wikiflow` | Logic is testable without a scheduler; the unit suite runs in about 2 seconds | Logic inside DAG files |
| Domain-code parsing in Python, not dbt | `.m` means three different things by position; two implementations would drift | Re-deriving it in SQL |
| An Airflow pool for the source | `max_active_runs` is per DAG and counts run types separately; it did not prevent the incident | Relying on `max_active_runs` |
| Refuse hours with unparseable lines | `ignore_errors` silently turned a truncated file into a plausible short hour | Counting malformed lines without acting on the count |
| Asset-triggered transforms | Source publication lag varies by ~90 minutes, so any cron offset is wrong one way or the other | A scheduled transform DAG |
| Audit is append-only, data is overwrite | A partition that failed twice before succeeding keeps all three records | Recording only the latest attempt |
| Anomaly check compares the same hour across days | Traffic swings ~1.6× daily, so hour-over-hour checks fire every dawn and dusk | Comparing with the previous hour |
| Retention is its own DAG | Backfill runs out of order, so a cleanup step inside ingestion could delete data still needed | A cleanup task in the ingest DAG |

---

## Architecture

```
dumps.wikimedia.org  (hourly .gz, immutable; limit: 3 connections per IP)
      │
      ▼   sensor + download, both in the `wikimedia_dumps` pool (2 slots)
   download → verify (length, gzip CRC, lines) → land raw bytes
      │
      ▼   RAW   s3://wikiflow-raw/…/dt=YYYY-MM-DD/hour=HH/        7-day retention
      │
      ▼   one DuckDB scan, two outputs; refused if lines fail to parse
   BRONZE  company_pageviews   ~400 rows/hr    the fact grain            kept
           domain_totals     ~1,950 rows/hr    denominator + reconciliation key
      │
      ▼   both triggered by the same Airflow Asset, independent of each other
      ├──────────────────────────────────────┐
      ▼   dbt --target prod                  ▼   ETag-manifest load, then dbt --target pg
   MARTS in DuckDB                         MARTS in Postgres        ─┐
   dim_company, dim_wiki,                  (same models, same tests) │ wikiflow.parity:
   fct_company_pageviews_hourly,                                     │ identical row
   agg_company_share_of_traffic,                                     │ for row
   agg_company_daily                                                ─┘
      │
      ▼
   Streamlit dashboard  (reads marts, dbt run results, and the audit trail)

   independent DAGs:  company_pages_refresh  (daily: articles + redirects -> filter snapshot)
                      pageviews_quality      (hourly: gaps, freshness, volume)
                      raw_retention          (daily: dry-run manifest, then delete)
```

### Storage lifecycle

| Layer | Per hour | Retention | Steady state |
|---|---|---|---|
| Raw `.gz` | 45–70 MB | 7 days | ~10 GB |
| Bronze Parquet | tens of KB | indefinite | well under 1 GB/year |
| Marts | small | indefinite | < 1 GB/year |

The 7-day raw window is the **replay budget**: a parsing bug can be fixed and every
affected hour reprocessed from object storage — measured at about 4 seconds an hour —
without re-downloading.

---

## At 100× the volume

What would change, in the order it would break:

1. **The source's connection limit, first.** Compute isn't the constraint; downloading is,
   and Wikimedia allows 3 connections. A year of history is 8,760 hours at ~20 s each on
   2 slots — about a day today, and download-bound at any larger file size. More workers
   would not help; a mirror or a bulk export would.
2. **gzip decompression.** A `.gz` stream can't be split, so each file decompresses on one
   core however many DuckDB has. Files 100× larger would take minutes each — still inside
   the hour, but with shrinking headroom.
3. **The single-writer warehouse.** DuckDB allows one writer. One transform DAG is fine;
   many concurrent writers are not. The marts would move to a server warehouse. That move
   has been rehearsed: the models built on Postgres with two dispatched macros. The real
   work was the load step DuckDB never needed ([ADR 0008](docs/adr/0008-a-second-warehouse.md)).
4. **The executor.** LocalExecutor runs everything on one node. Hundreds of DAGs would need
   Celery or Kubernetes executors.

What would *not* need to change: the partition-as-watermark design, idempotent overwrites,
the reconciliation test, and the retention policy all scale unchanged. Moving MinIO to S3
is a configuration change.

---

## Running it

**Prerequisites:** Docker Desktop with at least 8 GB allocated, and
[uv](https://docs.astral.sh/uv/). Airflow is never installed locally; it runs in containers.

```bash
uv sync --all-groups     # local dev environment (Python 3.12)
make up                  # build (if anything changed) and start the stack
```

On Windows, use `.\make.ps1 <target>`; both runners expose the same targets.

| Service | Address |
|---|---|
| Airflow UI | <http://localhost:8080> (`airflow` / `airflow`) |
| Dashboard | <http://localhost:8501> |
| MinIO console | <http://localhost:9001> |
| Postgres warehouse | `localhost:5433` (`wikiflow` / `wikiflow_dev_secret`, database `wikiflow`) |

All ports bind to `127.0.0.1` only; the development credentials are not exposed to the
local network.

### Tests

```bash
make test             # unit tests, local, no containers
make test-container   # DAG integrity, integration, and "is dbt really installed" checks
make check            # lint + types + unit tests — what CI runs first
```

`src/wikiflow` has no Airflow imports, so most tests run locally in about 2 seconds,
including on Windows. Tests that need Airflow run in the container, as they do in CI.

### Operations

```bash
make parity                                          # DuckDB vs Postgres, every mart, row by row
make pg-sync                                         # load new or rewritten bronze into Postgres
make reprocess FROM=2026-09-14T00 TO=2026-09-14T23   # rebuild bronze from raw, no download
```

When something goes wrong, [`docs/RUNBOOK.md`](docs/RUNBOOK.md) maps each alert to the
command that fixes it.

---

## Roadmap

- [x] **Phase 0** — Foundation: containerised stack, quality gates, CI
- [x] **Phase 1** — Ingestion: verified download, two-grain extraction, idempotent partitions
- [x] **Phase 2** — Modelling: dbt staging and marts, exact reconciliation test
- [x] **Phase 3** — Operations: audit trail, quality checks, retention, runbook
- [x] **Phase 4** — Presentation: dashboard, benchmark, incident write-up and failure drills
- [x] **Phase 5** — Depth: redirect resolution, and Postgres as a second warehouse with proven parity

---

## What this is not

- **Single-node.** No horizontal scaling is claimed.
- **Not production-alerted.** Alerting is a best-effort webhook, with no paging or SLAs.
- **Redirects are counted from the latest filter.** A redirect created today counts from
  the next daily refresh onward. Hours already ingested are not re-extracted unless
  reprocessed, and that is only possible within the 7-day raw window.
- **Two days of data.** The seasonality-aware volume check needs at least three days of
  history, and says so rather than guessing.

---

## Data source and attribution

Pageviews data is published by the Wikimedia Foundation under
[CC0](https://creativecommons.org/publicdomain/zero/1.0/) at
[dumps.wikimedia.org](https://dumps.wikimedia.org/other/pageviews/). Requests identify
themselves with a descriptive `User-Agent`, as Wikimedia asks of automated clients, and
stay within its published connection limit.
