# ADR 0007 — Counting views that arrive through redirects

- **Status:** Accepted
- **Date:** 2026-09-17
- **Decision:** Expand every tracked article into its redirect titles, resolve that
  filter once a day into a versioned snapshot in object storage, and have ingestion read
  the snapshot instead of resolving the filter itself.

## Context

Pageviews are recorded against the title a reader **requested**. Someone who follows a
link to `Apple_Computer` reads the *Apple Inc.* article, but the dump counts that view
under `Apple_Computer`. The filter was built from Wikidata sitelinks, which name only the
article itself, so every one of those views was missed. The README listed this as a
known undercount.

How big was it? The redirect counts looked large: English Wikipedia alone has 103
redirects to *Apple Inc.*, 74 to *Microsoft* and 138 to *Amazon*. Across every wiki the
seven tracked entities have **3,252 redirect titles**, turning a filter of 2,240 rows
into one of 8,742.

## What it measured

All 54 hours in the raw layer were rebuilt with the expanded filter (see *Backfill*
below) and compared:

| Entity | Views | Via a redirect | Share |
|---|---:|---:|---:|
| Alphabet Inc. | 11,542 | 828 | 7.2% |
| Apple Inc. | 57,840 | 2,925 | 5.1% |
| Meta Platforms | 13,925 | 657 | 4.7% |
| Amazon | 30,378 | 1,370 | 4.5% |
| Microsoft | 28,983 | 574 | 2.0% |
| Google | 79,979 | 870 | 1.1% |
| Facebook | 69,076 | 645 | 0.9% |
| **All** | **291,723** | **7,869** | **2.7%** |

The undercount was real but **smaller than the redirect counts suggested**: 1,277 of the
3,252 redirect titles received any traffic at all. It is also uneven. It matters most for
entities whose article title is not what readers type: *Apple Inc.* loses 366 views to
`Apple_Inc` (no final period) and 227 to `Apple_Computer`. It matters least for names
readers already type exactly, like *Google* and *Facebook*.

At 0.9% to 7.2%, the share shifts enough to move an entity's ranking in a close hour.
It is too uneven to fix with a flat correction factor.

## Decisions

### Redirect rows are labelled, not merged

Each redirect row carries `match_type = 'redirect'` and the `canonical_title` it points
at, through bronze and into `fct_company_pageviews_hourly`. Totals include them. The
split stays queryable, so the effect above can be re-measured at any time, and the
dashboard shows each company's redirect share next to its mobile share.

A redirect inherits the domain code and access method of its article: one redirect
yields a desktop and a mobile row, exactly as the article does. When a redirect title
collides with an article title on the same domain, the article row wins. A duplicate
`(domain_code, page_title)` key would double-count that row in the extraction join.

### Resolved daily, not per ingest run

Ingestion used to call Wikidata in every run. Redirect resolution adds one MediaWiki
request per wiki: 315 requests, taking 2m40s to 2m54s in Airflow. The ingest DAG runs 24
times a day, and during a backfill twice at once, while the inputs change over weeks. So
`company_pages_refresh` resolves the filter daily and publishes it:

```
reference/company_pages/snapshots/20260917T040517Z.parquet   immutable, one per refresh
reference/company_pages/current.parquet                      what ingestion reads
```

The snapshot is written first and `current` replaced second, so a failure between them
leaves the previous filter in place. On a fresh stack with nothing published, the first
ingest run resolves and publishes the filter itself. That path was exercised live: it
took 2m42s.

Requests to MediaWiki are **serial** and send `maxlag=5`, as Wikimedia's API etiquette
asks. A `maxlag` refusal or a 429 is waited out exactly as long as `Retry-After` says.

### Every hour records which filter it was extracted with

Keeping every snapshot is only useful if you can tell which one an hour used. Each snapshot
file names itself in a `snapshot_key` column, including the `current.parquet` copy. The
ingest task and the reprocess both write that key into the hour's audit record
(`filter_snapshot`). So after the filter changes, the exact filter behind any hour can be
found. This holds for hours ingested from 2026-09-17 on; earlier audit records predate
the field and show none.

### A shrinking filter is refused

`publish` refuses a snapshot that has lost more than 10% of the current one's article
rows **or** of its rows overall. The second condition is the one that matters for this
change: a MediaWiki outage leaves the Wikidata articles intact and loses only the
redirects. Measured by article rows alone, that would look healthy, while ingestion
silently lost 2.7% of views.

This follows the same reasoning as the retention guard (ADR 0006). A sudden large loss is
far more likely a failed lookup than a real change. A failed refresh costs only a day of
staleness, while a published undercount affects every hour ingested afterwards.

Resolution also never returns a partial result. If one wiki cannot be asked, the refresh
fails, because a filter missing one wiki's redirects is an undercount for that language
that nothing downstream could detect.

### Backfill from raw, not from Wikimedia

A bigger filter changes what extraction keeps, so every existing bronze partition was
stale. The 7-day raw window exists for exactly this (ADR 0003). `python -m
wikiflow.reprocess` rebuilt all 54 hours from object storage in **3m35s** (about 4 s an
hour), re-verifying each file and writing an audit record per hour under its own
`dag_id`. Nothing was downloaded from Wikimedia.

## Found along the way

- **A title containing `"` broke the filter load.** Hebrew uses `"` as an abbreviation
  mark (`אלפבת_בע"מ`, a redirect to Alphabet). Python's `csv.writer` doubles the quote,
  and DuckDB's auto-detected escape rejected it. Extraction loads the filter through CSV
  the same way, so it had the same latent bug; no sitelink title had contained a quote
  before. Both now declare `quote` and `escape` explicitly, and a test covers it.
- **Adding columns to an incremental model is silently lossy by default.** dbt's
  `on_schema_change='ignore'` would have built `fct_company_pageviews_hourly` without
  the new columns. The model now uses `'fail'`, so a schema change demands a
  `--full-refresh` instead of losing a column quietly.
- **Old bronze partitions lack the new columns.** The bronze source reads with
  `union_by_name=true`, and staging defaults them to `sitelink`, which is exactly what
  every row in such a partition was.

## Consequences

- Company view counts rise 2.7% overall, and comparisons with figures from before this
  change are not like-for-like. The split is recorded, so the earlier basis can be
  recovered by filtering `match_type = 'sitelink'`.
- Wikimedia's per-article pageviews API does not fold redirects in either. Figures here
  will now be higher than that API reports for the same article, by design.
- A redirect created today is counted only from the next refresh onward. Hours
  already ingested are not re-extracted unless reprocessed.
- Redirects to a section (`Apple_Inc.#History`) are included: the reader landed on the
  article.

## Addendum — the guard, exercised

The shrink guard was tested against the live filter, not only in unit tests. A refresh was
simulated in which Wikidata answered but every redirect lookup came back empty: 8,742 rows
dropped to 2,240, with all 2,240 articles intact. `publish` refused in 0.08 s with
`rows fell from 8,742 to 2,240 (74.4%, limit 10%)`. `current.parquet` kept its ETag, and
the snapshot count stayed at 3. Full write-up: `docs/FAILURE_DRILLS.md`, drill 4.
