# Runbook

What each failure means and what to do about it. Every alert below should be
actionable — if one fires and the answer is "ignore it", the threshold is wrong and
should be changed rather than tolerated.

## Is the pipeline healthy right now?

```bash
docker compose ps                                    # all services healthy?
docker compose exec airflow-scheduler airflow dags list-runs -d pageviews_ingest --state failed
```

Or trigger the checks directly:

```bash
docker compose exec airflow-scheduler airflow dags test pageviews_quality "$(date -u +%Y-%m-%dT%H:00:00+00:00)"
```

`pageviews_quality` runs hourly and reports all three checks together rather than
failing on the first — one run tells you everything that is wrong.

---

## `pageviews_ingest` — sensor times out

**Symptom:** `wait_for_publication` fails after 5 hours.

**Means:** Wikimedia has not published the file for that hour.

Remember the label is the **end** of the period (ADR 0001), so the file for interval
`14:00` is named `...-150000.gz`. Check it directly:

```bash
curl -I -A "wikiflow/0.1" https://dumps.wikimedia.org/other/pageviews/2026/2026-09/pageviews-20260914-150000.gz
```

| Result | Action |
|---|---|
| `200` | The sensor was unlucky or the network blipped. Clear the task and re-run. |
| `404`, file < 5h old | Normal. Wikimedia's publication lag reaches ~3h50m past the end of the covered interval. Wait. |
| `404`, file > 8h old | Genuinely missing upstream. Mark the run failed and let the gap check report it. Do **not** synthesise a partition. |

---

## `pageviews_ingest` — download verification fails

**Symptom:** `DownloadVerificationError` in the `ingest` task.

**Means:** the file arrived corrupt or truncated. This is the check working, not
failing — the alternative is an hour that looks fine and is quietly short.

The message says which of the four checks tripped:

| Message | Cause |
|---|---|
| `expected N bytes from Content-Length but wrote M` | Connection dropped mid-transfer |
| `content failed to decompress` | Truncated mid-stream; gzip's CRC32 caught it |
| `response body was empty` | Upstream served a zero-length file |
| `file contains no lines` | Decompressed to nothing |

**Action:** retry. All four are usually transient. Retries are already configured with
exponential backoff, so a failure that survives three attempts is worth investigating
rather than re-running by hand.

---

## `pageviews_ingest` — zero company rows

**Symptom:** `verify` raises "produced zero company rows".

**Means:** the filter matched nothing. Real traffic to these articles never stops, so
this is a broken filter, not a quiet hour.

**Diagnose in order:**

1. Did Wikidata change an article title? Re-run the resolution and compare:
   ```bash
   docker compose exec airflow-scheduler python -c "
   from pathlib import Path
   from wikiflow.wikidata import load_companies, resolve_company_pages
   pages = resolve_company_pages(load_companies(Path('/opt/airflow/seeds/companies.csv')))
   print(len(pages), 'pairs')"
   ```
   Expect ~2,238 pairs. A much smaller number means the Wikidata lookup partly failed.
2. Did the Wikidata API return an error? The task would have failed earlier with
   `SitelinkResolutionError`, so if it got this far the API answered.
3. Is the dump itself unusually small? Compare `rows_scanned` in the audit record
   against neighbouring hours.

---

## `pageviews_quality` — gaps detected

**Means:** one or more hours have no bronze partition.

**Action:** backfill exactly those hours. The scheduler owns it, so it survives your
shell:

```bash
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id pageviews_ingest \
  --from-date 2026-09-14T03:00:00+00:00 \
  --to-date   2026-09-14T05:00:00+00:00 \
  --max-active-runs 2 --reprocess-behavior failed
```

`--max-active-runs 2` matches the DAG limit, which exists to be a good citizen toward
a free public service. Raising it routes around the limit rather than respecting it.

If the gap is older than the 7-day retention window the raw file is gone locally, but
Wikimedia's dumps are archival — the same backfill works, just with a re-download.

---

## `pageviews_quality` — stale data

**Means:** the newest partition is more than 8 hours old.

**Check in order:**

1. Is the scheduler alive? `docker compose ps`
2. Is the DAG paused? `airflow dags list` → `is_paused`
3. Are runs failing? `airflow dags list-runs -d pageviews_ingest --state failed`
4. Is Wikimedia publishing at all? Open
   <https://dumps.wikimedia.org/other/pageviews/2026/2026-09/> and look at the newest
   file.

If (4) is the answer, there is nothing to fix here — note it and wait.

---

## `pageviews_quality` — volume anomaly

**Means:** an hour's total views deviate more than 4σ from the same hour on other days.

The comparison is against the **same hour of day**, not the previous hour, because
traffic swings about 1.6× daily and an hour-over-hour check would fire every dawn and
dusk until people stopped reading it.

| Direction | Likely cause |
|---|---|
| Much lower | Truncated download that passed verification, or a partially-written partition |
| Much higher | A genuine news event. Real, and not a defect |

**Action:** cross-check the hour against Wikimedia's own totals — that is what the
reconciliation test does, so run it for the affected day:

```bash
cd dbt && dbt test --select assert_domain_totals_reconcile_with_projectviews
```

If reconciliation passes, the data is right and the world was simply busy.

---

## `pageviews_transform` — a dbt test fails

`dbt build` interleaves models and tests, so a failed test stops its dependents being
built on top of data already known to be wrong. The marts keep their previous contents.

**The important one:** `assert_domain_totals_reconcile_with_projectviews`. It compares
our independently computed per-domain totals against Wikimedia's published figures,
asserting **exact** equality. A failure means the extraction path is wrong — not the
model. Look at ingestion, not at SQL.

```bash
cd dbt && dbt test --select assert_domain_totals_reconcile_with_projectviews --store-failures
```

Then inspect the failing rows: `our_views` vs `their_views` per domain and hour.

---

## `raw_retention` — safety refusal

**Symptom:** `RetentionSafetyError: refusing to sweep: all N partitions are older
than ...`

**Means:** every partition looked expired, which means the window or the clock is
wrong, not that everything expired at once. The sweep deleted nothing.

**Check:** `WIKIFLOW_RAW_RETENTION_DAYS` in `.env`, and the container clock
(`docker compose exec airflow-scheduler date -u`).

To see what a sweep *would* do without touching anything — dry run is the default:

```bash
docker compose exec airflow-scheduler python -c "
from wikiflow.retention import sweep
print(sweep(dry_run=True).manifest())"
```

---

## Common operations

**Reprocess hours from raw, without re-downloading.** Within the 7-day window this runs
at about 4 seconds per hour instead of 3½ minutes:

```bash
docker compose exec airflow-scheduler python -c "
import csv, tempfile
from datetime import UTC, datetime
from pathlib import Path
import duckdb
from wikiflow.config import get_settings
from wikiflow.extract import extract_hour
from wikiflow.source import HourlyDump
from wikiflow.storage import configure_duckdb_s3, overwrite_partition
from wikiflow.wikidata import CompanyPage
# ... see scripts/ for the full form"
```

**Rebuild the marts from scratch:**

```bash
docker compose exec airflow-scheduler /home/airflow/dbt-venv/bin/dbt build \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt \
  --target prod --full-refresh
```

Safe at any time: the marts are derived, and a full refresh is idempotent.

**Re-run one hour end to end.** Safe by construction — every task replaces its
partition wholesale, so re-running cannot double-count (ADR 0002):

```bash
docker compose exec airflow-scheduler airflow dags test pageviews_ingest 2026-09-14T11:00:00+00:00
```
