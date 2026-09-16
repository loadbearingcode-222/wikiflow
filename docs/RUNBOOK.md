# Runbook

What each failure means and what to do about it. Every alert below should be
actionable — if one fires and the answer is "ignore it", the threshold is wrong and
should be changed rather than tolerated.

## Is the pipeline healthy right now?

```bash
docker compose ps                                    # all services healthy?
docker compose exec airflow-scheduler airflow dags list-runs pageviews_ingest --state failed
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

## `pageviews_ingest` — `429 Too Many Requests`

**Means:** we exceeded dumps.wikimedia.org's limit of **3 connections per IP**.

This has happened. Five downloads once ran at once: a scheduler backfill, a shell-loop
backfill that was believed dead but was not, and the hourly schedule. `max_active_runs`
did not prevent it, because it is per DAG and counts those run types separately.

The `wikimedia_dumps` pool (2 slots) is what bounds this now, across every run type.
Downloads also retry 429s, waiting exactly as long as `Retry-After` says. If the server
asks for longer than 5 minutes, the download stops retrying and leaves it to Airflow's
slower task-level retry rather than shortening the wait.

**Check:**

```bash
docker compose exec airflow-scheduler airflow pools list          # wikimedia_dumps | 2 ?
docker compose exec airflow-scheduler airflow dags list-runs pageviews_ingest --state running
```

If the pool is missing, tasks assigned to it will not run at all — recreate it:

```bash
docker compose exec airflow-scheduler airflow pools set wikimedia_dumps 2 "Connections to dumps.wikimedia.org (cap: 3 per IP)"
```

**Do not raise the pool above 2.** Wikimedia states that clients evading its limits may
be blocked, and a block would stop ingestion entirely.

Also check for anything outside the scheduler downloading from the same IP. Pools only
bound tasks the scheduler runs: `airflow dags test` executes in-process and ignores them.
That is why backfills go through `airflow backfill create` (see *gaps detected* below)
and never through a shell loop around `dags test` - the project once had such a script,
and it was a cause of this incident.

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

**Action:** first establish whether Airflow already has a run for that hour, because
the recovery command depends on it. Airflow 3.3 allows **one run per logical date** — a
second `dags trigger` for the same hour fails with `duplicate key ... dag_run_dag_id_
logical_date_key`.

```bash
# dag_id is positional. Filter by the hour in the output, not with --start-date /
# --end-date: those filter by when a run EXECUTED, not by which hour it covers.
docker compose exec airflow-scheduler airflow dags list-runs pageviews_ingest | grep "2026-09-14T0[3-5]"
```

| Situation | Command |
|---|---|
| **A run exists** — including one marked *success* whose partition was later lost | `airflow tasks clear` (below) |
| **No run exists** for the hour | `airflow backfill create` (below) |

A run that exists and succeeded is the case the backfill command gets wrong:
`--reprocess-behavior failed` skips successful runs, so it would leave the hole in place.
This was found by deleting a published partition deliberately — see `FAILURE_DRILLS.md`.

```bash
# A run exists: re-run it in place. Measured: 21 seconds, identical partition restored.
docker compose exec airflow-scheduler airflow tasks clear pageviews_ingest   --start-date 2026-09-14T03:00:00+00:00 --end-date 2026-09-14T05:00:00+00:00 --yes

# No run exists: create runs. The scheduler owns them, so they survive your shell.
docker compose exec airflow-scheduler airflow backfill create   --dag-id pageviews_ingest   --from-date 2026-09-14T03:00:00+00:00 --to-date 2026-09-14T05:00:00+00:00   --max-active-runs 2
```

Load on Wikimedia is bounded by the `wikimedia_dumps` pool whichever you use.

If the gap is older than the 7-day retention window the raw file is gone locally, but
Wikimedia's dumps are archival — the same backfill works, just with a re-download.

---

## `pageviews_quality` — stale data

**Means:** the newest partition is more than 8 hours old.

**Check in order:**

1. Is the scheduler alive? `docker compose ps`
2. Is the DAG paused? `airflow dags list` → `is_paused`
3. Are runs failing? `airflow dags list-runs pageviews_ingest --state failed`
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
partition wholesale, so re-running cannot double-count (ADR 0002). Clear the existing
run rather than triggering a new one: Airflow permits one run per logical date, so
`dags trigger --logical-date` for an hour that has a run fails with a uniqueness error.

```bash
docker compose exec airflow-scheduler airflow tasks clear pageviews_ingest   --start-date 2026-09-14T11:00:00+00:00 --end-date 2026-09-14T11:00:00+00:00 --yes
```

Avoid `airflow dags test` for this while anything else is running: it executes
in-process and **ignores pools**, so it can push past Wikimedia's connection cap.

**A task stuck in `restarting`.** Clearing a task Airflow believes is *running* sets it
to `restarting`, which waits for that process to exit first. If the process is already
dead — its container was restarted, which kills everything inside — it waits forever.
Confirm it is dead (the container's start time is later than the task's start time),
mark it failed, then clear it:

```bash
docker inspect -f '{{.State.StartedAt}}' wikiflow-airflow-scheduler-1   # later than the task start?
# mark failed via the REST API, then:
docker compose exec airflow-scheduler airflow tasks clear pageviews_ingest   --start-date <hour> --end-date <hour> --yes
```

Forcing state is correct only here, where the recorded state is already false.
