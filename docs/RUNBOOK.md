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

## `pageviews_ingest` — `ContractViolation`

**Means:** extraction produced a partition that does not match the bronze data contract
(ADR 0009), and **refused to publish it**. Bronze still holds the previous version of that
hour, or nothing if it is new.

The message names the model, the number of violations, and the first five with the row
and value. For example:

```
domain_totals.parquet: 1 contract violation(s) in 1,954 rows against DomainTotalRecord
  row 17, views: Input should be a valid integer (got 3882.0)
```

| Kind | Likely cause |
|---|---|
| `columns do not match` | Extraction's `SELECT` changed without `wikiflow/contracts.py`, or the reverse. Change both, in one commit |
| A type error (`valid integer`, `valid string`) | A cast was lost in extraction SQL, as with the DOUBLE sums before Phase 5 |
| `not in partition dt=…/hour=…` | The dump was attributed to the wrong hour; see ADR 0001 |
| `match_type` / `access_method` / `qid` | The company filter contains a bad row; check the latest snapshot |

Do not loosen the contract to make an hour pass. Fix the cause, then re-run the hour.

---

## `pageviews_ingest` — zero company rows

**Symptom:** `verify` raises "produced zero company rows".

**Means:** the filter matched nothing. Real traffic to these articles never stops, so
this is a broken filter, not a quiet hour.

**Diagnose in order:**

1. What filter did the run use? `ensure_company_filter` logs its size. Expect about
   2,240 article rows and 8,700 rows in total (articles plus redirects). The filter is
   published daily by `company_pages_refresh`; see *filter refresh refused* below.
2. Did Wikidata change an article title? Re-resolve without publishing and compare:
   ```bash
   docker compose exec airflow-scheduler python -c "
   from pathlib import Path
   from wikiflow.wikidata import load_companies, resolve_company_pages
   pages = resolve_company_pages(load_companies(Path('/opt/airflow/seeds/companies.csv')))
   print(len(pages), 'article rows')"
   ```
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

## `company_pages_refresh` — filter refresh refused or failed

**Symptom:** `ReferenceShrinkError: refusing to publish: rows fell from N to M`, or
`RedirectResolutionError`.

**Means:** a new filter came back much smaller than the one in use, or one wiki could
not be asked about its redirects. **Nothing was published**: ingestion keeps using the
current filter, so the only cost is that it goes a day staler. Article titles change
over weeks, so this is not urgent.

Neither error is retried into a partial result. A filter missing one wiki's redirects
would undercount that language in every hour ingested afterwards, and nothing
downstream could tell that apart from a quiet day.

**Check:** the task log shows which wiki failed, or the before/after counts. Then look
at the published snapshots, one per successful refresh:

```bash
docker compose exec airflow-scheduler python -c "
from wikiflow.config import get_settings
from wikiflow.storage import list_keys
print(*list_keys(get_settings().lake_bucket, 'reference/company_pages/'), sep='
')"
```

If the shrink is real - a company was removed from `seeds/companies.csv` - publish once
with the guard lifted:

```bash
docker compose exec airflow-scheduler python -c "
from pathlib import Path
from wikiflow.redirects import resolve_filter
from wikiflow.reference import publish
print(publish(resolve_filter(Path('/opt/airflow/seeds/companies.csv')), max_ratio=1.0))"
```

---

## `pageviews_transform_postgres` — load or build fails

**Means:** the Postgres marts are stale. The DuckDB marts and the dashboard are
unaffected; the two transform DAGs are independent on purpose (ADR 0008).

| Failing task | Likely cause |
|---|---|
| `load_bronze`, connection refused | `warehouse-postgres` is down: `docker compose ps warehouse-postgres` |
| `load_bronze`, `more than one object` | Something other than the pipeline wrote into a bronze partition. Look at the named keys; do not delete either one until you know which is right |
| `dbt_build`, `dbt_utils` not found | The DuckDB transform DAG installs dbt packages. On a fresh stack, let it run once |
| `dbt_build`, a test fails | As for the DuckDB DAG below. The reconciliation test runs here too, against Postgres' copy of projectviews |

The load is safe to re-run at any time. It loads only partitions whose object changed
since the last load, each in its own transaction:

```bash
make pg-sync
```

**`warehouse_parity` failed.** Read the log's first line:

| Log says | Means | Action |
|---|---|---|
| `not comparable yet: N hour(s)…` | One warehouse built bronze writes the other has not yet. Timing, not a defect | Nothing: the task retries every 20 minutes. If retries run out, one transform DAG has stopped; check `pageviews_quality`'s Postgres lag result |
| `marts DIFFER` | Both built from the same bronze, and the data disagrees | A defect, and the task does not retry. The table names the mart; compare its rows in both warehouses. Most likely a model or dialect macro that behaves differently on one target |

**`pageviews_quality` — postgres lag.** The Postgres marts trail the newest bronze hour by
more than 2 hours, or are empty. Check `pageviews_transform_postgres` is unpaused and its
last runs succeeded; then `make pg-sync` and trigger it.

**Do the two warehouses agree?**

```bash
make parity
```

Run it while neither transform DAG is building. A difference in `fct_*` or `agg_*`
alone, with the dims identical, usually means one warehouse has not caught up yet: run
`make pg-sync`, rebuild, and compare again before investigating.

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

**Reprocess hours from raw, without re-downloading.** After a change to extraction
or to the company filter, rebuild every affected hour still inside the 7-day raw
window. Each hour is re-verified, extracted with the current published filter, written
to bronze exactly as ingestion does, and audited under `dag_id=reprocess_from_raw`:

```bash
make reprocess FROM=2026-09-14T00 TO=2026-09-15T23      # Windows: .\make.ps1 reprocess -From ... -To ...
```

Hours with no raw file are skipped and listed. Reprocessing does not publish an Airflow
Asset, so trigger both transform DAGs afterwards. No full refresh is needed: the marts
pick up every hour whose bronze was rewritten (ADR 0008, addendum).

```bash
docker compose exec airflow-scheduler airflow dags trigger pageviews_transform
docker compose exec airflow-scheduler airflow dags trigger pageviews_transform_postgres
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
