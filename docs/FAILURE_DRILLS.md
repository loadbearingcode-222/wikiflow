# Failures, real and rehearsed

A green DAG demonstrates that the happy path works. What matters in operation is what
happens when it doesn't: whether a failure is **detected**, whether it is **contained**
before bad data reaches anyone, and whether **recovery** is routine.

This document has two parts. The first is an incident that actually happened while
building this project, reconstructed from the audit trail. The second is a set of
deliberate drills against the running stack. Every number below was measured; nothing is
illustrative.

---

## Incident: rate-limited by the source (2026-09-16)

### Summary

A 48-hour backfill ran far slower than expected and some hours failed. The cause was
the pipeline opening more concurrent connections to dumps.wikimedia.org than its
published limit of **3 per IP**, which produced `429 Too Many Requests` and at least one
transfer cut off mid-body. No bad data was published — every failed hour failed loudly —
but the backfill stalled, and the root cause was misdiagnosed twice before the audit
trail made it obvious.

### Timeline (UTC)

| Time | Event |
|---|---|
| 07:24 | A shell-loop backfill starts (`scripts/backfill.sh`), running `airflow dags test` two hours at a time. |
| ~07:28 | The loop is **believed dead**: a later `ps` from a different shell session shows no process. This was wrong. |
| 07:32 | A proper scheduler backfill is created (`airflow backfill create`, max 2 active runs), and the DAG is unpaused, which also starts the hourly schedule. |
| 07:38–08:07 | New `manual__` runs appear at 07:38, 07:41, 07:59 and 08:07 — times when nothing was started by hand. The shell loop was still running. |
| ~07:50 | Task durations have risen from ~190 s to ~600 s. **Misdiagnosed as bandwidth**: "the downloads are contending". |
| 08:07 | Hour 04: `RemoteProtocolError: peer closed connection without sending complete message body (received 15,203,978 bytes, expected 52,149,995)` after 22 minutes. |
| — | Hour 05, attempt 3, and the scheduled run for 09-16 07:00: `HTTPStatusError: 429 Too Many Requests`. |
| 14:03 | Containers restart; work resumes. |
| 14:06 | The audit records are read back for the first time. The three failures above are all there, with their error messages. |
| 14:10 | Wikimedia's published policy is checked: *"capped at 3 connections per-IP"*. The concurrency is reconstructed: up to five downloads at once. |
| ~14:40 | Fixes deployed (below). |

### Root cause

Three independent sources of load, none of which knew about the others:

1. the scheduler backfill (2 concurrent runs),
2. the hourly schedule, once the DAG was unpaused,
3. the shell-loop backfill that was assumed to have exited.

The DAG's `max_active_runs=2` looked like it bounded this. It did not. It is **per DAG**
and counts backfill, scheduled and manual runs separately — and `airflow dags test`,
which the shell loop used, runs tasks in-process and bypasses scheduling limits entirely.

### Why detection was slow

The two misdiagnoses shared a cause: they were reasoned from symptoms (durations,
process listings) rather than from recorded outcomes. The durations were real, but
"slow" was compatible with many explanations. The process listing was simply wrong.

The audit trail settled it in one query, because it recorded **failed attempts with
their errors** — not just the final state of each hour. An hour that eventually
succeeded on retry would have looked healthy in the partition listing.

### Containment: what went right

No hour was published short. The dropped connection raised `RemoteProtocolError` during
the transfer itself — after 29% of the file's bytes — so the task failed before
verification even ran. Nothing is written to bronze until a download has completed and
verified, and the retry re-fetched the file from the beginning.

Drill 1 below tests the harder case: a transfer that *completes* but is truncated, where
no transport error saves you.

### Fixes

| Fix | Why it addresses the cause |
|---|---|
| A `wikimedia_dumps` Airflow **pool** with 2 slots, taken by every task that contacts the source | A pool is global across all runs and DAGs, which `max_active_runs` is not. Two slots leave one of Wikimedia's three spare. |
| Download **retries honour `Retry-After` exactly**, and defer to Airflow's task retry if the server asks for more than 5 minutes | Shortening the wait the server asked for is the evasion Wikimedia says it may block clients for. |
| `scripts/backfill.sh` **deleted** | It wrapped `dags test`, which ignores pools, so it would have bypassed the fix. Backfills go through `airflow backfill create`. |
| A DAG test asserts every source-contacting task is in the pool, and that the pool is created below the cap | So the protection cannot silently regress. |

The investigation also exposed two bugs in the audit table itself: its column types
depended on content (a successful run wrote `error` as INTEGER, a failure as VARCHAR,
so reading across both broke), and retries of the same run overwrote each other's
records. Both are fixed and tested.

### Outcome

After the fix: hours ingest in **17–21 seconds** each (the ~600 s figure was throttling,
not bandwidth), the backfill completed with **48 of 48 hours present**, and every failed
attempt in the audit trail predates the fix.

---

## Drill 1 — a download truncated mid-transfer

**Question:** if a transfer dies partway through, what reaches the warehouse?

**Method:** take a real hourly dump (2026-09-14 12:00, 66,743,551 bytes, gzipped), keep
the first 30%, and feed it to each layer.

| Reader | Result |
|---|---|
| DuckDB on the intact file | 7,635,493 rows, 22,050,854 views |
| DuckDB on the truncated file, `ignore_errors=true` | **2,111,414 rows, 5,315,410 views — no error** |
| DuckDB on the truncated file, `ignore_errors=false` | `InvalidInputException: CSV Error on Line: 2111415` |
| Download verification | Rejected: *"Compressed file ended before the end-of-stream marker was reached"* |

**Finding:** the silent loss was **our own configuration**, not DuckDB. `ignore_errors`
exists so one garbage line cannot abort a whole hour — and it also turns a truncated file
into a smaller, perfectly plausible hour. 72% of the data would have vanished with every
downstream check still passing on internal consistency.

Verification caught it, but that made verification the only defence, and the pipeline
was already counting skipped lines as `rows_malformed` — then ignoring the count.

**Change made:** extraction now refuses an hour when more than **1 line in 100,000**
fails to parse, and checks before writing anything. The threshold is strict because it
can be: across 40 real hours, about 270 million lines, exactly **zero** failed to parse.

---

## Drill 2 — a published partition is lost

**Question:** if an hour disappears from bronze after it was published, is that noticed,
and can it be restored exactly?

**Method:** delete both grains for 2026-09-15 09:00 from object storage, run the gap
check, recover through the scheduler, and compare the result with what the audit trail
recorded when the hour was first published.

| Step | Result |
|---|---|
| Baseline (from the audit record) | 396 company rows, 5,087 company views, 16,180,602 total views |
| Delete both grains | 2 objects removed |
| Gap check | **`1/24 hours MISSING: 2026-09-15 09:00`** |
| Recover | existing run cleared and re-executed through the pool: **21 s** |
| After | 396 company rows, 5,087 company views, 16,180,602 total views, 2 objects |
| Gap check | `no gaps: 24/24 hours present` |

**Result:** detected, and restored **identically** — the same rows, the same views, one
object per grain.

**Two things the drill exposed, both now fixed in the runbook:**

1. **The documented recovery command did not work.** The runbook said to re-run an hour
   with `airflow dags trigger --logical-date`. Airflow 3.3 enforces one run per logical
   date, so it failed with
   `duplicate key value violates unique constraint "dag_run_dag_id_logical_date_key"`.
   The gap section's `backfill create --reprocess-behavior failed` was also wrong for
   this case: the lost hour's run had *succeeded*, so it would have been skipped. The
   correct tool is `airflow tasks clear`, which re-runs the existing run in place.
   Neither mistake was visible until the procedure was actually executed.

2. **A dead task that Airflow never reaped.** A run left over from the rate-limiting
   incident still showed `ingest` as *running*, eight hours after its container had been
   restarted twice. It was occupying one of the DAG's two active-run slots. Clearing a
   task Airflow believes is running sets it to `restarting`, which waits for the process
   to exit — forever, when the process is already gone. The runbook now covers confirming
   the task is dead, marking it failed, and clearing it.

The drill script itself also hung: it called `docker compose exec` through
`subprocess.run` without redirecting stdin, and compose blocked waiting on it. It had
already deleted the partition, so the warehouse had a real hole until recovery was done
by hand. The same stdin trap had been noted earlier in this project and was repeated —
recorded here because a drill that damages data and then stalls is exactly the scenario
a runbook has to survive.

---

## Drill 3 — retention misconfigured to expire everything

**Question:** if the retention window or the clock is wrong, does the sweep destroy the
raw layer?

**Method:** run a real (not dry-run) sweep with `retention_days=0` and the clock set 30
days ahead, so every raw partition appears expired.

| Step | Result |
|---|---|
| Raw partitions before | 98 |
| Misconfigured sweep | **Refused:** *"all 98 partitions are older than 2026-10-16. That means the retention window or the clock is wrong, not that every hour expired at once."* |
| Raw partitions after | 98 — unchanged |
| Correct configuration, dry run | would delete 0 partitions; retained 98 (the data is 2 days old, the window is 7) |

**Result:** nothing deleted. The guard exists because retention is the only component
whose bug destroys data rather than producing a wrong answer, and a sweep that finds
*everything* expired is far more likely to be misconfigured than correct.

---

## Drill 4 — the filter refresh loses the redirects

**Question:** if a daily refresh comes back without the redirects, as it would during a
MediaWiki outage while Wikidata still answers, does the smaller filter get published?

**Why it is the dangerous case:** every article is still there. A guard that counted only
articles would pass it, while every hour ingested afterwards silently lost about 2.7% of
company views (ADR 0007). Nothing downstream could tell that from a quiet day.

**Method:** take the live published filter, keep only its article rows, and call
`publish` against the real object store.

| Step | Result |
|---|---|
| Current filter | 2,240 articles, 8,742 rows |
| Simulated refresh | 2,240 articles, 2,240 rows |
| `publish` | **Refused in 0.08 s:** *"rows fell from 8,742 to 2,240 (74.4%, limit 10%)"* |
| `current.parquet` | Same ETag as before |
| Snapshots | 3 before, 3 after |

**Result:** nothing published, and ingestion keeps using the full filter. The refusal
happens before any write, so there is no half-published state to clean up.

---

## Drill 5 — the Postgres load dies half-way through a partition

**Question:** the loader deletes an hour and then copies its rows back in, on a connection
in autocommit mode. If the connection dies between the two, is the hour gone?

**Method:** in the live warehouse, reload `bronze.domain_totals` for 2026-09-15 09:00. The
copy is made to raise after 999 rows, once the DELETE has already run. Then compare the
partition and its manifest row with their state before.

| | Rows | Views | Manifest ETag | Loaded at |
|---|---:|---:|---|---|
| Before | 1,949 | 16,180,602 | `3f461919…` | 04:13:19.90 |
| Load fails after 999 rows | | | | |
| After | 1,949 | 16,180,602 | `3f461919…` | 04:13:19.90 |

The next `sync` loaded 0 partitions and reported 162 unchanged. Parity against DuckDB:
identical.

**Result:** the DELETE rolled back with the COPY. The explicit per-partition transaction
does wrap both, even on an autocommit connection. The manifest never recorded the ETag
that the failed load was carrying, so a failed load cannot be mistaken for a completed
one. The view total matches the lost-partition drill (drill 2) for the same hour: two
independent paths, one number.

The same scenario is now part of the CI end-to-end test.

---

## A test that passed for the wrong reason

Not a drill against the pipeline, but against its tests, and worth recording for the same
reason.

The end-to-end test for "a rewritten hour reaches the marts" passed on its first run. It
was then run against the rule it was written to replace, the old `view_date` watermark.
**It passed there too.** Both fixture hours were on the same date, and the old rule
rebuilds every hour of the newest date regardless. The test could not fail, so it proved
nothing.

With the rewritten hour moved to an earlier date, the test fails against the old rule
(its "not comparable yet" check finds the warehouses disagreeing) and passes against the
new one. Every test of a fix since then has been run once against the unfixed code first.
