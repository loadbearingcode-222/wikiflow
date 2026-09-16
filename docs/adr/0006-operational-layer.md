# ADR 0006 — What the operational layer watches, and what it refuses to do

- **Status:** Accepted
- **Date:** 2026-09-16
- **Decision:** Three independent concerns — audit, health checks, retention — each in
  its own DAG, none of them inside the ingest DAG.

## Context

The pipeline was correct before this layer existed, in the sense that every hour it
processed, it processed right. What it could not do was answer three operational
questions:

1. What did run, and how did it go? An hour that failed twice before succeeding left
   no trace of the first two attempts.
2. Is anything missing or stale? **A run that never happened raises no error.** No
   amount of in-pipeline validation can see a hole, because there is no pipeline
   execution in which to look.
3. Is storage growing without bound? Yes — about 500 GB a year.

## Decisions

### Audit is append-only; data partitions are overwrite

The data layer replaces a partition wholesale on re-run, which is what makes
re-running safe (ADR 0002). Audit does the opposite: each attempt writes its own file
keyed by run id.

The asymmetry is the point. For data, history is a liability — a second copy of an
hour is a double count. For audit, history is the entire value: during an incident the
question is "what happened on the attempts that failed", and overwrite-semantics would
have erased exactly that.

The record is written from a `finally` block, and the write itself never raises. An
exception in the audit path would replace the real failure with an audit-writing
failure, hiding the thing the record exists to explain.

### Audit lives in object storage, not the warehouse

DuckDB permits a single writer. Having the ingest DAG write audit rows into the same
file dbt builds marts in would create lock contention between two DAGs that are
deliberately decoupled by an Asset. Parquet in S3 has no such constraint and dbt can
still read it as a source.

### The anomaly check compares the same hour across days

Wikipedia traffic swings about 1.6× between its daily trough and peak. An
hour-over-hour check therefore fires every dawn and every dusk, and within a week
nobody reads the alert.

So each hour is compared against **the same hour of day on other days**, using their
mean and standard deviation. The threshold is 4σ, deliberately loose: this check exists
to catch a truncated file or a half-written partition, not to detect that something
interesting happened in the world. Real news events will exceed it, and that is a
correct result rather than a false positive.

With fewer than three other days of history the check reports *"skipped, and here is
why"* instead of judging. A standard deviation over two samples is not a signal, and a
fresh deployment should not alarm on every hour.

Both ends are pinned by tests: a five-day cycle where 18:00 is double 03:00 produces
zero anomalies, and a single hour at a quarter of its normal volume is flagged.

### Retention is a separate DAG, not a cleanup task

Backfill runs execute out of chronological order. A cleanup task inside the ingest DAG
would therefore run *inside a backfill of an old hour*, and delete "anything older than
N days" relative to a wall clock that has nothing to do with the hour being processed —
potentially removing raw files a later backfill run still needs.

Decoupling makes retention depend on wall-clock time only, which is what the policy is
actually about.

### Retention is written around what it refuses to do

It is the only module here whose bugs destroy data rather than produce wrong answers,
so the guards are the design:

| Guard | Reason |
|---|---|
| Only raw prefixes are eligible | Bronze and marts are kept indefinitely and must be unreachable from this code, even by misconfiguration |
| Unparseable keys are reported, never swept | An unrecognised layout means something changed; that is a reason to look, not to wildcard-delete |
| Refuses to delete *every* partition | Everything expiring at once means the window or the clock is wrong |
| `dry_run` is the default | A mistaken call should be harmless |
| Plan task runs before apply task | The manifest is logged before anything is deleted, so the two can be compared afterwards |

### Alerting reads its own configuration

`wikiflow_airflow.alerts` reads the webhook straight from the environment rather than
through `wikiflow.config`. Alerting is what you need most when something else is broken,
so a failure to load settings must still be able to report that settings failed to load.
Delivery is best-effort and never raises — a webhook that is down must not turn one task
failure into two.

## Consequences

- Three ops DAGs run independently of ingestion and of each other. None can block the
  data path.
- The quality DAG reports all three checks together rather than failing on the first,
  so one run surfaces everything wrong instead of turning one incident into three.
- Thresholds are tied to measured behaviour, not intuition: the freshness threshold
  (8h) exceeds the measured worst-case publication lag (~3h50m past the end of the
  covered interval, ADR 0001), and a DAG test asserts that relationship so tightening
  it later fails the build rather than the on-call.
