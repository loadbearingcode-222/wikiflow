# ADR 0005 — dbt for transformation, and where business logic is allowed to live

- **Status:** Accepted
- **Date:** 2026-09-16
- **Decision:** dbt-duckdb builds the marts, in its own virtualenv, triggered by an
  Airflow Asset. Rules that are hard to express correctly stay in Python.

## Context

Bronze is two Parquet grains per hour in object storage. Turning that into something
answerable — "what share of Wikipedia traffic did Apple get this hour, and is that
unusual?" — needs a modelling layer.

Three questions had to be settled.

## 1. Why dbt rather than SQL in Airflow tasks

dbt brings three things this project wants and would otherwise hand-roll: model-level
tests that run next to the models, a lineage graph, and `ref()` so the DAG of models is
derived rather than maintained. `dbt build` interleaves models and their tests, so a
failing test stops its dependents being built on top of data already known to be wrong —
which is the behaviour a quality gate should have.

The cost is another tool. Accepted, because the alternative is reimplementing the same
three features worse.

## 2. Why dbt lives in a separate virtualenv

dbt-core and Airflow both pin `jinja2`, `click` and `protobuf`, and resolving them into
one environment is a well-known source of breakage — often at runtime rather than
install time, which is worse.

The image therefore builds `/home/airflow/dbt-venv` and the DAG invokes the CLI by
absolute path. It costs about 120 MB and removes the conflict class entirely.

Rejected alternatives: installing dbt alongside Airflow (fragile), and a separate dbt
container invoked by DockerOperator (more moving parts than the problem justifies here).

## 3. Why domain-code parsing is NOT a dbt macro

This is the load-bearing one.

The `domain_code` field encodes project, language and access method, and the rules are
genuinely hostile:

- `en.m` — `.m` means **mobile**
- `commons.m` — `.m` means **wikimedia.org**, and the row is desktop
- `commons.m.m` — both meanings, in one code
- `m.wd` — prefix-less wiki, so the mobile marker moves to the **front**
- `simple`, `abstract`, `test`, `beta` — look like special sites, behave like languages

Implementing that twice, once in Python and once in SQL, guarantees the two drift. So it
is implemented **once**, in `wikiflow.parsing`, tested against all 1,946 codes in a real
hourly file, and the result is written into the bronze layer. dbt consumes the decoded
columns and never re-derives them.

This is affordable only because the parse runs on the **aggregated** grain: ~1,955
distinct domain codes per hour, not 7.45M rows. Had it needed to run per row, the
trade-off would have gone the other way and the rule would have had to be expressed in
SQL — which is worth knowing, because it is the reason the two-grain design (ADR 0003)
makes this possible.

Unparseable codes are kept with `is_parseable = false` rather than dropped. A real file
contains a literal `""` code carrying thousands of views an hour, and Wikimedia counts
it in their own totals — dropping it would break the reconciliation test below.

## Consequences

- **The reconciliation test asserts exact equality.** Our independently computed wide
  grain must equal Wikimedia's published `projectviews` for the same hour, per domain
  code. Measured before the test was written: all 1,955 codes matched to the view,
  totals identical (19,322,047). A tolerance would hide precisely the bugs worth
  catching.
- **The fact table is incremental with `delete+insert` on `(view_date, view_hour)`** —
  the SQL expression of the same rule ingestion follows (ADR 0002). No append path
  exists anywhere in the pipeline.
- **Transformation is Asset-triggered, not scheduled.** Publication latency varies by
  about 90 minutes (ADR 0001), so any cron offset would be wrong in one direction or
  the other. A DAG test asserts the producing outlet and consuming schedule name the
  same URI, since a typo there would silently mean the transform never runs.
- Shared Airflow objects live in `src/wikiflow_airflow/`, not next to the DAGs: under
  Airflow 3.3's bundle loader the dags folder is **not** on `sys.path`, while
  `/opt/airflow/src` is. `src/wikiflow/` stays Airflow-free, enforced by a test.
