"""Pools: global limits that hold across every run of every DAG.

`max_active_runs` looked like it bounded load on dumps.wikimedia.org. It did not. It is
per DAG and counts backfill, scheduled and manual runs separately, and in practice five
downloads ran at once - against a source that caps clients at 3 connections per IP.
The result was `429 Too Many Requests` and a transfer that died after 15 MB of 52 MB.

A pool is the right primitive: a fixed number of slots shared by every task assigned to
it, whatever run or DAG it belongs to.
"""

from __future__ import annotations

#: Every task that opens a connection to dumps.wikimedia.org takes a slot here.
WIKIMEDIA_DUMPS_POOL = "wikimedia_dumps"

#: Wikimedia's stated cap is 3 connections per IP. Two leaves a connection spare for a
#: human with curl, or anything else on the same network, without anyone tripping it.
WIKIMEDIA_DUMPS_SLOTS = 2

#: The documented limit, kept here so a test can assert the pool stays below it.
WIKIMEDIA_CONNECTION_CAP = 3

#: Every dbt invocation takes the single slot. Both transform DAGs - DuckDB and
#: Postgres - run dbt from the same project directory, and `dbt deps` rewrites
#: dbt_packages/ in place: a build parsing macros while another task reinstalls them
#: fails intermittently, in a way that would look like flakiness rather than a race.
DBT_PROJECT_POOL = "dbt_project"
DBT_PROJECT_SLOTS = 1
