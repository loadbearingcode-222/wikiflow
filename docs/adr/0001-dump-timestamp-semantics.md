# ADR 0001 — What the hour in a dump filename means

- **Status:** Accepted
- **Date:** 2026-09-16
- **Decision:** The filename hour is the **END** of the aggregation period.
  `pageviews-20260915-150000.gz` contains views from **14:00:00–14:59:59 UTC**.

## Context

The mapping from an Airflow data interval to a dump URL is the single most load-bearing
line of code in this pipeline. If it is off by one hour, every row lands in the wrong
partition — silently. No schema test, row-count check or freshness monitor would catch
it, because the data is perfectly well-formed; it is just attributed to the wrong hour.

The documentation is ambiguous in practice:

- Wikitech states filenames "refer to the end of the aggregation period, not the
  beginning."
- Observed publication times are consistent with **either** reading. The file labelled
  `00:00` on 2026-09-01 appeared at 02:21, and the one labelled `16:00` appeared at
  18:46. Under "label = start" that is a 1h21m–2h46m publication lag; under
  "label = end" it is 2h21m–3h46m. Both are plausible for a batch job.

Publication timing therefore cannot settle it, and the cost of trusting prose and being
wrong is a silently corrupt warehouse. So it was measured.

## Method

Wikimedia's AQS REST API reports hourly pageviews against unambiguous wall-clock hours.
If the dumps and AQS derive from the same upstream aggregation, their counts should match
**exactly** at the correct alignment — not merely correlate.

1. Download `projectviews-YYYYMMDD-HH0000` for 12 consecutive hours (~26 KB each) and
   take the `en` row. A bare `en` is English Wikipedia **desktop** only; mobile is a
   separate `en.m` row.
2. Query AQS for the same window: `project=en.wikipedia.org`, `access=desktop`,
   `agent=user`.
3. For each candidate lag, count hours matching to the exact view.

Reproduce with: `uv run python scripts/investigate_timestamp_semantics.py`

### A method that did not work

Ratio stability was tried first: compute `dump / AQS` per hour for each lag and pick the
lag with the lowest coefficient of variation. It was **inconclusive** — 13.3% vs 14.8%,
a 1.1x margin that justifies nothing.

The reason is worth recording. The AQS `all-agents` series includes a large, roughly
constant automated-traffic baseline. It swings only 1.24x across the day while the dump's
user-driven `en` series swings 1.63x. The ratio was therefore dominated by the dump's own
variation and barely responded to lag at all. Restricting to `agent=user` removed the
baseline and made the two series directly comparable.

The lesson generalises: when comparing two measurements of "the same" quantity, confirm
they cover the same population before trusting a similarity metric.

## Evidence

Sample of the 12-hour run from 2026-09-14 (full output in the script):

| dump label | dump `en` | AQS hour | AQS user/desktop | equal? |
|---|---|---|---|---|
| 08:00 | 2,689,847 | 07 | 2,689,847 | yes |
| 12:00 | 3,283,165 | 11 | 3,283,165 | yes |
| 15:00 | 4,379,219 | 14 | 4,379,219 | yes |
| 19:00 | 4,320,857 | 18 | 4,320,857 | yes |

Exact-match counts across all 12 hours:

| Hypothesis | Exact matches |
|---|---|
| label = START (`15:00` covers 15:00–15:59) | **0 / 12** |
| label = END (`15:00` covers 14:00–14:59) | **12 / 12** |
| label = START+1 (sanity check) | **0 / 12** |

## Consequences

1. **The URL builder shifts forward by one hour.** For an Airflow data interval starting
   at `H`, the file to fetch is labelled `H + 1 hour`. This is implemented once, in
   `wikiflow.source.dump_url_for`, and is exercised by explicit unit tests — including
   ones that would fail if the shift were removed.

2. **The last hour of a day lives in the next day's directory.** The interval
   `2026-09-15 23:00–23:59` is served by `pageviews-20260916-000000.gz`, under
   `.../2026/2026-09/`. Month and year rollovers are unit-tested for the same reason.

3. **Effective publication lag is larger than it looks.** Measuring from the end of the
   period the data actually covers, the observed lag is 2h21m–3h46m rather than
   1h21m–2h46m. The availability sensor's 5h timeout is sized against the larger figure.

4. **A second exact reconciliation source exists.** Dump `en` equals AQS
   `(en.wikipedia.org, desktop, user)` to the view. Alongside the `projectviews`
   cross-check, this gives an independent external oracle for data quality tests, and it
   establishes that a bare project code in these dumps is **user traffic, desktop only**.
