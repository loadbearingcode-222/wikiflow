# Test fixtures

Committed so the test suite never touches the network. CI runs offline.

## `projectviews-20260915-120000`

A real, unmodified `projectviews` file downloaded from
`dumps.wikimedia.org/other/pageviews/2026/2026-09/` on 2026-09-16.

26 KB, 1,946 rows, one per project. Per ADR 0001 the label is the END of the
period, so this file covers **11:00-11:59 UTC on 2026-09-15**.

It is kept because it contains the full real domain-code inventory, including
the awkward cases that a hand-written list of examples would miss:

- `.m` meaning **mobile** (`en.m`, `en.m.d`) - 383 rows
- `.m` meaning **wikimedia.org** (`commons.m`, `meta.m`) - the same token, different meaning
- both at once: `commons.m.m` - 27 rows
- prefix-less wikis where mobile comes FIRST: `m.wd`, `m.w`, `m.s`
- a literal `""` domain code with 5,383 views
- three-letter and hyphenated language codes (`bat-smg`, `zh-yue`)

Licence: Wikimedia pageviews data is released under CC0.

## `pageviews-sample-20260914-120000.gz`

A trimmed slice of the real hourly dump for 2026-09-14 11:00 UTC (label 12:00).
62 KB, 4,379 lines: **all 379 genuine tracked-company rows** from that hour, plus
4,000 randomly sampled filler rows, re-sorted to preserve the source file's
`(domain_code, page_title)` ordering.

Kept small enough to commit while still being real data with real Unicode titles,
real domain codes and real view counts. Expected extraction results:

| | |
|---|---|
| lines | 4,379 |
| company rows | 379 |
| company views | 5,138 |
| distinct domain codes | 320 |
| total views | 15,255 |

The company-view total (5,138) is identical to what the full 7.4M-row file produces,
because every company row was kept.

## `company_pages.csv`

A snapshot of the Wikidata sitelink resolution for the seven seed entities: 2,238
`(domain_code, page_title)` pairs. Committed so integration tests do not call the
Wikidata API. Refresh it by re-running the resolver when `seeds/companies.csv`
changes.
