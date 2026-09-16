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
