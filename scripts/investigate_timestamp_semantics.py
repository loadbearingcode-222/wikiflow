"""Determine empirically what the hour in a Wikimedia dump filename refers to.

Why this exists
---------------
Wikitech states that pageviews dump filenames "refer to the end of the aggregation
period, not the beginning". Observed publication times are consistent with either
reading, and the mapping from an Airflow data interval to a URL is the single most
load-bearing line of code in this pipeline. Getting it wrong shifts every row by an
hour - silently, and in a way no schema test would catch.

So: measure it, do not trust it.

Method
------
The AQS REST API reports hourly pageviews against unambiguous wall-clock hours. If the
dump files and AQS are derived from the same upstream aggregation, their counts should
match *exactly* at the correct alignment, not merely correlate.

1. Download `projectviews-YYYYMMDD-HH0000` for a run of consecutive hours (~26 KB each).
   Take the `en` row: English Wikipedia, DESKTOP only, since mobile is a separate `en.m`.
2. Query AQS for the same window with project=en.wikipedia.org, access=desktop,
   agent=user.
3. For each candidate lag, count how many hours match to the exact view.

An exact-match count is a far stronger test than correlation: ratio stability was tried
first and was inconclusive (13.3% vs 14.8% coefficient of variation), because the AQS
all-agents series carries a large flat automated baseline that damps the diurnal swing.
Restricting to agent=user removes it and the alignment becomes unambiguous.

Run:  uv run python scripts/investigate_timestamp_semantics.py
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

USER_AGENT = "wikiflow/0.1 (portfolio project; contact github.com/loadbearingcode-222)"

# A settled day in the past, so every file is certainly published.
START = datetime(2026, 9, 14, 8, 0, tzinfo=UTC)
N_HOURS = 12

DUMPS = "https://dumps.wikimedia.org/other/pageviews"
AQS = "https://wikimedia.org/api/rest_v1/metrics/pageviews/aggregate"


@dataclass(frozen=True)
class Alignment:
    """One hypothesis about what the filename hour means."""

    lag_hours: int
    name: str
    description: str


HYPOTHESES = [
    Alignment(0, "label = START", "pageviews-...-15:00 covers 15:00-15:59"),
    Alignment(-1, "label = END", "pageviews-...-15:00 covers 14:00-14:59"),
    Alignment(+1, "label = START+1", "sanity check; expected to fail"),
]


def projectviews_url(ts: datetime) -> str:
    return f"{DUMPS}/{ts:%Y}/{ts:%Y-%m}/projectviews-{ts:%Y%m%d}-{ts:%H}0000"


def fetch_dump_en_views(client: httpx.Client, ts: datetime) -> int:
    """Return the `en` (English Wikipedia desktop) total for the labelled hour."""
    response = client.get(projectviews_url(ts))
    response.raise_for_status()

    for line in response.text.splitlines():
        # Format: `domain_code page_title count_views total_response_size`, where
        # projectviews uses a literal "-" in the page_title slot.
        parts = line.split(" ")
        if len(parts) == 4 and parts[0] == "en":
            return int(parts[2])
    raise ValueError(f"no 'en' row found in {projectviews_url(ts)}")


def fetch_aqs_hourly(client: httpx.Client, start: datetime, end: datetime) -> dict[str, int]:
    """Return {YYYYMMDDHH: views} for en.wikipedia desktop/user, inclusive of both ends."""
    url = f"{AQS}/en.wikipedia.org/desktop/user/hourly/{start:%Y%m%d%H}/{end:%Y%m%d%H}"
    response = client.get(url)
    response.raise_for_status()
    return {item["timestamp"][:10]: int(item["views"]) for item in response.json()["items"]}


def main() -> int:
    hours = [START + timedelta(hours=i) for i in range(N_HOURS)]

    with httpx.Client(
        headers={"User-Agent": USER_AGENT}, timeout=30.0, follow_redirects=True
    ) as client:
        print(f"Fetching {N_HOURS} projectviews files from dumps.wikimedia.org...")
        dump_views: dict[datetime, int] = {}
        for ts in hours:
            dump_views[ts] = fetch_dump_en_views(client, ts)
            print(f"  label {ts:%Y-%m-%d %H:00}  en = {dump_views[ts]:>10,}")
            time.sleep(0.3)  # be polite to a free public service

        print("\nFetching the same window from AQS (access=desktop, agent=user)...")
        aqs = fetch_aqs_hourly(
            client, hours[0] - timedelta(hours=2), hours[-1] + timedelta(hours=2)
        )
        for key in sorted(aqs):
            print(f"  hour  {key}      views = {aqs[key]:>10,}")

    print("\n" + "=" * 74)
    print("Exact-match count per hypothesis")
    print("=" * 74)

    results: list[tuple[int, int, Alignment]] = []
    for hypothesis in HYPOTHESES:
        exact = 0
        comparable = 0
        for ts in hours:
            aqs_key = f"{ts + timedelta(hours=hypothesis.lag_hours):%Y%m%d%H}"
            if aqs_key not in aqs:
                continue
            comparable += 1
            if dump_views[ts] == aqs[aqs_key]:
                exact += 1

        results.append((exact, comparable, hypothesis))
        share = exact / comparable if comparable else 0.0
        print(f"\n{hypothesis.name}  ({hypothesis.description})")
        print(f"    exact matches  {exact}/{comparable}  ({share:.0%})")

    results.sort(key=lambda r: -r[0])
    best_exact, best_comparable, best = results[0]

    print("\n" + "=" * 74)
    if best_exact == best_comparable and best_exact > 0:
        print(f"CONCLUSION: {best.name}")
        print(f"            {best.description}")
        print(f"            {best_exact}/{best_comparable} hours match to the exact view.")
        print("            Counts are identical, not merely correlated.")
        print("=" * 74)
        return 0

    print("INCONCLUSIVE: no hypothesis matched every comparable hour exactly.")
    print("=" * 74)
    return 1


if __name__ == "__main__":
    sys.exit(main())
