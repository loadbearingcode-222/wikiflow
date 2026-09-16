"""Benchmark the two-grain extraction: DuckDB (what the pipeline uses) vs pandas.

    uv run --group bench python scripts/benchmark_extraction.py [--hour 2026-09-14T12]

Methodology, because a benchmark is only as good as its fairness:

* **Same input, same output.** Both methods read the same real hourly dump from object
  storage and produce the same two grains: per-domain totals, and rows matching the
  2,238-pair company filter. Their outputs are compared, and the run fails if they
  differ - a fast wrong answer is not a result.
* **Isolated processes.** Each method runs in its own subprocess, repeated, so neither
  inherits the other's warm caches or allocated memory.
* **Memory measured from outside.** Peak resident memory is sampled from the parent via
  psutil. Python-level tools like tracemalloc cannot see allocations DuckDB makes in
  C++, and would flatter it.
* **Download excluded.** The file is fetched once, beforehand. Network time dominates
  ingestion but is identical for both methods, so including it would only hide the
  difference being measured.

Pandas is given its best reasonable shot: typed columns declared up front, the C parser,
QUOTE_NONE (page titles legally contain quote characters), and the category dtype for
the join key.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
COMPANY_PAGES = REPO / "tests/fixtures/company_pages.csv"


# --- the two methods, each run in a child process ----------------------------------


def run_duckdb(source: Path, out: Path) -> dict[str, object]:
    from datetime import UTC, datetime

    from wikiflow.extract import extract_hour
    from wikiflow.source import HourlyDump
    from wikiflow.wikidata import CompanyPage

    with COMPANY_PAGES.open(newline="", encoding="utf-8") as handle:
        pages = [
            CompanyPage(**{**row, "language": row["language"] or None})
            for row in csv.DictReader(handle)
        ]
    interval = datetime.strptime(source.name.split("__")[1], "%Y%m%dT%H.gz").replace(tzinfo=UTC)
    result = extract_hour(
        source=str(source),
        dump=HourlyDump.for_interval(interval),
        company_pages=pages,
        output_dir=out,
    )

    import duckdb

    domains = dict(
        duckdb.connect()
        .execute(f"SELECT domain_code, views FROM '{result.domain_path.as_posix()}'")
        .fetchall()
    )
    return {
        "rows_scanned": result.rows_scanned,
        "company_rows": result.company_rows,
        "company_views": result.company_views,
        "total_views": result.total_views,
        "domain_views": domains,
    }


def run_pandas(source: Path, out: Path) -> dict[str, object]:
    import pandas as pd

    frame = pd.read_csv(
        source,
        sep=" ",
        header=None,
        names=["domain_code", "page_title", "count_views", "total_response_size"],
        dtype={
            "domain_code": "category",
            "page_title": "string",
            "count_views": "int64",
            "total_response_size": "int64",
        },
        quoting=csv.QUOTE_NONE,
        engine="c",
        on_bad_lines="skip",
        keep_default_na=False,
        compression="gzip",
    )

    domain_totals = frame.groupby("domain_code", observed=True)["count_views"].sum().astype("int64")

    pages = pd.read_csv(COMPANY_PAGES, dtype="string", keep_default_na=False)
    company = frame.merge(
        pages[["domain_code", "page_title"]].astype({"domain_code": "string"}),
        left_on=[frame["domain_code"].astype("string"), "page_title"],
        right_on=["domain_code", "page_title"],
        how="inner",
    )

    domain_totals.to_frame("views").to_parquet(out / "domain_totals.parquet")
    company.to_parquet(out / "company_pageviews.parquet")

    return {
        "rows_scanned": len(frame),
        "company_rows": len(company),
        "company_views": int(company["count_views"].sum()),
        "total_views": int(domain_totals.sum()),
        "domain_views": {str(k): int(v) for k, v in domain_totals.items()},
    }


def child(method: str, source: str) -> None:
    runner = {"duckdb": run_duckdb, "pandas": run_pandas}[method]
    with tempfile.TemporaryDirectory() as tmp:
        started = time.perf_counter()
        result = runner(Path(source), Path(tmp))
        result["seconds"] = time.perf_counter() - started
    print(json.dumps(result))


# --- the parent: fetch once, run children, sample memory, compare --------------------


def fetch_raw(hour: str, destination: Path) -> Path:
    from datetime import datetime

    import boto3  # noqa: F401  - imported so a missing dependency fails early

    from wikiflow.config import get_settings
    from wikiflow.source import HourlyDump
    from wikiflow.storage import s3_client

    dump = HourlyDump.for_interval(datetime.fromisoformat(hour + ":00+00:00"))
    # The interval is carried in the filename so the child can rebuild HourlyDump. Compact
    # form, not isoformat(): colons are illegal in Windows filenames.
    target = destination / f"pageviews__{dump.interval_start:%Y%m%dT%H}.gz"
    s3_client().download_file(get_settings().raw_bucket, dump.raw_key("pageviews"), str(target))
    return target


def measure(method: str, source: Path) -> tuple[dict[str, object], float]:
    import psutil

    # Output goes to temporary FILES, not pipes. With stdout=PIPE the child blocks once it
    # fills the pipe buffer, while the parent waits for it to exit before reading: a
    # deadlock. The first version of this script hung exactly that way.
    with (
        tempfile.TemporaryFile("w+", encoding="utf-8") as out,
        tempfile.TemporaryFile("w+", encoding="utf-8") as err,
    ):
        process = subprocess.Popen(
            [sys.executable, __file__, "--child", method, str(source)],
            stdout=out,
            stderr=err,
        )
        # On Windows a venv's python.exe is a launcher that runs the real interpreter as
        # a child, so the work - and the memory - is one level down. Find that process
        # once, then sample it directly. Rescanning the process table on every tick cost
        # the parent 127 CPU-seconds in an earlier version, stealing CPU from the very
        # thing being measured.
        launcher = psutil.Process(process.pid)
        tracked: list[psutil.Process] = [launcher]
        peak = 0
        while process.poll() is None:
            if len(tracked) == 1:
                with contextlib.suppress(psutil.Error):
                    tracked.extend(launcher.children(recursive=True))
            rss = 0
            for tracked_process in tracked:
                with contextlib.suppress(psutil.Error):
                    rss += tracked_process.memory_info().rss
            peak = max(peak, rss)
            time.sleep(0.2)

        out.seek(0)
        err.seek(0)
        stdout, stderr = out.read(), err.read()

    if process.returncode != 0:
        raise RuntimeError(f"{method} failed:\n{stderr[-2000:]}")
    return json.loads(stdout.strip().splitlines()[-1]), peak / 1024**2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hour", default="2026-09-14T12", help="UTC hour, e.g. 2026-09-14T12")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--child", nargs=2, metavar=("METHOD", "SOURCE"), help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.child:
        child(*args.child)
        return 0

    with tempfile.TemporaryDirectory() as tmp:
        source = fetch_raw(args.hour, Path(tmp))
        size_mb = source.stat().st_size / 1024**2
        print(f"input: {source.name}  ({size_mb:.1f} MB gzipped)\n")

        results: dict[str, list[tuple[dict[str, object], float]]] = {}
        for method in ("duckdb", "pandas"):
            results[method] = []
            for attempt in range(1, args.repeats + 1):
                outcome, peak_mb = measure(method, source)
                results[method].append((outcome, peak_mb))
                print(
                    f"  {method:7} run {attempt}: {outcome['seconds']:6.2f}s  peak {peak_mb:7.0f} MB"
                )

    duck, pandas_ = results["duckdb"][0][0], results["pandas"][0][0]
    for key in ("rows_scanned", "company_rows", "company_views", "total_views"):
        if duck[key] != pandas_[key]:
            print(f"\nMISMATCH on {key}: duckdb={duck[key]} pandas={pandas_[key]}")
            return 1
    if duck["domain_views"] != pandas_["domain_views"]:
        print("\nMISMATCH: per-domain totals differ between methods")
        return 1

    def med_time(m: str) -> float:
        return statistics.median(float(o["seconds"]) for o, _ in results[m])  # type: ignore[arg-type]

    def med_peak(m: str) -> float:
        return statistics.median(p for _, p in results[m])

    print("\noutputs identical: rows, company rows, company views, total views, every domain")
    print(f"\n{'':8} {'median time':>12} {'median peak RSS':>16}")
    for method in ("duckdb", "pandas"):
        print(f"{method:8} {med_time(method):11.2f}s {med_peak(method):14.0f} MB")
    print(
        f"\nduckdb is {med_time('pandas') / med_time('duckdb'):.1f}x faster "
        f"and uses {med_peak('pandas') / med_peak('duckdb'):.1f}x less peak memory "
        f"on {duck['rows_scanned']:,} rows"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
