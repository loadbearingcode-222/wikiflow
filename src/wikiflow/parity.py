"""Do the two warehouses hold the same marts?

    python -m wikiflow.parity

"The same dbt models build on both" is a claim about SQL compiling. This checks the
claim that matters - that they produce the same DATA - row by row, for every mart.

DuckDB does the comparing: it opens its own warehouse file read-only and attaches
Postgres read-only through its postgres extension, so both sides are queried in one
engine and compared with `EXCEPT ALL` in each direction. `ALL` keeps duplicates, so a
row present twice on one side and once on the other is a difference, not a match.

Every column is compared exactly, floats included. Both warehouses compute the shares
with IEEE-754 doubles from identical integer inputs, so exact equality is the expected
result; a tolerance would only hide a real divergence.

Run it when neither transform DAG is mid-build: the DuckDB file is single-writer, and
comparing one warehouse's new marts with the other's old ones reports differences that
are only timing.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

from wikiflow.config import get_settings

MARTS = (
    "dim_company",
    "dim_wiki",
    "fct_company_pageviews_hourly",
    "agg_company_share_of_traffic",
    "agg_company_daily",
)

DUCKDB_SCHEMA = "main_marts"
POSTGRES_SCHEMA = "public_marts"


@dataclass(frozen=True, slots=True)
class MartParity:
    mart: str
    left_rows: int
    right_rows: int
    only_left: int
    only_right: int
    columns: int

    @property
    def identical(self) -> bool:
        return self.only_left == 0 and self.only_right == 0 and self.left_rows == self.right_rows


def compare_table(
    connection: duckdb.DuckDBPyConnection, left: str, right: str, mart: str
) -> MartParity:
    """Compare two fully qualified tables with the same columns, row by row.

    Columns are taken from the left side and selected BY NAME from both, so a
    difference in column order between warehouses is not a false mismatch. A column
    missing on the right raises, which is the right outcome: the schemas differ.
    """
    columns = [row[0] for row in connection.execute(f"DESCRIBE SELECT * FROM {left}").fetchall()]
    select = ", ".join(f'"{c}"' for c in columns)

    def count(sql: str) -> int:
        result = connection.execute(sql).fetchone()
        assert result is not None
        return int(result[0])

    return MartParity(
        mart=mart,
        left_rows=count(f"SELECT count(*) FROM {left}"),
        right_rows=count(f"SELECT count(*) FROM {right}"),
        only_left=count(
            f"SELECT count(*) FROM (SELECT {select} FROM {left} "
            f"EXCEPT ALL SELECT {select} FROM {right})"
        ),
        only_right=count(
            f"SELECT count(*) FROM (SELECT {select} FROM {right} "
            f"EXCEPT ALL SELECT {select} FROM {left})"
        ),
        columns=len(columns),
    )


def compare_warehouses(duckdb_path: Path, *, pg_conninfo: str | None = None) -> list[MartParity]:
    pg_conninfo = pg_conninfo or get_settings().pg_conninfo
    connection = duckdb.connect()
    try:
        connection.execute("INSTALL postgres; LOAD postgres;")
        path = duckdb_path.as_posix().replace("'", "''")
        conninfo = pg_conninfo.replace("'", "''")
        connection.execute(f"ATTACH '{path}' AS duck (READ_ONLY)")
        connection.execute(f"ATTACH '{conninfo}' AS pg (TYPE postgres, READ_ONLY)")
        return [
            compare_table(
                connection,
                f"duck.{DUCKDB_SCHEMA}.{mart}",
                f"pg.{POSTGRES_SCHEMA}.{mart}",
                mart,
            )
            for mart in MARTS
        ]
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare the DuckDB and Postgres marts")
    parser.add_argument(
        "--duckdb",
        type=Path,
        default=Path("/opt/airflow/warehouse/wikiflow.duckdb"),
        help="DuckDB warehouse file (default: the path inside the Airflow containers)",
    )
    args = parser.parse_args(argv)

    results = compare_warehouses(args.duckdb)
    print(f"{'mart':32} {'duckdb':>10} {'postgres':>10} {'only duck':>10} {'only pg':>8}  result")
    for r in results:
        print(
            f"{r.mart:32} {r.left_rows:>10,} {r.right_rows:>10,} {r.only_left:>10,} "
            f"{r.only_right:>8,}  {'identical' if r.identical else 'DIFFERENT'}"
        )
    identical = all(r.identical for r in results)
    total = sum(r.left_rows for r in results)
    print(
        f"\n{'all marts identical' if identical else 'marts DIFFER'}: "
        f"{len(results)} marts, {total:,} rows compared"
    )
    return 0 if identical else 1


if __name__ == "__main__":
    sys.exit(main())
