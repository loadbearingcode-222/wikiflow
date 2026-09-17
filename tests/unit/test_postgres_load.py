"""The incremental rule for loading bronze into Postgres, and the parity comparison."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from wikiflow.parity import compare_table
from wikiflow.postgres_load import DATASETS, Partition, partition_of, plan_sync

CPV = "bronze.company_pageviews"
KEY_12 = "bronze/company_pageviews/dt=2026-09-14/hour=12/part-0.parquet"
KEY_13 = "bronze/company_pageviews/dt=2026-09-14/hour=13/part-0.parquet"
P12 = Partition(CPV, date(2026, 9, 14), 12)
P13 = Partition(CPV, date(2026, 9, 14), 13)


@pytest.mark.unit
def test_partition_is_parsed_from_the_key() -> None:
    assert partition_of(CPV, KEY_12) == P12
    raw = "projectviews/dt=2026-09-15/hour=7/projectviews-20260915-080000"
    assert partition_of("raw.projectviews", raw) == Partition(
        "raw.projectviews", date(2026, 9, 15), 7
    )
    assert partition_of(CPV, "bronze/company_pageviews/_SUCCESS") is None


@pytest.mark.unit
def test_first_sync_loads_everything() -> None:
    plan = plan_sync([(CPV, KEY_12, "a"), (CPV, KEY_13, "b")], manifest={})
    assert list(plan.to_load) == [P12, P13]
    assert plan.unchanged == 0


@pytest.mark.unit
def test_unchanged_partitions_are_skipped() -> None:
    plan = plan_sync([(CPV, KEY_12, "a"), (CPV, KEY_13, "b")], manifest={P12: "a", P13: "b"})
    assert plan.to_load == {}
    assert plan.unchanged == 2


@pytest.mark.unit
def test_a_rewritten_partition_is_reloaded_even_though_it_is_not_newer() -> None:
    """The case a high-watermark gets wrong: a reprocess REPLACES an old hour.

    "Load everything after the newest loaded hour" would never look at hour 12 again.
    """
    plan = plan_sync(
        [(CPV, KEY_12, "rewritten"), (CPV, KEY_13, "b")], manifest={P12: "a", P13: "b"}
    )
    assert plan.to_load == {P12: (KEY_12, "rewritten")}


@pytest.mark.unit
def test_partitions_gone_from_storage_are_reported_not_deleted() -> None:
    plan = plan_sync([(CPV, KEY_13, "b")], manifest={P12: "a", P13: "b"})
    assert plan.missing_from_storage == [P12]
    assert plan.to_load == {}


@pytest.mark.unit
def test_two_objects_in_one_partition_is_refused() -> None:
    other = KEY_12.replace("part-0", "part-1")
    with pytest.raises(ValueError, match="more than one object"):
        plan_sync([(CPV, KEY_12, "a"), (CPV, other, "c")], manifest={})


@pytest.mark.unit
def test_loaded_columns_include_the_redirect_fields() -> None:
    columns = {d.name: [c for c, _ in d.columns] for d in DATASETS}
    assert {"match_type", "canonical_title"} <= set(columns[CPV])
    for name, cols in columns.items():
        assert cols[:2] == ["dt", "hour"], name


@pytest.fixture
def two_warehouses() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    for schema in ("a", "b"):
        connection.execute(f"CREATE SCHEMA {schema}")
        connection.execute(f"CREATE TABLE {schema}.t (k VARCHAR, share DOUBLE)")
    return connection


@pytest.mark.unit
def test_parity_accepts_identical_tables_in_any_column_order(
    two_warehouses: duckdb.DuckDBPyConnection,
) -> None:
    c = two_warehouses
    c.execute("INSERT INTO a.t VALUES ('x', 1.0 / 3), ('y', 0.5)")
    c.execute("DROP TABLE b.t")
    c.execute("CREATE TABLE b.t (share DOUBLE, k VARCHAR)")
    c.execute("INSERT INTO b.t VALUES (0.5, 'y'), (1.0 / 3, 'x')")

    assert compare_table(c, "a.t", "b.t", "t").identical


@pytest.mark.unit
def test_parity_counts_duplicates_and_float_differences(
    two_warehouses: duckdb.DuckDBPyConnection,
) -> None:
    c = two_warehouses
    c.execute("INSERT INTO a.t VALUES ('x', 0.1), ('x', 0.1), ('y', 0.3)")
    c.execute("INSERT INTO b.t VALUES ('x', 0.1), ('y', 0.30000000000000004)")

    result = compare_table(c, "a.t", "b.t", "t")
    assert not result.identical
    assert (result.only_left, result.only_right) == (2, 1)


@pytest.mark.unit
def test_every_declared_type_has_a_duckdb_cast() -> None:
    from wikiflow.postgres_load import _DUCKDB_TYPES

    for dataset in DATASETS:
        for name, pg_type in dataset.columns:
            assert pg_type.removesuffix(" not null") in _DUCKDB_TYPES, (dataset.name, name)
