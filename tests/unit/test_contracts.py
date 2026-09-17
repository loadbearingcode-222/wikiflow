"""The bronze data contract, against small Parquet files written the way extraction does."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from wikiflow.contracts import (
    CompanyPageviewRecord,
    ContractViolation,
    DomainTotalRecord,
    columns,
    postgres_columns,
    validate_partition,
)

PARTITION = """
    DATE '2026-09-14'                     AS dt,
    11                                    AS hour,
    TIMESTAMP '2026-09-14 11:00:00'       AS interval_start_utc,
    's3://wikiflow-raw/pageviews/x.gz'    AS source_url,
    TIMESTAMPTZ '2026-09-17 04:00:00+00'  AS ingested_at
"""


def write(tmp_path: Path, select: str) -> Path:
    path = tmp_path / "part.parquet"
    duckdb.connect().execute(f"COPY (SELECT {select}) TO '{path.as_posix()}' (FORMAT PARQUET)")
    return path


def domain_row(**overrides: str) -> str:
    values = {
        "partition": PARTITION,
        "domain_code": "'en.m'",
        "project": "'wikipedia'",
        "language": "'en'",
        "access_method": "'mobile-web'",
        "is_parseable": "true",
        "views": "3882::BIGINT",
        "response_bytes": "0::BIGINT",
        "distinct_pages": "12::BIGINT",
    } | overrides
    partition = values.pop("partition")
    return partition + ", " + ", ".join(f"{v} AS {k}" for k, v in values.items())


def company_row(**overrides: str) -> str:
    values = {
        "partition": PARTITION,
        "qid": "'Q312'",
        "company_name": "'Apple Inc.'",
        "language": "'en'",
        "project": "'wikipedia'",
        "access_method": "'desktop'",
        "match_type": "'redirect'",
        "canonical_title": "'Apple_Inc.'",
        "domain_code": "'en'",
        "page_title": "'Apple_Computer'",
        "count_views": "40::BIGINT",
        "total_response_size": "0::BIGINT",
    } | overrides
    partition = values.pop("partition")
    return partition + ", " + ", ".join(f"{v} AS {k}" for k, v in values.items())


@pytest.mark.unit
def test_well_formed_partitions_pass(tmp_path: Path) -> None:
    assert validate_partition(write(tmp_path, domain_row()), DomainTotalRecord) == 1
    assert validate_partition(write(tmp_path, company_row()), CompanyPageviewRecord) == 1


@pytest.mark.unit
def test_a_double_where_an_integer_belongs_is_refused(tmp_path: Path) -> None:
    """The real defect: two early partitions stored views as DOUBLE (3882.0).

    Non-strict validation would coerce 3882.0 to 3882 and pass - which is exactly how
    the problem stayed invisible inside DuckDB.
    """
    path = write(tmp_path, domain_row(views="3882.0::DOUBLE"))
    with pytest.raises(ContractViolation, match=r"views: Input should be a valid integer"):
        validate_partition(path, DomainTotalRecord)


@pytest.mark.unit
def test_a_row_filed_under_the_wrong_hour_is_refused(tmp_path: Path) -> None:
    select = domain_row(partition=PARTITION.replace("11  ", "12  "))
    with pytest.raises(ContractViolation, match="not in partition dt=2026-09-14/hour=12"):
        validate_partition(write(tmp_path, select), DomainTotalRecord)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"match_type": "'alias'"}, "match_type"),
        ({"access_method": "'mobile-app'"}, "access_method"),
        ({"count_views": "0::BIGINT"}, "count_views"),
        ({"qid": "'Apple'"}, "qid"),
        ({"canonical_title": "''"}, "canonical_title"),
    ],
)
def test_company_rows_break_the_contract_in_the_ways_that_matter(
    tmp_path: Path, override: dict[str, str], message: str
) -> None:
    with pytest.raises(ContractViolation, match=message):
        validate_partition(write(tmp_path, company_row(**override)), CompanyPageviewRecord)


@pytest.mark.unit
def test_unparseable_domains_may_lack_attributes_but_parseable_ones_may_not(
    tmp_path: Path,
) -> None:
    unparseable = domain_row(
        domain_code="'\"\"'",
        project="NULL::VARCHAR",
        language="NULL::VARCHAR",
        access_method="NULL::VARCHAR",
        is_parseable="false",
    )
    assert validate_partition(write(tmp_path, unparseable), DomainTotalRecord) == 1

    broken = domain_row(project="NULL::VARCHAR")
    with pytest.raises(ContractViolation, match="parseable but has no project"):
        validate_partition(write(tmp_path, broken), DomainTotalRecord)


@pytest.mark.unit
def test_a_missing_column_is_reported_as_a_contract_change(tmp_path: Path) -> None:
    select = company_row().replace(", 'Apple_Inc.' AS canonical_title", "")
    with pytest.raises(ContractViolation, match=r"missing:    \['canonical_title'\]"):
        validate_partition(write(tmp_path, select), CompanyPageviewRecord)


@pytest.mark.unit
def test_contract_order_is_the_order_extraction_writes() -> None:
    for model in (CompanyPageviewRecord, DomainTotalRecord):
        assert columns(model)[:5] == (
            "dt",
            "hour",
            "interval_start_utc",
            "source_url",
            "ingested_at",
        )


@pytest.mark.unit
def test_postgres_tables_are_derived_from_the_contract() -> None:
    derived = dict(postgres_columns(CompanyPageviewRecord))
    assert list(derived) == list(columns(CompanyPageviewRecord))
    assert derived["dt"] == "date not null"
    assert derived["interval_start_utc"] == "timestamp"
    assert derived["ingested_at"] == "timestamptz"
    assert derived["count_views"] == "bigint"
    assert derived["match_type"] == "text"
    assert dict(postgres_columns(DomainTotalRecord))["is_parseable"] == "boolean"
