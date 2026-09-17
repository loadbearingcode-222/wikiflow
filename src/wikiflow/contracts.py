"""The data contract for bronze: what every published row must look like.

Bronze is the boundary between this pipeline and everything downstream of it - two
warehouses, the dashboard, and anyone who reads the Parquet directly. Until this module,
its shape was implied by a SQL `SELECT` in extract.py and restated by hand in the
Postgres loader. Twice, that implied shape was wrong without anything noticing:

* the first two `domain_totals` partitions stored `views` as DOUBLE (`3882.0`), which
  DuckDB silently unified and only Postgres eventually rejected (ADR 0008);
* adding `match_type` and `canonical_title` meant editing extraction, the loader's column
  list and the dbt source separately, with nothing checking they agreed.

So the contract is declared once, here, and enforced at the point of publication: every
row of a partition is validated **before** it is written to bronze. Validation is
**strict** - an integer column holding `3882.0` fails, rather than being coerced into
passing. The Postgres loader derives its tables from the same models, so the two cannot
drift apart.

Validation is per row because the partitions are small: a few hundred company rows and
about two thousand domain rows an hour, validated in milliseconds. The ~7 million raw
rows are never validated this way - they are checked in aggregate, by the malformed-line
gate and by reconciliation against Wikimedia's totals.
"""

from __future__ import annotations

import types
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal, Union, get_args, get_origin

import duckdb
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NaiveDatetime,
    TypeAdapter,
    ValidationError,
    model_validator,
)

AccessMethod = Literal["desktop", "mobile-web"]
MatchType = Literal["sitelink", "redirect"]

NonNegative = Annotated[int, Field(ge=0)]
Positive = Annotated[int, Field(ge=1)]
NonEmpty = Annotated[str, Field(min_length=1)]


class _BronzeRecord(BaseModel):
    """Partition keys and provenance, carried by every bronze row."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    dt: date
    hour: Annotated[int, Field(ge=0, le=23)]
    interval_start_utc: NaiveDatetime
    """UTC, stored without a zone - the hour the data DESCRIBES (ADR 0001)."""
    source_url: NonEmpty
    ingested_at: AwareDatetime
    """When this partition was written. The marts use it to find rewritten hours."""

    @model_validator(mode="after")
    def _interval_matches_partition(self) -> _BronzeRecord:
        # A row filed under one hour but describing another would be counted in the
        # wrong place by every downstream consumer, and reconcile nowhere.
        if (self.interval_start_utc.date(), self.interval_start_utc.hour) != (self.dt, self.hour):
            raise ValueError(
                f"interval_start_utc {self.interval_start_utc} is not in partition "
                f"dt={self.dt}/hour={self.hour}"
            )
        return self


class CompanyPageviewRecord(_BronzeRecord):
    """Narrow grain: one row per tracked article title, per hour, per access method."""

    qid: Annotated[str, Field(pattern=r"^Q[1-9]\d*$")]
    company_name: NonEmpty
    language: str | None
    project: NonEmpty
    access_method: AccessMethod
    match_type: MatchType
    canonical_title: NonEmpty
    domain_code: NonEmpty
    page_title: NonEmpty
    count_views: Positive
    """A dump row exists only because something was viewed, so zero is impossible."""
    total_response_size: NonNegative


class DomainTotalRecord(_BronzeRecord):
    """Wide grain: total views per domain code per hour."""

    domain_code: str
    """Can be `""`-like garbage from upstream; kept, because Wikimedia counts it."""
    project: str | None
    language: str | None
    access_method: AccessMethod | None
    is_parseable: bool
    views: Positive
    response_bytes: NonNegative
    distinct_pages: Positive

    @model_validator(mode="after")
    def _parsed_iff_parseable(self) -> DomainTotalRecord:
        if self.is_parseable and (self.project is None or self.access_method is None):
            raise ValueError(f"{self.domain_code!r} is parseable but has no project/access")
        return self


BRONZE_CONTRACTS: dict[str, type[_BronzeRecord]] = {
    "company_pageviews": CompanyPageviewRecord,
    "domain_totals": DomainTotalRecord,
}


class ContractViolation(ValueError):
    """A partition does not satisfy its contract and must not be published."""


def columns(model: type[BaseModel]) -> tuple[str, ...]:
    """Column names in contract order - the order bronze files are written in."""
    return tuple(model.model_fields)


def validate_partition(path: Path, model: type[_BronzeRecord], *, max_errors: int = 5) -> int:
    """Validate every row of a local Parquet file. Returns the number of rows.

    Checks the column set and order first: a missing or extra column is a contract
    change, and is reported as one instead of as thousands of row errors.
    """
    connection = duckdb.connect()
    try:
        result = connection.execute("SELECT * FROM read_parquet(?)", [path.as_posix()])
        found = tuple(d[0] for d in result.description)
        rows = result.fetchall()
    finally:
        connection.close()

    expected = columns(model)
    if found != expected:
        detail = (
            f"  missing:    {sorted(set(expected) - set(found))}\n"
            f"  unexpected: {sorted(set(found) - set(expected))}"
            if set(found) != set(expected)
            else f"  same columns, different order: {list(found)}"
        )
        raise ContractViolation(f"{path.name}: columns do not match {model.__name__}.\n{detail}")

    adapter: TypeAdapter[list[Any]] = TypeAdapter(list[model])  # type: ignore[valid-type]
    try:
        adapter.validate_python([dict(zip(expected, row, strict=True)) for row in rows])
    except ValidationError as exc:
        shown = "\n  ".join(
            f"row {error['loc'][0]}, {'.'.join(map(str, error['loc'][1:])) or 'row'}: "
            f"{error['msg']} (got {error.get('input')!r})"
            for error in exc.errors()[:max_errors]
        )
        raise ContractViolation(
            f"{path.name}: {exc.error_count()} contract violation(s) in {len(rows):,} rows "
            f"against {model.__name__}; refusing to publish.\n  {shown}"
        ) from None
    return len(rows)


# --- types for the warehouses, derived from the contract -------------------------------

_POSTGRES_TYPES: dict[Any, str] = {
    date: "date",
    int: "bigint",
    str: "text",
    bool: "boolean",
}


def postgres_columns(model: type[BaseModel]) -> tuple[tuple[str, str], ...]:
    """(name, Postgres type) for each field, so the loader's tables follow the contract."""
    out = []
    for name, field in model.model_fields.items():
        annotation = field.annotation
        if get_origin(annotation) in (Union, types.UnionType):
            annotation = next(a for a in get_args(annotation) if a is not type(None))
        if get_origin(annotation) is Annotated:
            annotation = get_args(annotation)[0]
        if get_origin(annotation) is Literal:
            annotation = type(get_args(annotation)[0])

        if name == "interval_start_utc":
            kind = "timestamp"
        elif name == "ingested_at":
            kind = "timestamptz"
        else:
            kind = _POSTGRES_TYPES[annotation]
        # Only the partition keys are NOT NULL in the database: every delete and every
        # manifest row depends on them. Nullability of the rest is enforced by the
        # contract before publication, in one place rather than two.
        if name in ("dt", "hour"):
            kind += " not null"
        out.append((name, kind))
    return tuple(out)
