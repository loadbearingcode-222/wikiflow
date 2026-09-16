/*
    The reconciliation test.

    Wikimedia publishes its own per-project hourly totals alongside the per-page dump.
    Our wide grain is computed independently, by summing millions of per-page rows. If
    the two agree, the whole extraction path is correct in one assertion: the file
    downloaded intact, every line parsed, nothing was dropped, the domain codes were
    grouped correctly, and the hour was attributed to the right partition.

    This asserts EXACT equality rather than a tolerance. That is not optimism - it was
    measured before the test was written: all 1,955 domain codes matched to the view,
    with identical totals (19,322,047). A tolerance would hide exactly the bugs worth
    catching.

    Scope is limited to hours present on BOTH sides. Raw projectviews files are deleted
    after 7 days (ADR 0003) while bronze is kept indefinitely, so older hours have no
    oracle left to compare against. That is expected, not a failure - and restricting
    up front is also what keeps the join honest, since a missing oracle would otherwise
    look like a mismatch.
*/

with theirs as (

    select
        cast(dt as date)      as view_date,
        cast(hour as integer) as view_hour,
        domain_code,
        count_views           as views
    from {{ source('raw', 'projectviews') }}

),

comparable_hours as (

    select distinct view_date, view_hour from theirs

),

ours as (

    select
        o.view_date,
        o.view_hour,
        o.domain_code,
        o.views
    from {{ ref('stg_domain_totals') }} o
    -- Inner join rather than a row-constructor IN: DuckDB will not compare
    -- (date, int) tuples against a subquery without an explicit cast, and a plain
    -- join says the same thing more clearly.
    join comparable_hours c
      on  o.view_date = c.view_date
      and o.view_hour = c.view_hour

),

compared as (

    select
        coalesce(o.view_date, t.view_date)     as view_date,
        coalesce(o.view_hour, t.view_hour)     as view_hour,
        coalesce(o.domain_code, t.domain_code) as domain_code,
        o.views                                as our_views,
        t.views                                as their_views
    from ours o
    full outer join theirs t
      on  o.view_date   = t.view_date
      and o.view_hour   = t.view_hour
      and o.domain_code = t.domain_code

)

-- `is distinct from` rather than `!=` so a domain present on only one side - a NULL on
-- the other - is reported rather than silently comparing as unknown and passing.
select *
from compared
where our_views is distinct from their_views
