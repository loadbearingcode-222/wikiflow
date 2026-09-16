-- Company attention per hour as a share of ALL Wikimedia traffic.
--
-- This is the model the two-grain design exists for. Without the wide grain you could
-- report "Apple got 12,000 views" but not whether that was a spike or a quiet Tuesday.

with company_hourly as (

    select
        view_date,
        view_hour,
        company_qid,
        max(company_name)      as company_name,
        max(interval_start_utc) as interval_start_utc,
        sum(views)             as company_views
    from {{ ref('fct_company_pageviews_hourly') }}
    group by view_date, view_hour, company_qid

),

traffic_hourly as (

    -- Every domain, including codes that failed to parse: they are real traffic, and
    -- excluding them would inflate every share figure.
    select
        view_date,
        view_hour,
        sum(views) as total_views
    from {{ ref('stg_domain_totals') }}
    group by view_date, view_hour

)

select
    c.view_date,
    c.view_hour,
    c.interval_start_utc,
    c.company_qid,
    c.company_name,
    c.company_views,
    t.total_views,
    c.company_views::double / nullif(t.total_views, 0) as share_of_traffic

from company_hourly c
join traffic_hourly t
  on c.view_date = t.view_date
 and c.view_hour = t.view_hour
