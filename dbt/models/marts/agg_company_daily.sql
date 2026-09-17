-- Daily rollup per company, with the desktop/mobile split.

with hourly as (

    select * from {{ ref('fct_company_pageviews_hourly') }}

)

select
    view_date,
    company_qid,
    max(company_name)                                                as company_name,
    sum(views)                                                       as views,
    sum(case when access_method = 'mobile-web' then views else 0 end) as mobile_views,
    sum(case when access_method = 'desktop'    then views else 0 end) as desktop_views,
    sum(case when match_type = 'redirect'      then views else 0 end) as redirect_views,
    {{ as_double("sum(case when access_method = 'mobile-web' then views else 0 end)") }}
        / nullif(sum(views), 0)                                      as mobile_share,
    count(distinct wiki_language)                                    as language_editions,
    count(distinct view_hour)                                        as hours_present

from hourly
group by view_date, company_qid
