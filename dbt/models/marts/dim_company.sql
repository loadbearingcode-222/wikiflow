-- One row per tracked entity.
--
-- Meta Platforms (Q380) and Facebook (Q355) are separate entities on purpose: one is
-- the company, the other the service, and they draw very different traffic.

with pageviews as (

    select * from {{ ref('stg_company_pageviews') }}

)

select
    company_qid,
    min(company_name)              as company_name,
    count(distinct wiki_language)  as language_editions,
    count(distinct page_title)     as distinct_titles,
    count(distinct domain_code)    as distinct_domains,
    min(interval_start_utc)        as first_seen_utc,
    max(interval_start_utc)        as last_seen_utc,
    sum(views)                     as lifetime_views

from pageviews
group by company_qid
