-- One row per domain_code seen, with the decomposition carried from extraction.
-- Built from the WIDE grain so it covers every wiki, not only those with a tracked
-- company article.

with domains as (

    select * from {{ ref('stg_domain_totals') }}

)

select
    domain_code,
    max(wiki_project)       as wiki_project,
    max(wiki_language)      as wiki_language,
    max(access_method)      as access_method,
    bool_and(is_parseable)  as is_parseable,
    sum(views)              as lifetime_views,
    count(*)                as hours_observed

from domains
group by domain_code
