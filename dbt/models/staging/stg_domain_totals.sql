-- Wide grain: total views per domain_code per hour. The denominator for
-- share-of-traffic, and the key that reconciles against Wikimedia's own projectviews.
--
-- The domain_code has ALREADY been decomposed into (project, language, access_method)
-- upstream in Python, where the parser is tested against all 1,946 codes in a real
-- hourly file. Deliberately not re-implemented here: `.m` means "mobile" in `en.m`,
-- "wikimedia.org" in `commons.m`, and both in `commons.m.m`, and two definitions of
-- that rule would drift.

with source as (

    select * from {{ source('bronze', 'domain_totals') }}

)

select
    dt                  as view_date,
    hour                as view_hour,
    interval_start_utc,

    domain_code,
    project             as wiki_project,
    language            as wiki_language,
    access_method,
    -- False for codes that do not fit the documented scheme. Such rows are RETAINED
    -- (a real `""` code carries thousands of views an hour) because Wikimedia counts
    -- them in projectviews, and dropping them would break reconciliation.
    is_parseable,

    views,
    response_bytes,
    distinct_pages,

    source_url,
    ingested_at

from source
