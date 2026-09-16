-- The fact table. Grain: one row per (hour, company, domain, article).
--
-- Incremental with delete+insert on the partition key - the SQL expression of the same
-- rule the ingestion layer follows: a re-run REPLACES an hour rather than adding to it.
-- There is no append path anywhere in this pipeline, so re-running cannot double-count.

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['view_date', 'view_hour']
    )
}}

with pageviews as (

    select * from {{ ref('stg_company_pageviews') }}

    {% if is_incremental() %}
    -- Only hours at or after the newest already loaded. Backfilling an OLDER hour is
    -- done with --full-refresh or by targeting that partition, deliberately: silently
    -- reprocessing everything would hide a backfill rather than surface it.
    where view_date >= (select coalesce(max(view_date), DATE '1970-01-01') from {{ this }})
    {% endif %}

)

select
    view_date,
    view_hour,
    interval_start_utc,

    company_qid,
    company_name,
    domain_code,
    wiki_project,
    wiki_language,
    access_method,
    page_title,
    page_title_display,

    views,
    response_bytes

from pageviews
