-- The fact table. Grain: one row per (hour, company, domain, article).
--
-- Incremental with delete+insert on the partition key - the SQL expression of the same
-- rule the ingestion layer follows: a re-run REPLACES an hour rather than adding to it.
-- There is no append path anywhere in this pipeline, so re-running cannot double-count.
--
-- on_schema_change='fail': the default, 'ignore', silently drops a new upstream column
-- from an existing incremental table. A schema change here needs a --full-refresh, and
-- the build should say so rather than quietly lose the column.

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['view_date', 'view_hour'],
        on_schema_change='fail'
    )
}}

with pageviews as (

    select * from {{ ref('stg_company_pageviews') }}

    {% if is_incremental() %}
    -- Every hour whose bronze partition was WRITTEN after the newest one already here -
    -- not every hour that is newer in time. A reprocess or a recovered partition rewrites
    -- an old hour, and a watermark on view_date never looked at it again: after the
    -- redirect backfill (ADR 0007) both warehouses needed a manual --full-refresh.
    --
    -- The watermark is overlapped by an hour on purpose. Two ingest runs overlap, so an
    -- hour extracted slightly earlier can finish uploading after a build has already
    -- moved past it. Re-selecting a recent hour costs nothing: delete+insert replaces it.
    where ingested_at > coalesce(
        (select max(ingested_at) from {{ this }}),
        cast('1970-01-01 00:00:00+00' as timestamp with time zone)
    ) - interval '1 hour'
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
    -- `redirect`: the reader requested another title and landed on this article.
    match_type,
    canonical_title,

    views,
    response_bytes,

    -- When the bronze partition was written. Drives the incremental rule above.
    ingested_at

from pageviews
