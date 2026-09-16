-- Narrow grain: one row per tracked company article, per hour, per access method.
--
-- Bronze already carries the company attributes (they came from the Wikidata join at
-- extraction time), so staging only types, renames and decodes.

with source as (

    select * from {{ source('bronze', 'company_pageviews') }}

),

renamed as (

    select
        -- partition keys: the hour the data DESCRIBES, not the filename's hour (ADR 0001)
        dt                                    as view_date,
        hour                                  as view_hour,
        interval_start_utc,

        qid                                   as company_qid,
        company_name,

        language                              as wiki_language,
        project                               as wiki_project,
        access_method,
        domain_code,

        page_title,
        -- Dump titles are underscored and may be percent-encoded. Decode for display;
        -- the raw form is kept because it is the join key back to the source.
        {{ decode_page_title('page_title') }}  as page_title_display,

        count_views                           as views,
        total_response_size                   as response_bytes,

        source_url,
        ingested_at

    from source

)

select * from renamed
