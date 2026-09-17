{#
  The few places DuckDB and Postgres SQL differ, dispatched per adapter so every model
  is written once and builds on both (ADR 0008).

  Kept deliberately small. Each macro here exists because a model needed it, and each
  was checked by building the marts on both warehouses and comparing them row by row
  (wikiflow.parity) - not by reading documentation.
#}


{# ------------------------------------------------------------------ url_decode
   DuckDB has url_decode built in. Postgres has nothing equivalent, so the function is
   created before the run. Both behave the same, which was checked input by input
   against DuckDB rather than assumed: only valid %XX sequences are decoded, `+` is left
   alone, and a malformed sequence like `%zz` is kept literally.

   One case needed a change on the DuckDB side too. DuckDB's url_decode RAISES when the
   decoded bytes are not valid UTF-8 (`%E3` alone), so a single oddly encoded title would
   have failed the entire build. Both versions now return the title unchanged instead:
   it is a display column, and the raw title is kept beside it as the join key.
#}
{% macro url_decode(expression) %}
    {{ return(adapter.dispatch('url_decode')(expression)) }}
{% endmacro %}

{% macro default__url_decode(expression) %}
    coalesce(try(url_decode({{ expression }})), {{ expression }})
{% endmacro %}

{% macro postgres__url_decode(expression) %}
    {{ target.schema }}_udf.url_decode({{ expression }})
{% endmacro %}


{% macro create_warehouse_functions() %}
    {{ return(adapter.dispatch('create_warehouse_functions')()) }}
{% endmacro %}

{% macro default__create_warehouse_functions() %}
    select 1
{% endmacro %}

{% macro postgres__create_warehouse_functions() %}
    create schema if not exists {{ target.schema }}_udf;

    create or replace function {{ target.schema }}_udf.url_decode(input text)
    returns text
    language plpgsql
    immutable
    strict
    as $$
    declare
        decoded bytea := '';
        token   text;
    begin
        if position('%' in input) = 0 then
            return input;
        end if;
        for token in
            select (regexp_matches(input, '%[0-9A-Fa-f]{2}|[^%]+|%', 'g'))[1]
        loop
            if length(token) = 3 and left(token, 1) = '%' then
                decoded := decoded || decode(substr(token, 2, 2), 'hex');
            else
                decoded := decoded || convert_to(token, 'UTF8');
            end if;
        end loop;
        return convert_from(decoded, 'UTF8');
    exception
        -- Decoded bytes that are not valid UTF-8: keep the title as it arrived.
        when character_not_in_repertoire or untranslatable_character then
            return input;
    end
    $$;
{% endmacro %}


{# ------------------------------------------------------------------ double
   `::double` is DuckDB-only syntax. `double precision` is standard SQL, and both
   warehouses read it as an 8-byte float - unlike `float`, which DuckDB takes as 4-byte.
#}
{% macro as_double(expression) %}
    cast({{ expression }} as double precision)
{% endmacro %}
