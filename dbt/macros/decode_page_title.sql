{#
  Wikimedia page titles use underscores for spaces, and a minority are percent-encoded
  (`%27` for an apostrophe, and so on). This produces a human-readable form for display.

  The undecoded value is kept alongside it in the models, because that is the exact
  string that joins back to the source file.
#}
{% macro decode_page_title(column_name) %}
    replace({{ url_decode(column_name) }}, '_', ' ')
{% endmacro %}
