{% macro redshift__get_delete_sql(target, source, unique_key) -%}
  {% call statement('delete_' ~ this.alias, fetch_result=False) %}
    {{ log("▶ running custom Redshift DELETE (no alias)", info=true) }}
    {# build fully‐qualified name without alias #}
    {% set parts = [] %}
    {% if target.database %}  {% do parts.append(adapter.quote(target.database)) %}  {% endif %}
    {% if target.schema   %}  {% do parts.append(adapter.quote(target.schema))   %}  {% endif %}
    {% do parts.append(adapter.quote(target.identifier)) %}
    {% set raw_name = parts | join('.')  %}
    DELETE FROM {{ raw_name }}
    {%- if unique_key %}
    WHERE ({{ unique_key }}) IN (
      SELECT {{ unique_key }}
      FROM {{ source }}
    )
    {%- endif %}
  {% endcall %}
{%- endmacro %}
