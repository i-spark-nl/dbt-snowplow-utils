{% call statement('delete_' ~ this.alias, fetch_result=False) %}
  {{ log("▶ running custom DELETE without alias", info=true) }}
  {#–– manually build the fully qualified name ––#}
  {% set parts = [] %}
  {% if this.database %}  {% do parts.append(adapter.quote(this.database)) %}  {% endif %}
  {% if this.schema   %}  {% do parts.append(adapter.quote(this.schema))   %}  {% endif %}
  {% do parts.append(adapter.quote(this.identifier)) %}
  {% set raw_name = parts | join('.') %}
  DELETE FROM {{ raw_name }}
  {%- if unique_key %}
  WHERE ({{ unique_key }}) IN (
    SELECT {{ unique_key }}
    FROM {{ tmp_relation }}
  )
  {%- endif %}
{% endcall %}
