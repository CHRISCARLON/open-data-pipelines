{% macro get_highway_authorities() %}
  {% set current_schema = 'raw_data_' ~ var('year') %}
  {% set current_table = '"' ~ var('month') ~ '_' ~ var('year') ~ '"' %}
  
  {% set query %}
    SELECT DISTINCT highway_authority
    FROM {{ current_schema }}.{{ current_table }}
  {% endset %}
  
  {% set results = run_query(query) %}
  {% set authority_list = [] %}
  {% if execute %}
    {% for row in results %}
      {% do authority_list.append(row[0]) %}
    {% endfor %}
  {% endif %}
  {{ return(authority_list) }}
{% endmacro %}