{% macro get_tables_24() %}
    {% set query %}
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw_data_2024'
        AND table_name SIMILAR TO '\d{2}_2024'
    {% endset %}

    {% set results = run_query(query) %}
    {% set table_list = [] %}

    {% if execute %}
        {% for row in results %}
            {% do table_list.append("raw_data_2024" ~ '."' ~ row.table_name ~ '"') %}
        {% endfor %}
    {% endif %}

    {{ return(table_list) }}
{% endmacro %}
