{%- set archive_date = modules.datetime.datetime.strptime(var('year') + var('month'), '%Y%m') -%}
{%- set table_alias = 'england_impact_scores_' ~ archive_date.strftime('%m_%Y') -%}

{{ config(
    materialized='table',
    schema='archive',
    alias=table_alias
) }}

SELECT *
FROM {{ source('street_manager', 'impact_scores_england_latest') }}