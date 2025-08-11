{%- set archive_date = (run_started_at - modules.datetime.timedelta(days=63)) -%}
{%- set table_alias = 'england_impact_scores_' ~ archive_date.strftime('%m_%Y') -%}

{{ config(
    materialized='table',
    schema='archive',
    alias=table_alias
) }}

SELECT *
FROM {{ source('street_manager', 'impact_scores_england_latest') }}