{{ config(
    materialized='table',
    post_hook=[
        "DROP TABLE IF EXISTS {{ this.schema }}.stg_emergency_works_by_authority",
        "DROP TABLE IF EXISTS {{ this.schema }}.stg_major_works_by_authority",
        "DROP TABLE IF EXISTS {{ this.schema }}.int_postcodes",
        "DROP TABLE IF EXISTS {{ this.schema }}.int_emergency_works_postcodes_by_authority",
        "DROP TABLE IF EXISTS {{ this.schema }}.int_major_works_postcodes_by_authority",
    ]
) }}

-- This is a cleanup model that runs after wellbeing impact tables are created
-- It creates a simple confirmation table and drops the intermediate tables
SELECT
    'Cleanup completed at ' || CURRENT_TIMESTAMP as cleanup_status,
    'Intermediate tables dropped' as message,
    (SELECT COUNT(*) FROM {{ ref('emergency_wellbeing') }}) as emergency_wellbeing_records,
    (SELECT COUNT(*) FROM {{ ref('major_wellbeing') }}) as major_wellbeing_records