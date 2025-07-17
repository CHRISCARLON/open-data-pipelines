{{ config(
    materialized='table',
    post_hook=[
        "DROP TABLE IF EXISTS {{ this.schema }}.stg_works_by_authority",
        "DROP TABLE IF EXISTS {{ this.schema }}.int_postcodes", 
        "DROP TABLE IF EXISTS {{ this.schema }}.int_works_postcodes_by_authority",
        "DROP TABLE IF EXISTS {{ this.schema }}.stg_works_by_authority_union",
    ]
) }}

-- This is a cleanup model that runs after emergency_wellbeing_impact
-- It creates a simple confirmation table and drops the intermediate tables
SELECT 
    'Cleanup completed at ' || CURRENT_TIMESTAMP as cleanup_status,
    'Intermediate tables dropped' as message,
    COUNT(*) as final_wellbeing_records
FROM {{ ref('emergency_wellbeing') }}