{% set table_alias = 'emergency_works_overview_england_2024' %}
{{ config(materialized='table', alias=table_alias) }}

WITH all_months AS (
    {% set tables = get_tables() %}
    {% for table in tables %}
        {% if not loop.first %}UNION ALL{% endif %}
        
        SELECT 
            permit_table.permit_reference_number,
            permit_table.promoter_organisation,
            permit_table.promoter_swa_code,
            permit_table.highway_authority,
            permit_table.highway_authority_swa_code,
            permit_table.work_category
        FROM {{ table }} AS permit_table
        WHERE permit_table.work_status_ref = 'completed'
        AND permit_table.event_type = 'WORK_STOP'
        AND permit_table.work_category IN ('Immediate - emergency', 'Immediate - urgent')
    {% endfor %}
),

-- Join with geoplace to get sector information
promoter_sector AS (
    SELECT 
        main.highway_authority,
        main.highway_authority_swa_code,
        main.promoter_organisation,
        main.promoter_swa_code,
        main.work_category,
        main.permit_reference_number,
        CASE 
            WHEN geo_place.ofcom_licence IS NOT NULL THEN 'Telecoms'
            WHEN geo_place.ofgem_electricity_licence IS NOT NULL THEN 'Electricity'
            WHEN geo_place.ofgem_gas_licence IS NOT NULL THEN 'Gas'
            WHEN geo_place.ofwat_licence IS NOT NULL THEN 'Water'
            ELSE 'Other'
        END AS sector
    FROM all_months main
    LEFT JOIN geoplace_swa_codes.LATEST_ACTIVE AS geo_place 
        ON CAST(main.promoter_swa_code AS INT) = CAST(geo_place.swa_code AS INT)
)

-- Final aggregation with work category counts
SELECT 
    highway_authority,
    sector,
    work_category,
    COUNT(DISTINCT permit_reference_number) as works_count
FROM promoter_sector
GROUP BY 
    highway_authority,
    sector,
    work_category
ORDER BY 
    highway_authority,
    sector,
    work_category
