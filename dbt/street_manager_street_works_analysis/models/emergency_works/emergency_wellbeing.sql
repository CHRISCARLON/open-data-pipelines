{% set table_alias = 'emergency_wellbeing_impact' %}
{{ config(materialized='table', alias=table_alias) }}

SELECT 
    wpm.permit_reference_number,
    w.promoter_organisation,
    w.highway_authority,
    w.street_name,
    w.activity_type,
    w.work_category,
    w.work_status,
    w.usrn,
    wpm.work_easting,
    wpm.work_northing,
    wpm.actual_start_date_time,
    wpm.actual_end_date_time,
    wpm.duration_days,
    COUNT(DISTINCT wpm.Postcode) as postcode_count,
    SUM(wpm.total_population) as total_population_affected,
    SUM(wpm.female_population) as total_female_population,
    SUM(wpm.male_population) as total_male_population,
    SUM(wpm.total_households) as total_households_affected,
    -- Calculate wellbeing impact: £1.61 × Days × Households_Affected
    ROUND(1.61 * wpm.duration_days * SUM(wpm.total_households), 2) as wellbeing_total_impact
FROM {{ ref('int_works_postcodes_by_authority') }} wpm
JOIN {{ ref('stg_works_by_authority') }} w 
    ON wpm.permit_reference_number = w.permit_reference_number
GROUP BY 
    wpm.permit_reference_number, 
    w.promoter_organisation, 
    w.highway_authority, 
    w.street_name, 
    w.activity_type, 
    w.work_category, 
    w.work_status, 
    w.usrn, 
    wpm.work_easting, 
    wpm.work_northing, 
    wpm.actual_start_date_time,
    wpm.actual_end_date_time,
    wpm.duration_days
ORDER BY wellbeing_total_impact DESC