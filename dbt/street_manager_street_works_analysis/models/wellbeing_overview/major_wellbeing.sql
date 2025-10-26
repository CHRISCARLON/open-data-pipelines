{% set table_alias = 'major_wellbeing_impact' %}
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
    -- Calculate wellbeing impact: £2.10 × Days × Households_Affected
    -- £2.10 represents the uplifted value to 25-26 prices (original was £1.61 in 18/19 prices)
    -- Uplifted value = £1.61 × (139.08 / 106.43)
    ROUND(2.10 * wpm.duration_days * SUM(wpm.total_households), 2) as wellbeing_total_impact
FROM {{ ref('int_major_works_postcodes_by_authority') }} wpm
JOIN {{ ref('stg_major_works_by_authority') }} w
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
