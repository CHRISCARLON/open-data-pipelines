{% set current_schema = 'raw_data_' ~ var('year') %}
{% set current_table = '"' ~ var('month') ~ '_' ~ var('year') ~ '"' %}

{{ config(
    materialized='table',
    pre_hook="INSTALL spatial; LOAD spatial;"
) }}

WITH promoter_totals AS (
  SELECT 
    promoter_organisation,
    promoter_swa_code,
    COUNT(DISTINCT permit_reference_number) as total_permits,
    COUNT(DISTINCT CASE 
      WHEN work_status_ref = 'completed' 
        AND event_type = 'WORK_STOP' 
        AND work_category_ref IN ('immediate_urgent', 'immediate_emergency')
      THEN permit_reference_number 
    END) as emergency_permits
  FROM {{ current_schema }}.{{ current_table }}
  GROUP BY promoter_organisation, promoter_swa_code
),
emergency_works AS (
  SELECT DISTINCT ON (permit_reference_number)
  permit_reference_number,
  promoter_organisation,
  promoter_swa_code,
  highway_authority,
  street_name,
  activity_type,
  work_category,
  work_status,
  actual_start_date_time,
  actual_end_date_time,
  works_location_coordinates,
  usrn,
  -- Create point geometry for the work location
  ST_Point(
    ST_X(ST_Centroid(ST_GeomFromText(works_location_coordinates))), 
    ST_Y(ST_Centroid(ST_GeomFromText(works_location_coordinates)))
  ) AS work_point,
  -- Create 500m buffer around work point
  ST_Buffer(
    ST_Point(
      ST_X(ST_Centroid(ST_GeomFromText(works_location_coordinates))), 
      ST_Y(ST_Centroid(ST_GeomFromText(works_location_coordinates)))
    ), 
    500
  ) AS work_buffer_500m,
  -- Extract eastings and northings for display
  ROUND(ST_X(ST_Centroid(ST_GeomFromText(works_location_coordinates))), 2) AS easting,
  ROUND(ST_Y(ST_Centroid(ST_GeomFromText(works_location_coordinates))), 2) AS northing,
  -- Calculate duration as actual days worked within the end month
  CASE 
    WHEN EXTRACT(YEAR FROM CAST(actual_start_date_time AS TIMESTAMP)) = EXTRACT(YEAR FROM CAST(actual_end_date_time AS TIMESTAMP)) 
         AND EXTRACT(MONTH FROM CAST(actual_start_date_time AS TIMESTAMP)) = EXTRACT(MONTH FROM CAST(actual_end_date_time AS TIMESTAMP))
    THEN 
      -- Same month: count days from start to end
      EXTRACT(DAY FROM CAST(actual_end_date_time AS TIMESTAMP)) - EXTRACT(DAY FROM CAST(actual_start_date_time AS TIMESTAMP))
    ELSE 
      -- Different months: count from 1st of end month to end date
      EXTRACT(DAY FROM CAST(actual_end_date_time AS TIMESTAMP))
  END AS duration_days
FROM {{ current_schema }}.{{ current_table }}
WHERE work_status_ref = 'completed'
  AND event_type = 'WORK_STOP'
  AND work_category_ref IN ('immediate_urgent', 'immediate_emergency')
  AND actual_start_date_time IS NOT NULL 
  AND actual_end_date_time IS NOT NULL
  AND works_location_coordinates IS NOT NULL
ORDER BY permit_reference_number, actual_start_date_time
)
SELECT 
  ew.*,
  pt.total_permits as promoter_total_permits,
  pt.emergency_permits as promoter_emergency_permits,
  ROUND(100.0 * pt.emergency_permits / NULLIF(pt.total_permits, 0), 2) as emergency_percentage
FROM emergency_works ew
LEFT JOIN promoter_totals pt 
  ON ew.promoter_organisation = pt.promoter_organisation 
  AND ew.promoter_swa_code = pt.promoter_swa_code