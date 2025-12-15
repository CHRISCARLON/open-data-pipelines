{% set current_schema = 'raw_data_' ~ var('year') %}
{% set current_table = '"' ~ var('month') ~ '_' ~ var('year') ~ '"' %}

{{ config(
    materialized='table',
    pre_hook="INSTALL spatial; LOAD spatial;"
) }}

WITH completed_major_works AS (
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
  AND work_category_ref = 'major'
  AND actual_start_date_time IS NOT NULL
  AND actual_end_date_time IS NOT NULL
  AND works_location_coordinates IS NOT NULL
  AND highway_authority_swa_code = '4720'
ORDER BY permit_reference_number, actual_start_date_time
),
in_progress_major_works AS (
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
  -- For in-progress works, assume 7 day duration
  CAST(actual_start_date_time AS TIMESTAMP) + INTERVAL '7 days' AS actual_end_date_time,
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
  -- Assume 7 day duration for in-progress works
  7 AS duration_days
FROM {{ current_schema }}.{{ current_table }}
WHERE work_status_ref = 'in_progress'
  AND work_category_ref = 'major'
  AND actual_start_date_time IS NOT NULL
  AND works_location_coordinates IS NOT NULL
  AND highway_authority_swa_code = '4720'
  AND permit_reference_number NOT IN (
    SELECT permit_reference_number
    FROM {{ current_schema }}.{{ current_table }}
    WHERE work_status_ref = 'completed'
    AND event_type = 'WORK_STOP'
  )
ORDER BY permit_reference_number, actual_start_date_time
),
major_works AS (
  SELECT * FROM completed_major_works
  UNION ALL
  SELECT * FROM in_progress_major_works
)
SELECT * FROM major_works
