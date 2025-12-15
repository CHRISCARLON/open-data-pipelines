-- depends_on: {{ ref('stg_emergency_works_by_authority') }}
-- depends_on: {{ ref('int_postcodes') }}

{{ config(
    materialized='table',
    pre_hook="INSTALL spatial; LOAD spatial;"
) }}

SELECT
  w.permit_reference_number,
  w.easting as work_easting,
  w.northing as work_northing,
  w.duration_days,
  w.actual_start_date_time,
  w.actual_end_date_time,
  w.highway_authority,
  cp.postcode,
  cp.positional_quality_indicator,
  ST_X(ST_GeomFromText(cp.postcode_point)) as postcode_easting,
  ST_Y(ST_GeomFromText(cp.postcode_point)) as postcode_northing,
  cp.country_code,
  cp.nhs_regional_ha_code,
  cp.nhs_ha_code,
  cp.admin_county_code,
  cp.admin_district_code,
  cp.admin_ward_code,
  cp.total_population,
  cp.female_population,
  cp.male_population,
  cp.total_households,
  ST_Distance(w.work_point, ST_GeomFromText(cp.postcode_point)) as distance_m
FROM {{ ref('stg_emergency_works_by_authority') }} w
LEFT JOIN {{ ref('int_postcodes') }} cp
  ON w.easting - 500 <= ST_X(ST_GeomFromText(cp.postcode_point))
  AND w.easting + 500 >= ST_X(ST_GeomFromText(cp.postcode_point))
  AND w.northing - 500 <= ST_Y(ST_GeomFromText(cp.postcode_point))
  AND w.northing + 500 >= ST_Y(ST_GeomFromText(cp.postcode_point))
  AND ST_Contains(w.work_buffer_500m, ST_GeomFromText(cp.postcode_point))
