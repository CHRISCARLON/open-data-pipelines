-- depends_on: {{ ref('stg_works_by_authority') }}
-- depends_on: {{ ref('int_postcodes') }}

{{ config(
    materialized='table',
    pre_hook="INSTALL spatial; LOAD spatial;"
) }}

{% set highway_authorities = get_highway_authorities() %}

{% for authority in highway_authorities %}
  {%- if not loop.first %}
UNION ALL
  {%- endif %}
  (
    SELECT 
      w.permit_reference_number,
      w.easting as work_easting,
      w.northing as work_northing,
      w.duration_days,
      w.actual_start_date_time,
      w.actual_end_date_time,
      w.highway_authority,
      cp.Postcode,
      cp.PQI,
      cp.Easting as postcode_easting,
      cp.Northing as postcode_northing,
      cp.Country_code,
      cp.NHS_regional_code,
      cp.NHS_health_code,
      cp.Admin_county_code,
      cp.Admin_district_code,
      cp.Admin_ward_code,
      cp.total_population,
      cp.female_population,
      cp.male_population,
      cp.total_households,
      ST_Distance(w.work_point, cp.postcode_point) as distance_m
    FROM {{ ref('stg_works_by_authority') }} w
    LEFT JOIN {{ ref('int_postcodes') }} cp
      ON ST_Contains(w.work_buffer_500m, cp.postcode_point)
    WHERE w.highway_authority = '{{ authority }}'
  )
{% endfor %}