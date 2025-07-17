{{ config(
    materialized='table',
    pre_hook="INSTALL spatial; LOAD spatial;"
) }}

WITH postcodes_with_geometry AS (
    SELECT 
        Postcode,
        PQI,
        Easting,
        Northing,
        Country_code,
        NHS_regional_code,
        NHS_health_code,
        Admin_county_code,
        Admin_district_code,
        Admin_ward_code,
        ST_Point(Easting, Northing) AS postcode_point
    FROM post_code_data.code_point
),
population_data AS (
    SELECT 
        Postcode,
        SUM(Count) as total_population,
        SUM(CASE WHEN "Sex (2 categories) Code" = 1 THEN Count ELSE 0 END) as female_population,
        SUM(CASE WHEN "Sex (2 categories) Code" = 2 THEN Count ELSE 0 END) as male_population
    FROM post_code_data.pcd_p001
    GROUP BY Postcode
),
household_data AS (
    SELECT 
        Postcode,
        Count as total_households
    FROM post_code_data.pcd_p002
)
SELECT 
    cp.Postcode,
    cp.PQI,
    cp.Easting,
    cp.Northing,
    cp.Country_code,
    cp.NHS_regional_code,
    cp.NHS_health_code,
    cp.Admin_county_code,
    cp.Admin_district_code,
    cp.Admin_ward_code,
    cp.postcode_point,
    COALESCE(pd.total_population, 0) as total_population,
    COALESCE(pd.female_population, 0) as female_population,
    COALESCE(pd.male_population, 0) as male_population,
    COALESCE(hd.total_households, 0) as total_households
FROM postcodes_with_geometry cp
LEFT JOIN population_data pd ON cp.Postcode = pd.Postcode
LEFT JOIN household_data hd ON cp.Postcode = hd.Postcode