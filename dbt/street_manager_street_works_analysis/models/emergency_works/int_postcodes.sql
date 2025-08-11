{{ config(
    materialized='table',
    pre_hook="INSTALL spatial; LOAD spatial;"
) }}

WITH postcodes_with_geometry AS (
    SELECT
        postcode,
        positional_quality_indicator,
        country_code,
        nhs_regional_ha_code,
        nhs_ha_code,
        admin_county_code,
        admin_district_code,
        admin_ward_code,
        geometry AS postcode_point
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
    cp.postcode,
    cp.positional_quality_indicator,
    cp.country_code,
    cp.nhs_regional_ha_code,
    cp.nhs_ha_code,
    cp.admin_county_code,
    cp.admin_district_code,
    cp.admin_ward_code,
    cp.postcode_point,
    COALESCE(pd.total_population, 0) as total_population,
    COALESCE(pd.female_population, 0) as female_population,
    COALESCE(pd.male_population, 0) as male_population,
    COALESCE(hd.total_households, 0) as total_households
FROM postcodes_with_geometry cp
LEFT JOIN population_data pd ON cp.postcode = pd.Postcode
LEFT JOIN household_data hd ON cp.postcode = hd.Postcode
