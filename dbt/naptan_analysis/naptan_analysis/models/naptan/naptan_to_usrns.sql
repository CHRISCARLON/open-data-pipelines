{% set table_alias = 'naptan_to_usrns' %}
{{ config(
    materialized='table', 
    alias=table_alias,
    pre_hook="INSTALL spatial; LOAD spatial;"
) }}

-- HAVING PROBLEMMMMSSSSSSS
-- not getting many intersects when i think i should be... investigate!!
-- could be crs, could be buffer size, could be something else maybe where the stops are...?
-- plus this is slow as shit and i need to run this on 1.3 million usrns and 435k odd nodes
-- do this locally in duckdb first as doing it in motherduck = spenny

WITH usrn_geoms AS (
    SELECT 
        usrn,
        street_type,
        -- Keep in original EPSG:27700 (British National Grid)
        ST_GeomFromText(geometry) AS geometry,
        -- Add 20m buffer around road centerlines to create road corridors
        ST_Buffer(ST_GeomFromText(geometry), 50) AS buffered_geometry  -- 50 meters
    FROM os_open_usrns.open_usrns_latest
    WHERE geometry IS NOT NULL
    LIMIT 30000
),

naptan_geoms AS (
    SELECT 
        ATCOCode,
        NaptanCode,
        CommonName,
        Street,
        LocalityName,
        Status,
        -- Use Easting/Northing columns (also EPSG:27700)
        ST_Point(Easting, Northing) AS geometry,
        -- Add 30m buffer around each bus stop for positioning uncertainty
        ST_Buffer(ST_Point(Easting, Northing), 100) AS buffered_geometry  -- 100 meters
    FROM naptan_data.LATEST_STOPS
    WHERE Easting IS NOT NULL 
      AND Northing IS NOT NULL
      AND Status = 'active'  -- Only active bus stops
    LIMIT 30000
)

SELECT 
    n.ATCOCode,
    n.NaptanCode, 
    n.CommonName,
    n.Street AS naptan_street,
    n.LocalityName,
    n.Status,
    u.usrn,
    u.street_type,
    n.geometry AS naptan_point,
    u.geometry AS usrn_centerline,
    ST_Distance(n.geometry, u.geometry) AS distance_meters,
    {{ current_timestamp() }} AS date_processed
FROM naptan_geoms n
LEFT JOIN usrn_geoms u ON ST_Intersects(n.buffered_geometry, u.buffered_geometry)
WHERE n.geometry IS NOT NULL