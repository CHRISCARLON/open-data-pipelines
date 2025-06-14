{% set table_alias = 'impact_scores_highway_authority_london_latest' %}
{{ config(materialized='table', alias=table_alias) }}

WITH
    base_permit_data AS (
        SELECT
            usrn,
            street_name,
            highway_authority,
            highway_authority_swa_code,
            work_category,
            is_ttro_required,
            is_traffic_sensitive,
            traffic_management_type_ref,
            uprn_count,
            geometry,
            -- Base impact level from work category
            CASE
                WHEN work_category = 'Standard' THEN 2
                WHEN work_category = 'Major' THEN 5
                WHEN work_category = 'Minor' THEN 1
                WHEN work_category = 'HS2 (Highway)' THEN 2
                WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') THEN 4
                ELSE 0
            END
            -- Add TTRO impact
            + CASE
                WHEN is_ttro_required = 'Yes' THEN 0.5
                ELSE 0
            END
            -- Add traffic sensitive impact
            + CASE
                WHEN is_traffic_sensitive = 'Yes' THEN 0.5
                ELSE 0
            END
            -- Add traffic management impact
            + CASE
            -- High Impact Traffic Management
                WHEN traffic_management_type_ref IN (
                    'road_closure',
                    'contra_flow',
                    'lane_closure',
                    'convoy_workings',
                    'multi_way_signals',
                    'two_way_signals'
                ) THEN 2.0
                -- Medium Impact Traffic Management
                WHEN traffic_management_type_ref IN (
                    'give_and_take',
                    'stop_go_boards',
                    'priority_working'
                ) THEN 1.0
                -- Low Impact Traffic Management
                WHEN traffic_management_type_ref = 'some_carriageway_incursion' THEN 0.5
                -- No Impact
                WHEN traffic_management_type_ref = 'no_carriageway_incursion' THEN 0
                -- Default case for NULL
                WHEN traffic_management_type_ref IS NULL THEN 0.5
                ELSE 0
            END
            -- Add UPRN density
            + CASE
                WHEN uprn_count <= 5 THEN 0.2
                WHEN uprn_count <= 10 THEN 0.4
                WHEN uprn_count <= 25 THEN 0.6
                WHEN uprn_count <= 50 THEN 0.8
                WHEN uprn_count <= 100 THEN 1
                WHEN uprn_count <= 200 THEN 1.2
                WHEN uprn_count <= 500 THEN 1.4
                ELSE 1.6
            END AS impact_level
        FROM
            (
                SELECT
                    permit_data.usrn,
                    permit_data.street_name,
                    permit_data.highway_authority,
                    permit_data.highway_authority_swa_code,
                    permit_data.activity_type,
                    permit_data.work_category,
                    permit_data.actual_start_date_time,
                    permit_data.is_ttro_required,
                    permit_data.is_traffic_sensitive,
                    permit_data.traffic_management_type_ref,
                    permit_data.geometry,
                    COALESCE(uprn_usrn.uprn_count, 0) as uprn_count
                FROM
                    {{ ref('in_progress_list_london') }} permit_data
                    LEFT JOIN {{ ref('uprn_usrn_count') }} uprn_usrn ON permit_data.usrn = uprn_usrn.usrn
                UNION ALL
                SELECT
                    permit_data.usrn,
                    permit_data.street_name,
                    permit_data.highway_authority,
                    permit_data.highway_authority_swa_code,
                    permit_data.activity_type,
                    permit_data.work_category,
                    permit_data.actual_start_date_time,
                    permit_data.is_ttro_required,
                    permit_data.is_traffic_sensitive,
                    permit_data.traffic_management_type_ref,
                    permit_data.geometry,
                    COALESCE(uprn_usrn.uprn_count, 0) as uprn_count
                FROM
                    {{ ref('completed_list_london') }} permit_data
                    LEFT JOIN {{ ref('uprn_usrn_count') }} uprn_usrn ON permit_data.usrn = uprn_usrn.usrn
            ) AS combined_works
    ),
    raw_impact_level AS (
        -- Calculate non normalised impact scores to serve as a base 
        SELECT
            usrn,
            street_name,
            highway_authority,
            LOWER(highway_authority_swa_code) as highway_authority_swa_code,
            uprn_count,
            geometry,
            SUM(impact_level) AS total_impact_level
        FROM
            base_permit_data
        GROUP BY
            usrn,
            street_name,
            highway_authority,
            highway_authority_swa_code,
            uprn_count,
            geometry
    ),
     -- Calculate metrics to normalise the impact score
    network_scoring AS (
        SELECT
            LOWER(swa_code) as swa_code,
            total_road_length,
            traffic_flow_2023,
            -- Calculate traffic density per km
            (CAST(traffic_flow_2023 AS FLOAT) /
             NULLIF(CAST(total_road_length AS FLOAT), 0)) as traffic_density,

            -- Normalise density to 0-1 scale by dividing by max density
            traffic_density / NULLIF(MAX(traffic_density) OVER (), 0) as network_importance_factor
        FROM
            {{ ref('dft_data_joins') }}
    ),
    -- Calculate weighted impact at USRN level 
    usrn_weighted_impact AS (
        SELECT
            raw_impact_level.usrn,
            raw_impact_level.street_name,
            raw_impact_level.highway_authority,
            raw_impact_level.highway_authority_swa_code,
            raw_impact_level.uprn_count,
            raw_impact_level.geometry,
            raw_impact_level.total_impact_level,
            CAST(la_dft_data.total_road_length AS FLOAT) as total_road_length,
            CAST(la_dft_data.traffic_flow_2023 AS FLOAT) as traffic_flow_2023,
            network_scoring.traffic_density,
            network_scoring.network_importance_factor,
            -- Weighted impact calculation
            raw_impact_level.total_impact_level * (1 + COALESCE(network_scoring.network_importance_factor, 0)) as weighted_impact_level,
            
            -- Normalise to 1-100 scale using min-max normalization 
            PERCENT_RANK() OVER (
                ORDER BY raw_impact_level.total_impact_level * (1 + COALESCE(network_scoring.network_importance_factor, 0))
            ) * 100 AS impact_index_score,
            
            -- Create categorical impact levels
            CASE 
                WHEN impact_index_score >= 95 THEN 'Severe'
                WHEN impact_index_score >= 75 THEN 'High'
                WHEN impact_index_score >= 50 THEN 'Moderate'
                WHEN impact_index_score >= 25 THEN 'Low'
                ELSE 'Minimal'
            END AS impact_category
        FROM
            raw_impact_level
            LEFT JOIN {{ ref('dft_data_joins') }} la_dft_data ON raw_impact_level.highway_authority_swa_code = LOWER(la_dft_data.swa_code)
            LEFT JOIN network_scoring network_scoring ON raw_impact_level.highway_authority_swa_code = network_scoring.swa_code
    ),
    -- Get work category breakdown for additional context
    work_category_breakdown AS (
        SELECT
            highway_authority,
            LOWER(highway_authority_swa_code) as highway_authority_swa_code,
            COUNT(CASE WHEN work_category = 'Major' THEN 1 END) as major_works_count,
            COUNT(CASE WHEN work_category = 'Standard' THEN 1 END) as standard_works_count,
            COUNT(CASE WHEN work_category = 'Minor' THEN 1 END) as minor_works_count,
            COUNT(CASE WHEN work_category = 'HS2 (Highway)' THEN 1 END) as hs2_works_count,
            COUNT(CASE WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') THEN 1 END) as emergency_works_count,
            COUNT(CASE WHEN is_ttro_required = 'Yes' THEN 1 END) as ttro_required_count,
            COUNT(CASE WHEN is_traffic_sensitive = 'Yes' THEN 1 END) as traffic_sensitive_count,
            COUNT(CASE WHEN traffic_management_type_ref IN (
                'road_closure', 'contra_flow', 'lane_closure', 'convoy_workings', 'multi_way_signals', 'two_way_signals'
            ) THEN 1 END) as high_impact_traffic_mgmt_count
        FROM
            base_permit_data
        GROUP BY
            highway_authority,
            highway_authority_swa_code
    )
-- Aggregate the USRN-level weighted impacts to highway authority level
SELECT
    uwi.highway_authority,
    uwi.highway_authority_swa_code,
    COUNT(DISTINCT uwi.usrn) as total_usrns_count,
    AVG(uwi.uprn_count) as avg_uprn_count,
    SUM(uwi.uprn_count) as total_uprn_count,
    SUM(uwi.total_impact_level) AS total_impact_level,
    AVG(uwi.total_impact_level) AS avg_impact_level_per_usrn,
    MAX(uwi.total_impact_level) AS max_impact_level,
    MIN(uwi.total_impact_level) AS min_impact_level,
    SUM(uwi.weighted_impact_level) AS total_weighted_impact_level,
    AVG(uwi.weighted_impact_level) AS avg_weighted_impact_level,
    MAX(uwi.weighted_impact_level) AS max_weighted_impact_level,
    MIN(uwi.weighted_impact_level) AS min_weighted_impact_level,
    SUM(uwi.impact_index_score) AS total_impact_index_score,
    AVG(uwi.impact_index_score) AS avg_impact_index_score,
    -- Normalise total weighted impact to 0-100 scale
    PERCENT_RANK() OVER (ORDER BY SUM(uwi.weighted_impact_level)) * 100 AS highway_authority_impact_score,
    -- Create categorical impact levels based on the normalised score
    CASE 
        WHEN highway_authority_impact_score >= 95 THEN 'Severe'     -- Top 5%
        WHEN highway_authority_impact_score >= 75 THEN 'High'       -- Next 20%
        WHEN highway_authority_impact_score >= 50 THEN 'Moderate'   -- Middle 25%
        WHEN highway_authority_impact_score >= 25 THEN 'Low'        -- Next 25%
        ELSE 'Minimal'                                             -- Bottom 25%
    END AS impact_category,
    -- Network characteristics
    MAX(uwi.total_road_length) as total_road_length,
    MAX(uwi.traffic_flow_2023) as traffic_flow_2023,
    MAX(uwi.traffic_density) as traffic_density,
    MAX(uwi.network_importance_factor) as network_importance_factor,
    -- Work breakdown
    MAX(wcb.major_works_count) as major_works_count,
    MAX(wcb.standard_works_count) as standard_works_count,
    MAX(wcb.minor_works_count) as minor_works_count,
    MAX(wcb.hs2_works_count) as hs2_works_count,
    MAX(wcb.emergency_works_count) as emergency_works_count,
    MAX(wcb.ttro_required_count) as ttro_required_count,
    MAX(wcb.traffic_sensitive_count) as traffic_sensitive_count,
    MAX(wcb.high_impact_traffic_mgmt_count) as high_impact_traffic_mgmt_count,
    
    {{ current_timestamp() }} AS date_processed
FROM
    usrn_weighted_impact uwi
    LEFT JOIN work_category_breakdown wcb ON uwi.highway_authority = wcb.highway_authority 
        AND uwi.highway_authority_swa_code = wcb.highway_authority_swa_code
GROUP BY
    uwi.highway_authority,
    uwi.highway_authority_swa_code
ORDER BY highway_authority_impact_score DESC 