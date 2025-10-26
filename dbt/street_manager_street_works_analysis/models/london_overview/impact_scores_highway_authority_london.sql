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
    -- Calculate impact at USRN level
    usrn_impact AS (
        SELECT
            usrn,
            street_name,
            highway_authority,
            highway_authority_swa_code,
            uprn_count,
            geometry,
            total_impact_level,

            -- Normalise to 1-100 scale using percent rank
            PERCENT_RANK() OVER (ORDER BY total_impact_level) * 100 AS impact_index_score,

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
-- Aggregate the USRN-level impacts to highway authority level
SELECT
    ui.highway_authority,
    ui.highway_authority_swa_code,
    COUNT(DISTINCT ui.usrn) as total_usrns_count,
    AVG(ui.uprn_count) as avg_uprn_count,
    SUM(ui.uprn_count) as total_uprn_count,
    SUM(ui.total_impact_level) AS total_impact_level,
    AVG(ui.total_impact_level) AS avg_impact_level_per_usrn,
    MAX(ui.total_impact_level) AS max_impact_level,
    MIN(ui.total_impact_level) AS min_impact_level,
    SUM(ui.impact_index_score) AS total_impact_index_score,
    AVG(ui.impact_index_score) AS avg_impact_index_score,
    -- Normalise total impact to 0-100 scale
    PERCENT_RANK() OVER (ORDER BY SUM(ui.total_impact_level)) * 100 AS highway_authority_impact_score,
    -- Create categorical impact levels based on the normalised score
    CASE
        WHEN highway_authority_impact_score >= 95 THEN 'Severe'     -- Top 5%
        WHEN highway_authority_impact_score >= 75 THEN 'High'       -- Next 20%
        WHEN highway_authority_impact_score >= 50 THEN 'Moderate'   -- Middle 25%
        WHEN highway_authority_impact_score >= 25 THEN 'Low'        -- Next 25%
        ELSE 'Minimal'                                             -- Bottom 25%
    END AS impact_category,
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
    usrn_impact ui
    LEFT JOIN work_category_breakdown wcb ON ui.highway_authority = wcb.highway_authority
        AND ui.highway_authority_swa_code = wcb.highway_authority_swa_code
GROUP BY
    ui.highway_authority,
    ui.highway_authority_swa_code
ORDER BY highway_authority_impact_score DESC 