-- depends_on: {{ ref('impact_scores_england_archive') }}

{% set table_alias = 'impact_scores_england_latest' %}
{{ config(
    materialized='table',
    alias=table_alias,
) }}

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
            -- Add UPRN density impact based on actual distribution
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
                    {{ ref('in_progress_list_england') }} permit_data
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
                    {{ ref('completed_list_england') }} permit_data
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
    )
SELECT
     -- Calculate normalised impact scores to serve as a final metric
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
    raw_impact_level.total_impact_level * (1 + network_scoring.network_importance_factor) as weighted_impact_level,

    -- Normalise to 1-100 scale using min-max normalization
    CASE
        WHEN MAX(raw_impact_level.total_impact_level * (1 + network_scoring.network_importance_factor)) OVER () =
             MIN(raw_impact_level.total_impact_level * (1 + network_scoring.network_importance_factor)) OVER ()
        THEN 50.0  -- If all values are the same, assign middle value
        ELSE 1 + (99 *
            (raw_impact_level.total_impact_level * (1 + network_scoring.network_importance_factor) -
             MIN(raw_impact_level.total_impact_level * (1 + network_scoring.network_importance_factor)) OVER ()) /
            NULLIF(MAX(raw_impact_level.total_impact_level * (1 + network_scoring.network_importance_factor)) OVER () -
                   MIN(raw_impact_level.total_impact_level * (1 + network_scoring.network_importance_factor)) OVER (), 0)
        )
    END AS impact_index_score,

    -- Create categorical impact levels for easier interpretation
    CASE
        WHEN impact_index_score >= 80 THEN 'Critical'
        WHEN impact_index_score >= 60 THEN 'High'
        WHEN impact_index_score >= 40 THEN 'Medium'
        WHEN impact_index_score >= 20 THEN 'Low'
        ELSE 'Minimal'
    END AS impact_category,

    {{ current_timestamp() }} AS date_processed
FROM
    raw_impact_level
    LEFT JOIN {{ ref('dft_data_joins') }} la_dft_data ON raw_impact_level.highway_authority_swa_code = LOWER(la_dft_data.swa_code)
    LEFT JOIN network_scoring network_scoring ON raw_impact_level.highway_authority_swa_code = network_scoring.swa_code
