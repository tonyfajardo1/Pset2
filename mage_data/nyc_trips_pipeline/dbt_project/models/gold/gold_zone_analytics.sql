{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH pickup_stats AS (
    SELECT
        pu_location_id AS location_id,
        pu_borough AS borough,
        pu_zone_name AS zone_name,
        pickup_year,
        pickup_month,
        service_type,
        COUNT(*) AS pickup_count,
        SUM(total_amount) AS pickup_revenue,
        AVG(total_amount) AS avg_pickup_revenue
    FROM {{ ref('silver_trips_unified') }}
    WHERE pu_borough IS NOT NULL
    GROUP BY
        pu_location_id,
        pu_borough,
        pu_zone_name,
        pickup_year,
        pickup_month,
        service_type
),

dropoff_stats AS (
    SELECT
        do_location_id AS location_id,
        do_borough AS borough,
        do_zone_name AS zone_name,
        pickup_year,
        pickup_month,
        service_type,
        COUNT(*) AS dropoff_count,
        SUM(total_amount) AS dropoff_revenue
    FROM {{ ref('silver_trips_unified') }}
    WHERE do_borough IS NOT NULL
    GROUP BY
        do_location_id,
        do_borough,
        do_zone_name,
        pickup_year,
        pickup_month,
        service_type
)

SELECT
    COALESCE(p.location_id, d.location_id) AS location_id,
    COALESCE(p.borough, d.borough) AS borough,
    COALESCE(p.zone_name, d.zone_name) AS zone_name,
    COALESCE(p.pickup_year, d.pickup_year) AS year,
    COALESCE(p.pickup_month, d.pickup_month) AS month,
    COALESCE(p.service_type, d.service_type) AS service_type,
    COALESCE(p.pickup_count, 0) AS total_pickups,
    COALESCE(d.dropoff_count, 0) AS total_dropoffs,
    COALESCE(p.pickup_count, 0) + COALESCE(d.dropoff_count, 0) AS total_trips,
    ROUND(COALESCE(p.pickup_revenue, 0)::NUMERIC, 2) AS pickup_revenue,
    ROUND(COALESCE(d.dropoff_revenue, 0)::NUMERIC, 2) AS dropoff_revenue,
    ROUND(COALESCE(p.avg_pickup_revenue, 0)::NUMERIC, 2) AS avg_fare,
    CASE
        WHEN COALESCE(p.pickup_count, 0) > COALESCE(d.dropoff_count, 0) THEN 'Net Origin'
        WHEN COALESCE(p.pickup_count, 0) < COALESCE(d.dropoff_count, 0) THEN 'Net Destination'
        ELSE 'Balanced'
    END AS zone_flow_type,
    COALESCE(p.pickup_count, 0) - COALESCE(d.dropoff_count, 0) AS net_flow,
    CURRENT_TIMESTAMP AS processed_at
FROM pickup_stats p
FULL OUTER JOIN dropoff_stats d
    ON p.location_id = d.location_id
    AND p.pickup_year = d.pickup_year
    AND p.pickup_month = d.pickup_month
    AND p.service_type = d.service_type
