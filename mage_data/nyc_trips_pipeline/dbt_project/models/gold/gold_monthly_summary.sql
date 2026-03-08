{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH monthly_agg AS (
    SELECT
        pickup_year,
        pickup_month,
        service_type,
        COUNT(*) AS total_trips,
        COUNT(DISTINCT pickup_date) AS active_days,
        SUM(passenger_count) AS total_passengers,
        SUM(trip_distance) AS total_distance,
        SUM(total_amount) AS total_revenue,
        SUM(fare_amount) AS total_fares,
        SUM(tip_amount) AS total_tips,
        SUM(tolls_amount) AS total_tolls,
        AVG(trip_distance) AS avg_distance,
        AVG(trip_duration_minutes) AS avg_duration,
        AVG(total_amount) AS avg_revenue,
        AVG(passenger_count) AS avg_passengers,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) AS median_revenue,
        COUNT(CASE WHEN payment_type = 1 THEN 1 END) AS credit_card_trips,
        COUNT(CASE WHEN payment_type = 2 THEN 1 END) AS cash_trips,
        COUNT(CASE WHEN tip_amount > 0 THEN 1 END) AS tipped_trips
    FROM {{ ref('silver_trips_unified') }}
    GROUP BY
        pickup_year,
        pickup_month,
        service_type
)

SELECT
    pickup_year AS year,
    pickup_month AS month,
    TO_DATE(pickup_year || '-' || LPAD(pickup_month::TEXT, 2, '0') || '-01', 'YYYY-MM-DD') AS month_start_date,
    service_type,
    total_trips,
    active_days,
    ROUND(total_trips::NUMERIC / NULLIF(active_days, 0), 0) AS avg_trips_per_day,
    total_passengers,
    ROUND(total_distance::NUMERIC, 2) AS total_distance_miles,
    ROUND(total_revenue::NUMERIC, 2) AS total_revenue,
    ROUND(total_fares::NUMERIC, 2) AS total_fares,
    ROUND(total_tips::NUMERIC, 2) AS total_tips,
    ROUND(total_tolls::NUMERIC, 2) AS total_tolls,
    ROUND(avg_distance::NUMERIC, 2) AS avg_distance_miles,
    ROUND(avg_duration::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(avg_revenue::NUMERIC, 2) AS avg_revenue_per_trip,
    ROUND(avg_passengers::NUMERIC, 2) AS avg_passengers_per_trip,
    ROUND(median_revenue::NUMERIC, 2) AS median_revenue,
    credit_card_trips,
    cash_trips,
    tipped_trips,
    ROUND((tipped_trips::NUMERIC / NULLIF(total_trips, 0)) * 100, 2) AS tip_rate_percentage,
    ROUND((credit_card_trips::NUMERIC / NULLIF(total_trips, 0)) * 100, 2) AS credit_card_percentage,
    ROUND((total_tips::NUMERIC / NULLIF(total_fares, 0)) * 100, 2) AS avg_tip_rate,
    CURRENT_TIMESTAMP AS processed_at
FROM monthly_agg
ORDER BY pickup_year, pickup_month, service_type
