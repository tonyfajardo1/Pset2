{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH daily_agg AS (
    SELECT
        pickup_date,
        pickup_year,
        pickup_month,
        pickup_day_of_week,
        service_type,
        COUNT(*) AS total_trips,
        SUM(passenger_count) AS total_passengers,
        SUM(trip_distance) AS total_distance_miles,
        AVG(trip_distance) AS avg_distance_miles,
        AVG(trip_duration_minutes) AS avg_duration_minutes,
        SUM(fare_amount) AS total_fare,
        SUM(tip_amount) AS total_tips,
        SUM(tolls_amount) AS total_tolls,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_revenue_per_trip,
        AVG(tip_amount / NULLIF(fare_amount, 0)) AS avg_tip_percentage,
        COUNT(CASE WHEN tip_amount > 0 THEN 1 END) AS trips_with_tips,
        COUNT(CASE WHEN payment_type = 1 THEN 1 END) AS credit_card_trips,
        COUNT(CASE WHEN payment_type = 2 THEN 1 END) AS cash_trips
    FROM {{ ref('silver_trips_unified') }}
    GROUP BY
        pickup_date,
        pickup_year,
        pickup_month,
        pickup_day_of_week,
        service_type
)

SELECT
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_day_of_week,
    CASE pickup_day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    service_type,
    total_trips,
    total_passengers,
    ROUND(total_distance_miles::NUMERIC, 2) AS total_distance_miles,
    ROUND(avg_distance_miles::NUMERIC, 2) AS avg_distance_miles,
    ROUND(avg_duration_minutes::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(total_fare, 2) AS total_fare,
    ROUND(total_tips, 2) AS total_tips,
    ROUND(total_tolls, 2) AS total_tolls,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_revenue_per_trip, 2) AS avg_revenue_per_trip,
    ROUND(avg_tip_percentage * 100, 2) AS avg_tip_percentage,
    trips_with_tips,
    credit_card_trips,
    cash_trips,
    ROUND((credit_card_trips::NUMERIC / NULLIF(total_trips, 0)) * 100, 2) AS credit_card_percentage,
    CURRENT_TIMESTAMP AS processed_at
FROM daily_agg
