{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH hourly_agg AS (
    SELECT
        pickup_year,
        pickup_month,
        pickup_hour,
        pickup_day_of_week,
        service_type,
        COUNT(*) AS total_trips,
        AVG(trip_duration_minutes) AS avg_duration_minutes,
        AVG(trip_distance) AS avg_distance_miles,
        AVG(total_amount) AS avg_revenue,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('silver_trips_unified') }}
    GROUP BY
        pickup_year,
        pickup_month,
        pickup_hour,
        pickup_day_of_week,
        service_type
)

SELECT
    pickup_year,
    pickup_month,
    pickup_hour,
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
    CASE
        WHEN pickup_day_of_week IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    CASE
        WHEN pickup_hour BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN pickup_hour BETWEEN 10 AND 15 THEN 'Midday'
        WHEN pickup_hour BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN pickup_hour BETWEEN 20 AND 23 THEN 'Night'
        ELSE 'Late Night'
    END AS time_period,
    service_type,
    total_trips,
    ROUND(avg_duration_minutes::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(avg_distance_miles::NUMERIC, 2) AS avg_distance_miles,
    ROUND(avg_revenue::NUMERIC, 2) AS avg_revenue,
    ROUND(total_revenue::NUMERIC, 2) AS total_revenue,
    CURRENT_TIMESTAMP AS processed_at
FROM hourly_agg
