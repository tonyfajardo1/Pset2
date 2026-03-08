{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Dimension de fechas para el rango de datos (2022-2025)
WITH date_spine AS (
    SELECT generate_series(
        '2022-01-01'::date,
        '2025-12-31'::date,
        '1 day'::interval
    )::date AS date_key
)

SELECT
    date_key,
    EXTRACT(YEAR FROM date_key)::INTEGER AS year,
    EXTRACT(MONTH FROM date_key)::INTEGER AS month,
    EXTRACT(DAY FROM date_key)::INTEGER AS day,
    EXTRACT(DOW FROM date_key)::INTEGER AS day_of_week,
    EXTRACT(DOY FROM date_key)::INTEGER AS day_of_year,
    EXTRACT(WEEK FROM date_key)::INTEGER AS week_of_year,
    EXTRACT(QUARTER FROM date_key)::INTEGER AS quarter,
    CASE EXTRACT(DOW FROM date_key)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    TO_CHAR(date_key, 'Month') AS month_name,
    CASE WHEN EXTRACT(DOW FROM date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    TO_CHAR(date_key, 'YYYY-MM') AS year_month
FROM date_spine
