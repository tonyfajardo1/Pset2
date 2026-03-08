-- Test: Las horas de pickup deben estar entre 0 y 23
SELECT
    pickup_hour,
    COUNT(*) as invalid_count
FROM {{ ref('gold_hourly_patterns') }}
WHERE pickup_hour < 0 OR pickup_hour > 23
GROUP BY pickup_hour
