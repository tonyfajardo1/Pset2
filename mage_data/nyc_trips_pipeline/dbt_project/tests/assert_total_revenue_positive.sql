-- Test: Todos los ingresos diarios deben ser positivos
SELECT
    pickup_date,
    service_type,
    total_revenue
FROM {{ ref('gold_daily_revenue') }}
WHERE total_revenue < 0
