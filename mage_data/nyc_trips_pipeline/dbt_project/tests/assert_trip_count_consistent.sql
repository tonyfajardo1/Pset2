-- Test: El total de viajes unificados debe igualar la suma de yellow + green
WITH unified_count AS (
    SELECT COUNT(*) as total FROM {{ ref('silver_trips_unified') }}
),
separate_counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('silver_yellow_trips') }}) +
        (SELECT COUNT(*) FROM {{ ref('silver_green_trips') }}) as total
)
SELECT
    unified_count.total as unified,
    separate_counts.total as separate
FROM unified_count, separate_counts
WHERE unified_count.total != separate_counts.total
