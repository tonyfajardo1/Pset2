-- Recalcula row_count real en bronze.coverage
-- y ajusta status a: loaded / failed / missing

WITH counts AS (
    SELECT source_month AS year_month, 'yellow'::text AS service_type, COUNT(*)::bigint AS row_count
    FROM bronze.yellow_trips
    GROUP BY source_month
    UNION ALL
    SELECT source_month AS year_month, 'green'::text AS service_type, COUNT(*)::bigint AS row_count
    FROM bronze.green_trips
    GROUP BY source_month
)
UPDATE bronze.coverage c
SET
    row_count = COALESCE(
        (
            SELECT cnt.row_count
            FROM counts cnt
            WHERE cnt.year_month = c.year_month
              AND cnt.service_type = c.service_type
        ),
        0
    ),
    status = CASE
        WHEN COALESCE(
            (
                SELECT cnt.row_count
                FROM counts cnt
                WHERE cnt.year_month = c.year_month
                  AND cnt.service_type = c.service_type
            ),
            0
        ) > 0 THEN 'loaded'
        WHEN NULLIF(TRIM(COALESCE(c.file, '')), '') IS NOT NULL THEN 'failed'
        ELSE 'missing'
    END;

-- Verificacion rapida
SELECT status, COUNT(*) AS total
FROM bronze.coverage
GROUP BY status
ORDER BY status;

SELECT year_month, service_type, status, row_count
FROM bronze.coverage
ORDER BY year_month, service_type
LIMIT 20;
