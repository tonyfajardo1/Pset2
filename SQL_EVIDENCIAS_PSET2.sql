-- ===========================================================
-- Evidencias PSet 02: particionamiento, pruning y calidad
-- Ejecutar en psql conectado a la DB nyc_trips
-- ===========================================================

-- 1) Estructura de tablas particionadas
\d+ gold.fct_trips
\d+ gold.dim_zone
\d+ gold.dim_service_type
\d+ gold.dim_payment_type

-- 2) Lista de particiones creadas
SELECT
  parent.relname AS tabla_padre,
  child.relname AS particion,
  pg_get_expr(child.relpartbound, child.oid) AS expresion_particion
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace nmsp ON parent.relnamespace = nmsp.oid
WHERE nmsp.nspname = 'gold'
ORDER BY parent.relname, child.relname;

-- 3) Partition pruning en fct_trips (RANGE)
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM gold.fct_trips
WHERE pickup_date_key BETWEEN '2024-03-01' AND '2024-03-31';

-- 4) Partition pruning en dim_zone (HASH)
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM gold.dim_zone
WHERE zone_key = 132;

-- 5) Conteos de tablas principales
SELECT 'gold.fct_trips' AS table_name, COUNT(*) AS rows FROM gold.fct_trips
UNION ALL
SELECT 'gold.dim_date' AS table_name, COUNT(*) AS rows FROM gold.dim_date
UNION ALL
SELECT 'gold.dim_zone' AS table_name, COUNT(*) AS rows FROM gold.dim_zone
UNION ALL
SELECT 'gold.dim_service_type' AS table_name, COUNT(*) AS rows FROM gold.dim_service_type
UNION ALL
SELECT 'gold.dim_payment_type' AS table_name, COUNT(*) AS rows FROM gold.dim_payment_type
UNION ALL
SELECT 'gold.dim_vendor' AS table_name, COUNT(*) AS rows FROM gold.dim_vendor;

-- 6) Cobertura mensual (tabla obligatoria)
SELECT year_month, service_type, status, row_count
FROM bronze.coverage
ORDER BY year_month, service_type;
