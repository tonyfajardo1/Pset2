{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='zone_key',
        incremental_strategy='delete+insert',
        pre_hook="TRUNCATE TABLE gold.dim_zone"
    )
}}

-- Dimension de zonas desde taxi_zone_lookup
-- Tabla particionada por HASH (zone_key) - 4 particiones
SELECT
    "LocationID" AS zone_key,
    "Borough" AS borough,
    "Zone" AS zone_name,
    "service_zone" AS service_zone,
    CASE "Borough"
        WHEN 'Manhattan' THEN 1
        WHEN 'Brooklyn' THEN 2
        WHEN 'Queens' THEN 3
        WHEN 'Bronx' THEN 4
        WHEN 'Staten Island' THEN 5
        WHEN 'EWR' THEN 6
        ELSE 7
    END AS borough_code
FROM {{ source('bronze', 'taxi_zone_lookup') }}
