{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='service_type',
        incremental_strategy='delete+insert',
        pre_hook="TRUNCATE TABLE gold.dim_service_type"
    )
}}

-- Dimension de tipos de servicio
-- Tabla particionada por LIST (service_type): yellow, green
SELECT
    service_type::VARCHAR(10) AS service_type,
    CASE service_type
        WHEN 'yellow' THEN 'Yellow Taxi'
        WHEN 'green' THEN 'Green Taxi'
    END AS service_name,
    CASE service_type
        WHEN 'yellow' THEN 'Medallion taxis that can pick up passengers anywhere in NYC'
        WHEN 'green' THEN 'Boro taxis that primarily serve outer boroughs'
    END AS service_description
FROM (VALUES ('yellow'), ('green')) AS t(service_type)
