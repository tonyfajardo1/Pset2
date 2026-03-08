{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='trip_key',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        pre_hook="TRUNCATE TABLE gold.fct_trips"
    )
}}
-- Fact table particionada por RANGE (pickup_date_key)
-- Particiones mensuales 2022-2025

-- Fact table: 1 fila = 1 viaje
-- Granularidad: cada viaje individual de taxi
WITH trips AS (
    SELECT
        -- Surrogate key
        ROW_NUMBER() OVER (ORDER BY pickup_datetime, service_type, pu_location_id) AS trip_key,

        -- Foreign keys to dimensions
        pickup_date AS pickup_date_key,
        pu_location_id AS pu_zone_key,
        do_location_id AS do_zone_key,
        service_type,
        COALESCE(payment_type, 5) AS payment_type_id,
        COALESCE(vendor_id, 0)::INTEGER AS vendor_id,

        -- Degenerate dimensions (atributos del viaje)
        pickup_datetime,
        dropoff_datetime,
        EXTRACT(HOUR FROM pickup_datetime)::INTEGER AS pickup_hour,
        EXTRACT(DOW FROM pickup_datetime)::INTEGER AS pickup_day_of_week,

        -- Measures
        COALESCE(passenger_count, 1)::INTEGER AS passenger_count,
        COALESCE(trip_distance, 0)::NUMERIC(10,2) AS trip_distance,
        COALESCE(trip_duration_minutes, 0)::NUMERIC(10,2) AS trip_duration_minutes,

        -- Financial measures
        COALESCE(fare_amount, 0)::NUMERIC(10,2) AS fare_amount,
        COALESCE(extra, 0)::NUMERIC(10,2) AS extra,
        COALESCE(mta_tax, 0)::NUMERIC(10,2) AS mta_tax,
        COALESCE(tip_amount, 0)::NUMERIC(10,2) AS tip_amount,
        COALESCE(tolls_amount, 0)::NUMERIC(10,2) AS tolls_amount,
        COALESCE(improvement_surcharge, 0)::NUMERIC(10,2) AS improvement_surcharge,
        COALESCE(congestion_surcharge, 0)::NUMERIC(10,2) AS congestion_surcharge,
        COALESCE(airport_fee, 0)::NUMERIC(10,2) AS airport_fee,
        COALESCE(total_amount, 0)::NUMERIC(10,2) AS total_amount,

        -- Ratecode
        COALESCE(ratecode_id, 1)::INTEGER AS ratecode_id,
        COALESCE(store_and_fwd_flag, 'N') AS store_and_fwd_flag,

        -- Metadata
        source_month,
        ingest_ts

    FROM {{ ref('silver_trips_unified') }}
)

SELECT * FROM trips
