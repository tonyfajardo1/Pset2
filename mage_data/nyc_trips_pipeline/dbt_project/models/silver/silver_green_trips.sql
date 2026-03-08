{{
    config(
        materialized='view',
        schema='silver'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'green_trips') }}
),

taxi_zones AS (
    SELECT
        "LocationID" as location_id,
        "Borough" as borough,
        "Zone" as zone_name,
        "service_zone" as service_zone
    FROM {{ source('bronze', 'taxi_zone_lookup') }}
),

cleaned AS (
    SELECT
        t.vendor_id,
        t.pickup_datetime,
        t.dropoff_datetime,
        EXTRACT(EPOCH FROM (t.dropoff_datetime - t.pickup_datetime)) / 60.0 AS trip_duration_minutes,
        COALESCE(t.passenger_count, 1)::INTEGER AS passenger_count,
        t.trip_distance,
        COALESCE(t.ratecode_id, 1)::INTEGER AS ratecode_id,
        COALESCE(t.store_and_fwd_flag, 'N') AS store_and_fwd_flag,
        t.pu_location_id::INTEGER AS pu_location_id,
        t.do_location_id::INTEGER AS do_location_id,
        pu_zone.borough AS pu_borough,
        pu_zone.zone_name AS pu_zone_name,
        pu_zone.service_zone AS pu_service_zone,
        do_zone.borough AS do_borough,
        do_zone.zone_name AS do_zone_name,
        do_zone.service_zone AS do_service_zone,
        COALESCE(t.payment_type, 5)::INTEGER AS payment_type,
        COALESCE(t.trip_type, 1)::INTEGER AS trip_type,
        COALESCE(t.fare_amount, 0)::NUMERIC(10,2) AS fare_amount,
        COALESCE(t.extra, 0)::NUMERIC(10,2) AS extra,
        COALESCE(t.mta_tax, 0)::NUMERIC(10,2) AS mta_tax,
        COALESCE(t.tip_amount, 0)::NUMERIC(10,2) AS tip_amount,
        COALESCE(t.tolls_amount, 0)::NUMERIC(10,2) AS tolls_amount,
        COALESCE(t.improvement_surcharge, 0)::NUMERIC(10,2) AS improvement_surcharge,
        COALESCE(t.congestion_surcharge, 0)::NUMERIC(10,2) AS congestion_surcharge,
        COALESCE(t.total_amount, 0)::NUMERIC(10,2) AS total_amount,
        t.service_type,
        t.source_month,
        t.ingest_ts,
        t.pickup_datetime::DATE AS pickup_date,
        EXTRACT(YEAR FROM t.pickup_datetime)::INTEGER AS pickup_year,
        EXTRACT(MONTH FROM t.pickup_datetime)::INTEGER AS pickup_month,
        EXTRACT(DOW FROM t.pickup_datetime)::INTEGER AS pickup_day_of_week,
        EXTRACT(HOUR FROM t.pickup_datetime)::INTEGER AS pickup_hour
    FROM source_data t
    LEFT JOIN taxi_zones pu_zone ON t.pu_location_id = pu_zone.location_id
    LEFT JOIN taxi_zones do_zone ON t.do_location_id = do_zone.location_id
    WHERE
        t.pickup_datetime IS NOT NULL
        AND t.dropoff_datetime IS NOT NULL
        AND t.pickup_datetime <= t.dropoff_datetime
        AND t.trip_distance >= 0
        AND t.total_amount >= 0
        AND t.pu_location_id IS NOT NULL
        AND t.do_location_id IS NOT NULL
)

SELECT * FROM cleaned
