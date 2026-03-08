{{
    config(
        materialized='view',
        schema='silver'
    )
}}

WITH yellow AS (
    SELECT
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        trip_duration_minutes,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
        pu_borough,
        pu_zone_name,
        do_borough,
        do_zone_name,
        payment_type,
        NULL::INTEGER AS trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,
        service_type,
        source_month,
        ingest_ts,
        pickup_date,
        pickup_year,
        pickup_month,
        pickup_day_of_week,
        pickup_hour
    FROM {{ ref('silver_yellow_trips') }}
),

green AS (
    SELECT
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        trip_duration_minutes,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
        pu_borough,
        pu_zone_name,
        do_borough,
        do_zone_name,
        payment_type,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        0::NUMERIC(10,2) AS airport_fee,
        total_amount,
        service_type,
        source_month,
        ingest_ts,
        pickup_date,
        pickup_year,
        pickup_month,
        pickup_day_of_week,
        pickup_hour
    FROM {{ ref('silver_green_trips') }}
)

SELECT * FROM yellow
UNION ALL
SELECT * FROM green
