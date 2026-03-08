-- ============================================================
-- SCRIPT DE PARTICIONAMIENTO PARA GOLD LAYER
-- Ejecutar ANTES de dbt run --select gold
-- ============================================================

-- Crear schema gold si no existe
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- 1. FCT_TRIPS - PARTITION BY RANGE (pickup_date)
-- ============================================================

-- Eliminar tabla existente si existe (para recrear con particiones)
DROP TABLE IF EXISTS gold.fct_trips CASCADE;

-- Crear tabla padre particionada
CREATE TABLE gold.fct_trips (
    trip_key BIGINT,
    pickup_date_key DATE NOT NULL,
    pu_zone_key INTEGER,
    do_zone_key INTEGER,
    service_type VARCHAR(10),
    payment_type_id INTEGER,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pickup_hour INTEGER,
    pickup_day_of_week INTEGER,
    passenger_count INTEGER,
    trip_distance NUMERIC(10,2),
    trip_duration_minutes NUMERIC(10,2),
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    improvement_surcharge NUMERIC(10,2),
    congestion_surcharge NUMERIC(10,2),
    airport_fee NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    ratecode_id INTEGER,
    store_and_fwd_flag VARCHAR(1),
    source_month VARCHAR(10),
    ingest_ts TIMESTAMP
) PARTITION BY RANGE (pickup_date_key);

-- Crear particiones mensuales para 2022-2025
DO $$
DECLARE
    start_date DATE := '2022-01-01';
    end_date DATE := '2026-01-01';
    current_date_var DATE := start_date;
    partition_name TEXT;
    partition_start TEXT;
    partition_end TEXT;
BEGIN
    WHILE current_date_var < end_date LOOP
        partition_name := 'fct_trips_' || TO_CHAR(current_date_var, 'YYYY_MM');
        partition_start := TO_CHAR(current_date_var, 'YYYY-MM-DD');
        partition_end := TO_CHAR(current_date_var + INTERVAL '1 month', 'YYYY-MM-DD');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold.%I PARTITION OF gold.fct_trips
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, partition_start, partition_end
        );

        current_date_var := current_date_var + INTERVAL '1 month';
    END LOOP;
END $$;

-- ============================================================
-- 2. DIM_ZONE - PARTITION BY HASH (zone_key)
-- ============================================================

DROP TABLE IF EXISTS gold.dim_zone CASCADE;

CREATE TABLE gold.dim_zone (
    zone_key INTEGER NOT NULL,
    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50),
    borough_code INTEGER
) PARTITION BY HASH (zone_key);

-- Crear 4 particiones HASH
CREATE TABLE gold.dim_zone_p0 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE gold.dim_zone_p1 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE gold.dim_zone_p2 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE gold.dim_zone_p3 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- ============================================================
-- 3. DIM_SERVICE_TYPE - PARTITION BY LIST (service_type)
-- ============================================================

DROP TABLE IF EXISTS gold.dim_service_type CASCADE;

CREATE TABLE gold.dim_service_type (
    service_type VARCHAR(10) NOT NULL,
    service_name VARCHAR(50),
    service_description TEXT
) PARTITION BY LIST (service_type);

-- Crear particiones LIST
CREATE TABLE gold.dim_service_type_yellow PARTITION OF gold.dim_service_type FOR VALUES IN ('yellow');
CREATE TABLE gold.dim_service_type_green PARTITION OF gold.dim_service_type FOR VALUES IN ('green');

-- ============================================================
-- 4. DIM_PAYMENT_TYPE - PARTITION BY LIST (payment_type)
-- ============================================================

DROP TABLE IF EXISTS gold.dim_payment_type CASCADE;

CREATE TABLE gold.dim_payment_type (
    payment_type_id INTEGER NOT NULL,
    payment_type VARCHAR(10) NOT NULL,
    payment_type_name VARCHAR(50),
    payment_type_description TEXT
) PARTITION BY LIST (payment_type);

-- Crear particiones LIST
CREATE TABLE gold.dim_payment_type_card PARTITION OF gold.dim_payment_type FOR VALUES IN ('card');
CREATE TABLE gold.dim_payment_type_cash PARTITION OF gold.dim_payment_type FOR VALUES IN ('cash');
CREATE TABLE gold.dim_payment_type_other PARTITION OF gold.dim_payment_type FOR VALUES IN ('other');

-- ============================================================
-- 5. Tablas NO particionadas (dim_date, dim_vendor, dim_rate_code)
-- ============================================================

-- Estas tablas se crean normalmente por dbt sin particionamiento

RAISE NOTICE 'Particionamiento creado exitosamente';
