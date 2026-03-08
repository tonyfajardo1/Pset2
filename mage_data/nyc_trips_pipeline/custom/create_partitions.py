"""
Bloque Custom: Crea las tablas particionadas en PostgreSQL
Debe ejecutarse ANTES de dbt run --select gold

Particiones:
- fct_trips: RANGE por pickup_date_key (mensual 2022-2025)
- dim_zone: HASH por zone_key (4 particiones)
- dim_service_type: LIST por service_type (yellow, green)
- dim_payment_type: LIST por payment_type (card, cash, other)
"""
from mage_ai.data_preparation.shared.secrets import get_secret_value
import psycopg2
import os

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def create_partitions(*args, **kwargs):
    """
    Crea las estructuras de tablas particionadas en gold schema
    """
    # Obtener credenciales de Mage Secrets
    host = get_secret_value('POSTGRES_HOST') or os.getenv('POSTGRES_HOST', 'postgres')
    port = int(get_secret_value('POSTGRES_PORT') or os.getenv('POSTGRES_PORT', 5432))
    db = get_secret_value('POSTGRES_DB') or os.getenv('POSTGRES_DB', 'nyc_trips')
    user = get_secret_value('POSTGRES_USER') or os.getenv('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
    if not user or not password:
        raise ValueError('POSTGRES_USER y POSTGRES_PASSWORD deben venir de Mage Secrets o variables de entorno.')

    conn = psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=password
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("=" * 60)
    print("CREANDO TABLAS PARTICIONADAS EN GOLD")
    print("=" * 60)

    # 1. Crear schema gold
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    print("Schema gold creado/verificado")

    # 2. FCT_TRIPS - RANGE partitioning
    print("\n>>> Creando fct_trips (RANGE partition)...")
    cur.execute("DROP TABLE IF EXISTS gold.fct_trips CASCADE;")
    cur.execute("""
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
    """)

    # Crear particiones mensuales 2022-2025
    from datetime import date
    from dateutil.relativedelta import relativedelta

    start = date(2022, 1, 1)
    end = date(2026, 1, 1)
    current = start
    partition_count = 0

    while current < end:
        next_month = current + relativedelta(months=1)
        partition_name = f"fct_trips_{current.strftime('%Y_%m')}"
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS gold.{partition_name}
            PARTITION OF gold.fct_trips
            FOR VALUES FROM ('{current}') TO ('{next_month}');
        """)
        partition_count += 1
        current = next_month

    print(f"   fct_trips: {partition_count} particiones mensuales creadas")

    # 3. DIM_ZONE - HASH partitioning
    print("\n>>> Creando dim_zone (HASH partition)...")
    cur.execute("DROP TABLE IF EXISTS gold.dim_zone CASCADE;")
    cur.execute("""
        CREATE TABLE gold.dim_zone (
            zone_key INTEGER NOT NULL,
            borough VARCHAR(50),
            zone_name VARCHAR(100),
            service_zone VARCHAR(50),
            borough_code INTEGER
        ) PARTITION BY HASH (zone_key);
    """)

    for i in range(4):
        cur.execute(f"""
            CREATE TABLE gold.dim_zone_p{i}
            PARTITION OF gold.dim_zone
            FOR VALUES WITH (MODULUS 4, REMAINDER {i});
        """)
    print("   dim_zone: 4 particiones HASH creadas")

    # 4. DIM_SERVICE_TYPE - LIST partitioning
    print("\n>>> Creando dim_service_type (LIST partition)...")
    cur.execute("DROP TABLE IF EXISTS gold.dim_service_type CASCADE;")
    cur.execute("""
        CREATE TABLE gold.dim_service_type (
            service_type VARCHAR(10) NOT NULL,
            service_name VARCHAR(50),
            service_description TEXT
        ) PARTITION BY LIST (service_type);
    """)
    cur.execute("CREATE TABLE gold.dim_service_type_yellow PARTITION OF gold.dim_service_type FOR VALUES IN ('yellow');")
    cur.execute("CREATE TABLE gold.dim_service_type_green PARTITION OF gold.dim_service_type FOR VALUES IN ('green');")
    print("   dim_service_type: 2 particiones LIST creadas (yellow, green)")

    # 5. DIM_PAYMENT_TYPE - LIST partitioning
    print("\n>>> Creando dim_payment_type (LIST partition)...")
    cur.execute("DROP TABLE IF EXISTS gold.dim_payment_type CASCADE;")
    cur.execute("""
        CREATE TABLE gold.dim_payment_type (
            payment_type_id INTEGER NOT NULL,
            payment_type VARCHAR(10) NOT NULL,
            payment_type_name VARCHAR(50),
            payment_type_description TEXT
        ) PARTITION BY LIST (payment_type);
    """)
    cur.execute("CREATE TABLE gold.dim_payment_type_card PARTITION OF gold.dim_payment_type FOR VALUES IN ('card');")
    cur.execute("CREATE TABLE gold.dim_payment_type_cash PARTITION OF gold.dim_payment_type FOR VALUES IN ('cash');")
    cur.execute("CREATE TABLE gold.dim_payment_type_other PARTITION OF gold.dim_payment_type FOR VALUES IN ('other');")
    print("   dim_payment_type: 3 particiones LIST creadas (card, cash, other)")

    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print("PARTICIONAMIENTO COMPLETADO EXITOSAMENTE")
    print("=" * 60)

    return {
        "status": "success",
        "partitions_created": {
            "fct_trips": f"{partition_count} RANGE partitions",
            "dim_zone": "4 HASH partitions",
            "dim_service_type": "2 LIST partitions",
            "dim_payment_type": "3 LIST partitions"
        }
    }


@test
def test_output(output, *args) -> None:
    assert output is not None
    assert output.get('status') == 'success'
