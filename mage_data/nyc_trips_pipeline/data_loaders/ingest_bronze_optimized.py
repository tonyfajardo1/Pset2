import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import gc

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

DATA_DIR = "/home/src/raw_data"
YEARS = [2022, 2023, 2024, 2025]
MONTHS = list(range(1, 13))

COLUMN_MAPPING = {
    'VendorID': 'vendor_id',
    'tpep_pickup_datetime': 'pickup_datetime',
    'tpep_dropoff_datetime': 'dropoff_datetime',
    'lpep_pickup_datetime': 'pickup_datetime',
    'lpep_dropoff_datetime': 'dropoff_datetime',
    'passenger_count': 'passenger_count',
    'trip_distance': 'trip_distance',
    'RatecodeID': 'ratecode_id',
    'store_and_fwd_flag': 'store_and_fwd_flag',
    'PULocationID': 'pu_location_id',
    'DOLocationID': 'do_location_id',
    'payment_type': 'payment_type',
    'fare_amount': 'fare_amount',
    'extra': 'extra',
    'mta_tax': 'mta_tax',
    'tip_amount': 'tip_amount',
    'tolls_amount': 'tolls_amount',
    'improvement_surcharge': 'improvement_surcharge',
    'total_amount': 'total_amount',
    'congestion_surcharge': 'congestion_surcharge',
    'cbd_congestion_fee': 'cbd_congestion_fee',
    'Airport_fee': 'airport_fee',
    'airport_fee': 'airport_fee',
    'ehail_fee': 'ehail_fee',
    'trip_type': 'trip_type',
}


def get_postgres_engine():
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = int(os.getenv('POSTGRES_PORT', 5432))
    db = os.getenv('POSTGRES_DB', 'nyc_trips')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)


def ensure_bronze_schema(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))


def check_data_exists(engine, service_type, source_month):
    try:
        with engine.begin() as conn:
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM bronze.{service_type}_trips WHERE source_month = :sm"
            ), {'sm': source_month})
            return result.scalar() > 0
    except:
        return False


def process_single_file(engine, service_type, year, month):
    """Procesa UN archivo: lee, transforma, escribe y libera memoria."""
    source_month = f"{year}-{month:02d}"
    filename = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
    filepath = os.path.join(DATA_DIR, filename)

    # Verificar si existe el archivo
    if not os.path.exists(filepath):
        print(f"[NO EXISTE] {filename}")
        return 0, 'missing'

    # Verificar idempotencia
    if check_data_exists(engine, service_type, source_month):
        print(f"[YA EXISTE] {filename} - omitiendo")
        return 0, 'skipped'

    print(f"[PROCESANDO] {filename}")

    # LEER
    df = pd.read_parquet(filepath)
    total_rows = len(df)
    print(f"    Filas leidas: {total_rows:,}")

    # TRANSFORMAR
    df.columns = [COLUMN_MAPPING.get(c, c.lower()) for c in df.columns]
    df['service_type'] = service_type
    df['source_month'] = source_month
    df['ingest_ts'] = datetime.now()

    # ESCRIBIR en chunks
    table_name = f"{service_type}_trips"
    chunk_size = 250000
    rows_written = 0

    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunk.to_sql(
            name=table_name,
            schema="bronze",
            con=engine,
            if_exists='append',
            index=False
        )
        rows_written += len(chunk)
        print(f"    Escritas: {rows_written:,} / {total_rows:,}")
        del chunk
        gc.collect()

    # LIBERAR MEMORIA
    del df
    gc.collect()

    print(f"[OK] {filename}: {total_rows:,} filas")
    return total_rows, 'loaded'


def update_coverage(engine):
    """Actualiza tabla de cobertura."""
    coverage = []
    for year in YEARS:
        for month in MONTHS:
            for st in ['yellow', 'green']:
                sm = f"{year}-{month:02d}"
                fn = f"{st}_tripdata_{year}-{month:02d}.parquet"
                fp = os.path.join(DATA_DIR, fn)

                file_exists = os.path.exists(fp)
                try:
                    with engine.begin() as conn:
                        r = conn.execute(text(
                            f"SELECT COUNT(*) FROM bronze.{st}_trips WHERE source_month = :sm"
                        ), {'sm': sm})
                        rc = r.scalar()
                except:
                    rc = 0

                if rc > 0:
                    status = 'loaded'
                elif file_exists:
                    status = 'failed'
                else:
                    status = 'missing'

                coverage.append({
                    'year_month': sm,
                    'service_type': st,
                    'status': status,
                    'row_count': rc
                })

    pd.DataFrame(coverage).to_sql(
        name='coverage', schema='bronze', con=engine,
        if_exists='replace', index=False
    )
    print("\n[COVERAGE] Tabla actualizada")


@data_loader
def ingest_bronze_optimized(*args, **kwargs):
    """
    Pipeline optimizado: procesa UN archivo a la vez para evitar problemas de memoria.
    """
    engine = get_postgres_engine()
    ensure_bronze_schema(engine)

    total_inserted = 0
    total_skipped = 0
    total_missing = 0

    for year in YEARS:
        for month in MONTHS:
            for service_type in ['yellow', 'green']:
                try:
                    rows, status = process_single_file(engine, service_type, year, month)
                    if status == 'loaded':
                        total_inserted += rows
                    elif status == 'skipped':
                        total_skipped += 1
                    elif status == 'missing':
                        total_missing += 1
                except Exception as e:
                    print(f"[ERROR] {service_type} {year}-{month:02d}: {e}")

    update_coverage(engine)
    engine.dispose()

    print(f"\n{'='*50}")
    print(f"RESUMEN FINAL:")
    print(f"  - Filas insertadas: {total_inserted:,}")
    print(f"  - Archivos omitidos (ya existian): {total_skipped}")
    print(f"  - Archivos no encontrados: {total_missing}")
    print(f"{'='*50}")

    return {'inserted': total_inserted, 'skipped': total_skipped, 'missing': total_missing}


@test
def test_output(output, *args) -> None:
    assert output is not None, 'El output no puede ser None'
