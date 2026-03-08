import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import gc

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Importar funcion para obtener Mage Secrets
from mage_ai.data_preparation.shared.secrets import get_secret_value


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
    """
    Obtiene conexion a PostgreSQL usando Mage Secrets.
    Los secrets se configuran en: Mage UI -> Settings -> Secrets
    """
    # Obtener credenciales desde Mage Secrets (con fallback a env vars)
    host = get_secret_value('POSTGRES_HOST') or os.getenv('POSTGRES_HOST', 'postgres')
    port = int(get_secret_value('POSTGRES_PORT') or os.getenv('POSTGRES_PORT', 5432))
    db = get_secret_value('POSTGRES_DB') or os.getenv('POSTGRES_DB', 'nyc_trips')
    user = get_secret_value('POSTGRES_USER') or os.getenv('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
    if not user or not password:
        raise ValueError('POSTGRES_USER y POSTGRES_PASSWORD deben venir de Mage Secrets o variables de entorno.')

    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    print(f"[DB] Conectando a {host}:{port}/{db} como {user}")
    return create_engine(conn_str, pool_pre_ping=True)


def ensure_bronze_schema(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))


def check_exists_fast(engine, service_type, source_month):
    """Verificacion rapida con LIMIT 1 (no COUNT)."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT 1 FROM bronze.{service_type}_trips WHERE source_month = :sm LIMIT 1"
            ), {'sm': source_month})
            return result.fetchone() is not None
    except:
        return False


def get_row_count(engine, service_type, source_month):
    """Obtiene conteo real por mes y tipo de servicio."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM bronze.{service_type}_trips "
                    "WHERE source_month = :sm"
                ),
                {'sm': source_month},
            )
            return int(result.scalar() or 0)
    except Exception:
        return 0


def delete_existing_data(engine, service_type, source_month):
    """
    Elimina datos existentes para permitir re-ejecucion idempotente.
    Cumple requisito: DELETE FROM bronze... WHERE source_month = ... AND service_type=... antes de insertar
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text(
                f"DELETE FROM bronze.{service_type}_trips WHERE source_month = :sm"
            ), {'sm': source_month})
            deleted = result.rowcount
            if deleted > 0:
                print(f"    [IDEMPOTENCIA] Eliminadas {deleted} filas existentes para {source_month}")
            return deleted
    except Exception as e:
        print(f"    [WARNING] Error en DELETE: {e}")
        return 0


def process_single_file(engine, file_info):
    """
    Procesa UN archivo: lee, transforma, escribe y libera memoria.
    """
    filepath = file_info['filepath']
    service_type = file_info['service_type']
    source_month = file_info['source_month']
    filename = file_info['filename']

    print(f"\n[PROCESANDO] {filename}")

    # 1. LEER
    df = pd.read_parquet(filepath)
    total_rows = len(df)
    print(f"    Filas leidas: {total_rows:,}")

    # 2. TRANSFORMAR
    df.columns = [COLUMN_MAPPING.get(c, c.lower()) for c in df.columns]
    df['service_type'] = service_type
    df['source_month'] = source_month
    df['ingest_ts'] = datetime.now()

    # 3. ESCRIBIR en chunks
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

    # 4. LIBERAR MEMORIA
    del df
    gc.collect()

    print(f"[OK] {filename}: {total_rows:,} filas")
    return total_rows


def update_coverage_table(engine):
    """Actualiza tabla de cobertura con conteo real y estados del PDF."""
    DATA_DIR = "/home/src/raw_data"
    YEARS = [2022, 2023, 2024, 2025]
    MONTHS = list(range(1, 13))

    coverage = []
    for year in YEARS:
        for month in MONTHS:
            for st in ['yellow', 'green']:
                sm = f"{year}-{month:02d}"
                fn = f"{st}_tripdata_{year}-{month:02d}.parquet"
                fp = os.path.join(DATA_DIR, fn)

                file_exists = os.path.exists(fp)
                rc = get_row_count(engine, st, sm)

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


@data_exporter
def export_to_postgres(files_to_load, *args, **kwargs):
    """
    EXPORTER OPTIMIZADO:
    - Procesa UN archivo a la vez
    - Lee, transforma y escribe cada archivo
    - Libera memoria despues de cada archivo
    """
    if not files_to_load:
        print("No hay archivos nuevos para cargar")
        return {'total_inserted': 0, 'files_processed': 0}

    engine = get_postgres_engine()
    ensure_bronze_schema(engine)

    total_inserted = 0
    files_processed = 0
    files_skipped = 0

    for file_info in files_to_load:
        # Idempotencia: ELIMINAR datos existentes antes de insertar
        # Cumple requisito PDF: DELETE FROM bronze... WHERE source_month = ... antes de insertar
        delete_existing_data(engine, file_info['service_type'], file_info['source_month'])

        try:
            rows = process_single_file(engine, file_info)
            total_inserted += rows
            files_processed += 1
        except Exception as e:
            print(f"[ERROR] {file_info['filename']}: {e}")

    # Actualizar tabla de cobertura
    update_coverage_table(engine)
    engine.dispose()

    print(f"\n{'='*50}")
    print(f"RESUMEN FINAL:")
    print(f"  - Archivos procesados: {files_processed}")
    print(f"  - Total filas insertadas: {total_inserted:,}")
    print(f"{'='*50}")

    return {
        'total_inserted': total_inserted,
        'files_processed': files_processed
    }


@test
def test_output(output, *args) -> None:
    assert output is not None, 'El output no puede ser None'
