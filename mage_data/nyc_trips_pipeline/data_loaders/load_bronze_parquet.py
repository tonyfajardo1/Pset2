import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value


DATA_DIR = "/home/src/raw_data"
YEARS = [2022, 2023, 2024, 2025]
MONTHS = list(range(1, 13))


def get_postgres_engine():
    host = get_secret_value('POSTGRES_HOST') or os.getenv('POSTGRES_HOST', 'postgres')
    port = int(get_secret_value('POSTGRES_PORT') or os.getenv('POSTGRES_PORT', 5432))
    db = get_secret_value('POSTGRES_DB') or os.getenv('POSTGRES_DB', 'nyc_trips')
    user = get_secret_value('POSTGRES_USER') or os.getenv('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
    if not user or not password:
        raise ValueError('POSTGRES_USER y POSTGRES_PASSWORD deben venir de Mage Secrets o variables de entorno.')
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)


def ensure_bronze_schema():
    engine = get_postgres_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
    engine.dispose()


def load_taxi_zones():
    """Carga taxi_zone_lookup a bronze.taxi_zone_lookup (solo si no existe)."""
    engine = get_postgres_engine()
    ensure_bronze_schema()

    with engine.connect() as conn:
        table_exists = conn.execute(
            text("SELECT to_regclass('bronze.taxi_zone_lookup') IS NOT NULL")
        ).scalar()
        count = 0
        if table_exists:
            result = conn.execute(text("SELECT COUNT(*) FROM bronze.taxi_zone_lookup"))
            count = result.scalar()

    if count and count > 0:
        print("[SKIP] bronze.taxi_zone_lookup ya existe")
        engine.dispose()
        return count

    filepath = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")
    print(f"[LOAD] Leyendo {filepath}")

    df = pd.read_csv(filepath)
    df['ingest_ts'] = datetime.now()

    df.to_sql(
        name='taxi_zone_lookup',
        schema='bronze',
        con=engine,
        if_exists='replace',
        index=False
    )

    print(f"[OK] bronze.taxi_zone_lookup cargada ({len(df)} filas)")
    engine.dispose()
    return len(df)


@data_loader
def load_parquet_files(*args, **kwargs):
    """
    Carga taxi_zone_lookup y todos los archivos parquet de yellow y green taxis.
    Retorna una lista de diccionarios con la informacion de cada archivo.
    """
    # Paso 1: Cargar taxi_zone_lookup si no existe
    load_taxi_zones()

    # Paso 2: Listar archivos parquet a procesar
    files_to_process = []

    for year in YEARS:
        for month in MONTHS:
            for service_type in ['yellow', 'green']:
                source_month = f"{year}-{month:02d}"
                filename = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
                filepath = os.path.join(DATA_DIR, filename)

                if os.path.exists(filepath):
                    files_to_process.append({
                        'filepath': filepath,
                        'filename': filename,
                        'service_type': service_type,
                        'source_month': source_month,
                        'year': year,
                        'month': month
                    })
                    print(f"[ENCONTRADO] {filename}")
                else:
                    print(f"[NO EXISTE] {filename}")

    print(f"\nTotal archivos a procesar: {len(files_to_process)}")
    return files_to_process


@test
def test_output(output, *args) -> None:
    assert output is not None, 'No se encontraron archivos'
    assert len(output) > 0, 'La lista de archivos esta vacia'
