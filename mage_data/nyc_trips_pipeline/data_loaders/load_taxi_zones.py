"""
Data Loader: Carga taxi_zone_lookup a Bronze.
Este archivo carga el dataset complementario obligatorio.
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from mage_ai.data_preparation.shared.secrets import get_secret_value


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
    """Carga taxi_zone_lookup a bronze.taxi_zone_lookup."""
    engine = get_postgres_engine()
    ensure_bronze_schema()

    filepath = "/home/src/raw_data/taxi_zone_lookup.csv"
    print(f"[LOAD] Leyendo {filepath}")

    df = pd.read_csv(filepath)
    print(f"[LOAD] {len(df)} filas leidas")

    df['ingest_ts'] = datetime.now()

    df.to_sql(
        name='taxi_zone_lookup',
        schema='bronze',
        con=engine,
        if_exists='replace',
        index=False
    )

    print(f"[OK] bronze.taxi_zone_lookup cargada con {len(df)} filas")
    engine.dispose()
    return len(df)


@data_loader
def ingest_taxi_zones(*args, **kwargs):
    """
    Data loader para taxi_zone_lookup.
    Dataset complementario obligatorio segun PDF Seccion 3.
    """
    rows = load_taxi_zones()
    return {'rows_loaded': rows, 'table': 'bronze.taxi_zone_lookup'}
