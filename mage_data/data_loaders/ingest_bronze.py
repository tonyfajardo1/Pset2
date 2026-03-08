import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

DATA_DIR = "/home/src/raw_data"

def get_postgres_credentials():
    return {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'db': os.getenv('POSTGRES_DB', 'nyc_trips'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
    }

def get_postgres_engine():
    creds = get_postgres_credentials()
    conn_str = f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['db']}"
    return create_engine(conn_str)

YEARS = [2022, 2023, 2024, 2025]
MONTHS = list(range(1, 13))

def ensure_bronze_schema():
    engine = get_postgres_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
    engine.dispose()

def get_source_month(year, month):
    return f"{year}-{month:02d}"

def load_parquet_to_bronze(service_type, year, month):
    source_month = get_source_month(year, month)
    filename = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
    filepath = os.path.join(DATA_DIR, filename)
    
    if not os.path.exists(filepath):
        print(f"Archivo no encontrado: {filepath}")
        return 0
    
    df = pd.read_parquet(filepath)
    
    column_mapping = {
        'VendorID': 'vendor_id',
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
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
        'lpep_pickup_datetime': 'pickup_datetime',
        'lpep_dropoff_datetime': 'dropoff_datetime',
        'cbd_congestion_fee': 'cbd_congestion_fee',
        'Airport_fee': 'airport_fee',
    }
    
    df.columns = [column_mapping.get(c, c.lower()) for c in df.columns]
    
    df['service_type'] = service_type
    df['source_month'] = source_month
    df['ingest_ts'] = datetime.now()
    
    engine = get_postgres_engine()
    table_name = f"bronze.{service_type}_trips"
    
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    
    df.to_sql(
        name=f"{service_type}_trips",
        schema="bronze",
        con=engine,
        if_exists="replace",
        index=False
    )
    
    count = len(df)
    engine.dispose()
    
    print(f"Cargado {service_type} {year}-{month:02d}: {count:,} filas")
    return count

def update_coverage_table():
    engine = get_postgres_engine()
    
    coverage_data = []
    for year in YEARS:
        for month in MONTHS:
            for service_type in ['yellow', 'green']:
                source_month = get_source_month(year, month)
                filename = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
                filepath = os.path.join(DATA_DIR, filename)
                
                status = 'loaded' if os.path.exists(filepath) else 'missing'
                
                if status == 'loaded':
                    try:
                        with engine.begin() as conn:
                            result = conn.execute(text(
                                f"SELECT COUNT(*) FROM bronze.{service_type}_trips WHERE source_month = '{source_month}'"
                            ))
                            row_count = result.scalar()
                    except:
                        row_count = 0
                else:
                    row_count = 0
                
                coverage_data.append({
                    'year_month': source_month,
                    'service_type': service_type,
                    'status': status,
                    'row_count': row_count
                })
    
    coverage_df = pd.DataFrame(coverage_data)
    coverage_df.to_sql(
        name='coverage',
        schema='bronze',
        con=engine,
        if_exists='replace',
        index=False
    )
    
    engine.dispose()
    print("Tabla de cobertura actualizada")
    return coverage_df

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def ingest_bronze(*args, **kwargs):
    ensure_bronze_schema()
    
    total = 0
    for year in YEARS:
        for month in MONTHS:
            for service_type in ['yellow', 'green']:
                try:
                    count = load_parquet_to_bronze(service_type, year, month)
                    total += count
                except Exception as e:
                    print(f"Error cargando {service_type} {year}-{month}: {e}")
    
    update_coverage_table()
    
    print(f"\nTotal ingestado: {total:,} filas")
    return total
