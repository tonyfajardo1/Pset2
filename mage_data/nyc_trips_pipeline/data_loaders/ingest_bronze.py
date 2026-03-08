import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import gc

DATA_DIR = "/home/src/raw_data"

def get_postgres_credentials():
    return {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'db': os.getenv('POSTGRES_DB', 'nyc_trips'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }

def get_postgres_engine():
    creds = get_postgres_credentials()
    if not creds['user'] or not creds['password']:
        raise ValueError('POSTGRES_USER y POSTGRES_PASSWORD deben definirse en entorno/Mage Secrets.')
    conn_str = f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['db']}"
    return create_engine(conn_str, pool_pre_ping=True)

YEARS = [2022, 2023, 2024, 2025]
MONTHS = list(range(1, 13))

def ensure_bronze_schema():
    engine = get_postgres_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
    engine.dispose()

def get_source_month(year, month):
    return f"{year}-{month:02d}"

def check_exists(service_type, year, month):
    source_month = get_source_month(year, month)
    engine = get_postgres_engine()
    try:
        with engine.begin() as conn:
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM bronze.{service_type}_trips WHERE source_month = '{source_month}'"
            ))
            count = result.scalar()
    except:
        count = 0
    engine.dispose()
    return count > 0

def load_single_file(service_type, year, month):
    source_month = get_source_month(year, month)
    filename = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
    filepath = os.path.join(DATA_DIR, filename)
    
    if not os.path.exists(filepath):
        print(f">>> [NO EXISTE] {filename}")
        return 0
    
    # Verificar si ya existe
    if check_exists(service_type, year, month):
        print(f">>> [YA EXISTE] {service_type} {source_month} - omitiendo")
        return 0
    
    print(f">>> {filename}")
    
    engine = get_postgres_engine()
    
    df = pd.read_parquet(filepath)
    print(f">>>   rows: {len(df):,}")
    
    column_mapping = {
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
    }
    
    df.columns = [column_mapping.get(c, c.lower()) for c in df.columns]
    df['service_type'] = service_type
    df['source_month'] = source_month
    df['ingest_ts'] = datetime.now()
    
    chunk_size = 500000
    total = 0
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunk.to_sql(
            name=f"{service_type}_trips",
            schema="bronze",
            con=engine,
            if_exists='append',
            index=False
        )
        total += len(chunk)
        print(f">>>   {total:,} / {len(df):,}")
        del chunk
        gc.collect()
    
    engine.dispose()
    del df
    gc.collect()
    
    print(f">>> OK: {total:,} filas")
    return total

def update_coverage():
    engine = get_postgres_engine()
    coverage = []
    for year in YEARS:
        for month in MONTHS:
            for st in ['yellow', 'green']:
                ym = get_source_month(year, month)
                f = os.path.join(DATA_DIR, f"{st}_tripdata_{year}-{month:02d}.parquet")
                status = 'loaded' if os.path.exists(f) else 'missing'
                try:
                    with engine.begin() as conn:
                        r = conn.execute(text(f"SELECT COUNT(*) FROM bronze.{st}_trips WHERE source_month = '{ym}'"))
                        rc = r.scalar()
                except:
                    rc = 0
                coverage.append({'year_month': ym, 'service_type': st, 'status': status, 'row_count': rc})
    pd.DataFrame(coverage).to_sql(name='coverage', schema='bronze', con=engine, if_exists='replace', index=False)
    engine.dispose()

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def ingest_bronze(*args, **kwargs):
    ensure_bronze_schema()
    total = 0
    for year in YEARS:
        for month in MONTHS:
            for st in ['yellow', 'green']:
                try:
                    total += load_single_file(st, year, month)
                except Exception as e:
                    print(f">>> ERROR: {e}")
    update_coverage()
    print(f">>> TOTAL: {total:,}")
    return total
