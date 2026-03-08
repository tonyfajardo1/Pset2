import os
import pandas as pd
import requests
from datetime import datetime
import time

DATA_DIR = "C:/Users/Tony/Documents/Data mining/Deber 2/raw_data"
os.makedirs(DATA_DIR, exist_ok=True)

BASE_URL_YELLOW = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
BASE_URL_GREEN = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"

years = [2022, 2023, 2024]
months = range(1, 13)

def download_parquet(url, filepath):
    if os.path.exists(filepath):
        print(f"Ya existe: {filepath}")
        return True
    try:
        print(f"Descargando: {url}")
        response = requests.get(url, timeout=120)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            print(f"Guardado: {filepath}")
            return True
        else:
            print(f"Error {response.status_code}: {url}")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

coverage = []

for year in years:
    for month in months:
        month_str = f"{month:02d}"
        
        yellow_url = BASE_URL_YELLOW.format(year=year, month=month_str)
        yellow_file = os.path.join(DATA_DIR, f"yellow_tripdata_{year}-{month_str}.parquet")
        yellow_ok = download_parquet(yellow_url, yellow_file)
        coverage.append({
            'year_month': f"{year}-{month_str}",
            'service_type': 'yellow',
            'status': 'loaded' if yellow_ok else 'failed',
            'file': yellow_file if yellow_ok else None
        })
        
        time.sleep(0.5)
        
        green_url = BASE_URL_GREEN.format(year=year, month=month_str)
        green_file = os.path.join(DATA_DIR, f"green_tripdata_{year}-{month_str}.parquet")
        green_ok = download_parquet(green_url, green_file)
        coverage.append({
            'year_month': f"{year}-{month_str}",
            'service_type': 'green',
            'status': 'loaded' if green_ok else 'failed',
            'file': green_file if green_ok else None
        })
        
        time.sleep(0.5)

coverage_df = pd.DataFrame(coverage)
coverage_df.to_csv(os.path.join(DATA_DIR, "coverage.csv"), index=False)
print("\n=== Cobertura ===")
print(coverage_df.groupby(['year_month', 'service_type'])['status'].first().unstack())
