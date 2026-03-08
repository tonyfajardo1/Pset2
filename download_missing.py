import os
import requests
import time

DATA_DIR = "C:/Users/Tony/Documents/Data mining/Deber 2/raw_data"

alt_urls = [
    ("2024-06", "yellow", "https://nyc-tlc.s3.us-east-1.amazonaws.com/yellow/yellow_tripdata_2024-06.parquet"),
    ("2024-06", "green", "https://nyc-tlc.s3.us-east-1.amazonaws.com/green/green_tripdata_2024-06.parquet"),
]

for year_month, service_type, url in alt_urls:
    year, month = year_month.split("-")
    filename = f"{service_type}_tripdata_{year}-{month}.parquet"
    filepath = os.path.join(DATA_DIR, filename)
    
    if os.path.exists(filepath):
        print(f"Ya existe: {filename}")
        continue
    
    print(f"Intentando: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=120)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            print(f"  -> DESCARGADO: {filename}")
        else:
            print(f"  -> Error {response.status_code}")
    except Exception as e:
        print(f"  -> Error: {e}")
    
    time.sleep(1)
