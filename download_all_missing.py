import os
import requests
import time

DATA_DIR = "C:/Users/Tony/Documents/Data mining/Deber 2/raw_data"

missing = [
    ("2024-06", "green"),
    ("2024-07", "yellow"), ("2024-07", "green"),
    ("2024-08", "yellow"), ("2024-08", "green"),
    ("2024-09", "yellow"), ("2024-09", "green"),
    ("2024-10", "yellow"), ("2024-10", "green"),
    ("2024-11", "yellow"), ("2024-11", "green"),
    ("2024-12", "yellow"), ("2024-12", "green"),
]

alt_sources = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month}.parquet",
    "https://nyc-tlc.s3.us-east-1.amazonaws.com/{service}/{service}_tripdata_{year}-{month}.parquet",
    "https://s3.amazonaws.com/nyc-tlc/trip+data/{service}_tripdata_{year}-{month}.parquet",
]

for year_month, service_type in missing:
    year, month = year_month.split("-")
    filename = f"{service_type}_tripdata_{year}-{month}.parquet"
    filepath = os.path.join(DATA_DIR, filename)
    
    if os.path.exists(filepath):
        print(f"Ya existe: {filename}")
        continue
    
    downloaded = False
    for source_template in alt_sources:
        url = source_template.format(service=service_type, year=year, month=month)
        print(f"Intentando: {filename} -> {url[:60]}...")
        try:
            response = requests.get(url, timeout=60, headers={'User-Agent': 'Mozilla/5.0'})
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                print(f"  -> EXITO: {filename}")
                downloaded = True
                break
            else:
                print(f"  -> {response.status_code}")
        except Exception as e:
            print(f"  -> Error: {str(e)[:30]}")
        time.sleep(0.5)
    
    if not downloaded:
        print(f"  -> NO SE PUDO: {filename}")
