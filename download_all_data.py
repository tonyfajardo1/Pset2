import os
import requests
import time
import pandas as pd

DATA_DIR = "C:/Users/Tony/Documents/Data mining/Deber 2/raw_data"
os.makedirs(DATA_DIR, exist_ok=True)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month}.parquet"

years = [2022, 2023, 2024, 2025]
months = range(1, 13)
services = ['yellow', 'green']

results = []

for year in years:
    for month in months:
        month_str = f"{month:02d}"
        
        for service in services:
            url = BASE_URL.format(service=service, year=year, month=month_str)
            filename = f"{service}_tripdata_{year}-{month_str}.parquet"
            filepath = os.path.join(DATA_DIR, filename)
            
            if os.path.exists(filepath):
                print(f"Ya existe: {filename}")
                results.append({'year_month': f"{year}-{month_str}", 'service': service, 'status': 'exists'})
                continue
            
            print(f"Descargando: {filename}")
            try:
                response = requests.get(url, timeout=120)
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    print(f"  -> OK: {filename}")
                    results.append({'year_month': f"{year}-{month_str}", 'service': service, 'status': 'loaded'})
                else:
                    print(f"  -> Error {response.status_code}: {filename}")
                    results.append({'year_month': f"{year}-{month_str}", 'service': service, 'status': 'failed'})
            except Exception as e:
                print(f"  -> Error: {e}")
                results.append({'year_month': f"{year}-{month_str}", 'service': service, 'status': 'error'})
            
            time.sleep(0.3)

results_df = pd.DataFrame(results)
results_df.to_csv(os.path.join(DATA_DIR, "coverage_full.csv"), index=False)

print("\n=== RESUMEN ===")
print(results_df.groupby('status').size())
print("\n=== COBERTURA ===")
pivot = results_df.pivot_table(index='year_month', columns='service', values='status', aggfunc='first')
print(pivot)
