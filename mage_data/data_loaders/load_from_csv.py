import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data_from_csv(*args, **kwargs) -> pd.DataFrame:
    """
    Load NYC Yellow Taxi trips data from CSV file.
    """
    csv_path = '/home/src/yellow_tripdata_2024-01.csv'
    return pd.read_csv(csv_path)
