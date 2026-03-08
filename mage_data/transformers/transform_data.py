import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Transform the data:
    1. Create trip_duration_minutes column
    2. Filter trips with passenger_count > 1
    """
    df_transformed = df.copy()
    
    df_transformed['tpep_pickup_datetime'] = pd.to_datetime(df_transformed['tpep_pickup_datetime'])
    df_transformed['tpep_dropoff_datetime'] = pd.to_datetime(df_transformed['tpep_dropoff_datetime'])
    
    df_transformed['trip_duration_minutes'] = (
        df_transformed['tpep_dropoff_datetime'] - df_transformed['tpep_pickup_datetime']
    ).dt.total_seconds() / 60
    
    df_transformed = df_transformed[df_transformed['passenger_count'] > 1]
    
    return df_transformed
