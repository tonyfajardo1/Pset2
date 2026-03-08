import pandas as pd
from mage_ai.io.postgres import Postgres
from mage_ai.io.io_config import IOConfig

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: pd.DataFrame, **kwargs) -> None:
    """
    Export transformed data to PostgreSQL database.
    Table: nyc_yellow_trips
    """
    config = IOConfig.from_file('/home/src/io_config.yaml')
    
    with Postgres.with_config(config) as loader:
        loader.export(
            df,
            'nyc_yellow_trips',
            if_exists='replace',
            index=False
        )
