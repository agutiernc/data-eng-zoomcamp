import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{}.parquet'
    
    # Pad single digit months with leading zero
    months = [str(month).zfill(2) for month in range(1,  13)]
	
	# create urls with specified month
    urls = [base_url.format(month) for month in months]

    dfs = [pd.read_parquet(url) for url in urls]

  # Convert integer columns to datetime
    for df in dfs:
        df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
        df['lpep_dropoff_date'] = df['lpep_dropoff_datetime'].dt.date

    return pd.concat(dfs, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
