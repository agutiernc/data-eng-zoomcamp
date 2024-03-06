import sys
import dlt
import os
import io
import requests
from requests.exceptions import HTTPError
import pandas as pd
# from google.cloud import storage

os.environ["SCHEMA__NAMING"] = "direct"

# table schemas
table_schema_green = {
  'VendorID': pd.Int64Dtype(),
  'store_and_fwd_flag': str,
  'RatecodeID': pd.Int64Dtype(),
  'PULocationID': pd.Int64Dtype(),
  'DOLocationID': pd.Int64Dtype(),
  'passenger_count': pd.Int64Dtype(),
  'trip_distance': float,
  'fare_amount': float,
  'extra': float,
  'mta_tax': float,
  'tip_amount': float,
  'tolls_amount': float,
  'ehail_fee': float,
  'improvement_surcharge': float,
  'total_amount': float,
  'congestion_surcharge': float,
  'trip_type': pd.Int64Dtype(),
  'payment_type': pd.Int64Dtype(),
}

table_schema_yellow = {
  'VendorID': pd.Int64Dtype(),
  'passenger_count': pd.Int64Dtype(),
  'trip_distance': float,
  'RatecodeID': pd.Int64Dtype(),
  'store_and_fwd_flag': str,
  'PULocationID': pd.Int64Dtype(),
  'DOLocationID': pd.Int64Dtype(),
  'payment_type': pd.Int64Dtype(),
  'fare_amount': float,
  'extra': float,
  'mta_tax': float,
  'tip_amount': float,
  'tolls_amount': float,
  'improvement_surcharge': float,
  'total_amount': float,
  'congestion_surcharge': float
}

table_fhv = {
  'dispatching_base_num': str,
  'PULocationID': float,
  'DOLocationID': float,
  'SR_Flag': str,
  'Affiliated_base_number': str
}

# parse dates
parse_green_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
parse_yellow_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime']
parse_fhv_dates=['pickup_datetime', 'dropOff_datetime']

# Initialize the dlt pipeline --- change dataset_name value
pipeline = dlt.pipeline(pipeline_name="web_to_gcs", destination="filesystem", dataset_name="fhv_data")

def download_taxi_data(service, year):
  schema = table_schema_green if service == 'green' else table_schema_yellow if service == 'yellow' else table_fhv
  parse_dates = parse_green_dates if service == 'green' else parse_yellow_dates if service == 'yellow' else parse_fhv_dates

  init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

  for i in range(1, 13):
    month = str(i).zfill(2)
    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
    request_url = f"{init_url}{service}/{file_name}"
    
    try:
      response = requests.get(request_url)
      response.raise_for_status()

      print(f"Downloaded: {file_name}")

      # yield response.content # uncomment if large data set fails

      yield pd.read_csv(io.BytesIO(response.content), compression='gzip', dtype=schema, parse_dates=parse_dates)

      print(f"Read CSV: {file_name}")
    except HTTPError as http_err:
      print(f"HTTP error occurred: {http_err}")
      sys.exit(1) # terminate on error
    except Exception as err:
      print(f"An error occurred: {err}")
      # sys.exit(1) # terminate on error

# Run the pipeline for a specific service and year -- change necessary values
pipeline.run(download_taxi_data("fhv", "2019"), table_name="fhv_2019", loader_file_format="parquet", write_disposition="append", 
    schema_contract={"data_type": "discard_value", "columns": "evolve"})

print(f"loaded parquet file to GCS")
