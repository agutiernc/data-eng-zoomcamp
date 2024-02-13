-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE still-totality-411905.ny_taxi.green_taxi_2022_non_partitioned
AS
SELECT * FROM still-totality-411905.ny_taxi.green_taxi_2022_external;

-- question 2
SELECT COUNT(DISTINCT PULocationID) AS PULocationIDCount
FROM ny_taxi.green_taxi_2022_non_partitioned;

SELECT COUNT(DISTINCT PULocationID) AS PULocationIDCount
FROM ny_taxi.green_taxi_2022;

-- question 3
SELECT COUNT(fare_amount) AS fare_amount_zero
FROM ny_taxi.green_taxi_2022_non_partitioned
WHERE fare_amount=0;

-- question 4
CREATE OR REPLACE TABLE ny_taxi.green_taxi_2022_partitioned_clustered
PARTITION BY lpep_pickup_date
CLUSTER BY PUlocationID
AS
SELECT * FROM ny_taxi.green_taxi_2022_external;

-- question 5

SELECT DISTINCT PULocationID
FROM ny_taxi.green_taxi_2022_non_partitioned
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT PULocationID
FROM ny_taxi.green_taxi_2022_partitioned_clustered
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30';
