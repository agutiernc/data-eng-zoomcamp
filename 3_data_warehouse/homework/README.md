# Week 3 Homework

> ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??

**Answer**

`840,402`

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

**Answer**

`0 MB for the External Table and 6.41MB for the Materialized Table`

```SQL
-- materialized table
SELECT COUNT(DISTINCT PULocationID) AS PULocationIDCount
FROM ny_taxi.green_taxi_2022_non_partitioned;

-- external table
SELECT COUNT(DISTINCT PULocationID) AS PULocationIDCount
FROM ny_taxi.green_taxi_2022_external;
```

## Question 3:

How many records have a fare_amount of 0?

**Answer**

`1,622`

```SQL
SELECT COUNT(fare_amount) AS fare_amount_zero
FROM ny_taxi.green_taxi_2022_non_partitioned
WHERE fare_amount=0;
```

## Question 4:

What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

**Answer**

`Partition by lpep_pickup_datetime Cluster on PUlocationID`

```SQL
CREATE OR REPLACE TABLE ny_taxi.green_taxi_2022_partitioned_clustered
PARTITION BY lpep_pickup_date
CLUSTER BY PUlocationID
AS
SELECT * FROM ny_taxi.green_taxi_2022_external;
```


## Question 5:

Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

**Answer**

`12.82 MB for non-partitioned table and 1.12 MB for the partitioned table`

```SQL
-- materialized table
SELECT DISTINCT PULocationID
FROM ny_taxi.green_taxi_2022_non_partitioned
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30';

-- partitioned & clustered table
SELECT DISTINCT PULocationID
FROM ny_taxi.green_taxi_2022_partitioned_clustered
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30';
```

## Question 6:

Where is the data stored in the External Table you created?

**Answer**

`GCP Bucket`

## Question 7:

It is best practice in Big Query to always cluster your data:

**Answer**

`False`

> It is best to create clusters based on the attributes frequently used to query the data.