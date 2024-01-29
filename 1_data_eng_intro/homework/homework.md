# Module 1 Homework

## Docker & SQL

In this homework we'll prepare the environment and practice with terraform and SQL


### Question 1. Knowing docker tags

Run the command to get information on Docker

`docker --help`

Now run the command to get help on the "docker build" command:

`docker build --help`

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits*

**Answer:**

`--rm`


### Question 2. Understanding docker first run

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list ).

What is version of the package wheel ?

**Answer:**

`wheel  0.42.0`

```bash
docker run -it --entrypoint=bash python:3.9
```

### Question 3. Count records

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

**Answer:**

`15612 taxi trips completed on September 18th 2019`

```SQL
SELECT
  CAST(lpep_pickup_datetime AS DATE) AS "day_pickup",
  CAST(lpep_dropoff_datetime AS DATE) AS "day_dropoff",
  COUNT(1) AS "total_trips"
FROM green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
	AND CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18'
GROUP BY 
  CAST(lpep_pickup_datetime AS DATE),
  CAST(lpep_dropoff_datetime AS DATE);
```

### Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance? (Use the pick up time for your calculations)

**Answer:**

`2019-09-26`

```SQL
SELECT
	CAST(lpep_pickup_datetime AS DATE) AS "day_pickup",
	MAX(trip_distance) AS "trip_distance"
FROM green_taxi_trips
GROUP BY 1
ORDER BY "trip_distance" DESC;
```

### Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

**Answer:**

`"Brooklyn" "Manhattan" "Queens"`

```SQL
SELECT "Borough", SUM(total_amount) AS total_amount_sum
FROM green_taxi_trips
INNER JOIN taxi_zones
  ON "PULocationID" = taxi_zones."LocationID"
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
  AND "Borough" != 'Unknown'
GROUP BY 1
HAVING SUM(total_amount) > 50000
ORDER BY total_amount_sum DESC;
```


### Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's tip , not trip

**Answer:**

`JFK Airport`

```SQL
SELECT
  pickup_zone."Zone" AS pickup_location,
  dropoff_zone."Zone" AS dropoff_location,
  MAX(tip_amount) AS largest_tip
FROM green_taxi_trips
INNER JOIN taxi_zones AS pickup_zone
  ON green_taxi_trips."PULocationID" = pickup_zone."LocationID"
INNER JOIN taxi_zones AS dropoff_zone
  ON green_taxi_trips."DOLocationID" = dropoff_zone."LocationID"
WHERE CAST(lpep_pickup_datetime AS DATE) >= '2019-09-01'
  AND CAST(lpep_pickup_datetime AS DATE) < '2019-10-01'
  AND pickup_zone."Zone" = 'Astoria'
GROUP BY 1, 2
ORDER BY largest_tip DESC
LIMIT 10;
```


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. Copy the files from the course repo [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


### Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

`terraform apply`

Paste the output of this command into the homework submission form.