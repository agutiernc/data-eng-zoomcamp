## Week 5 Homework 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHV 2019-10 data found here. [FHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)

### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

**Answer**

`'3.3.2'`

### Question 2: 

**FHV October 2019**

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

**Answer**

`6MB`

```python
df.repartition(6).write.parquet('data/', mode='overwrite')
```

### Question 3: 

**Count records** 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

> [!IMPORTANT]
> Be aware of columns order when defining schema

**Answer**

`62,610`

```python
spark.sql("""
SELECT
    DATE(pickup_datetime) as pickup_date,
    count(*) AS total_trips
FROM fhv
WHERE
    DATE(pickup_datetime) = '2019-10-15'
GROUP BY 1
""").show()
```

### Question 4: 

**Longest trip for each day** 

What is the length of the longest trip in the dataset in hours?

**Answer**

`631,152.50 Hours`

```python
spark.sql("""
SELECT
     MAX(TIMESTAMPDIFF(HOUR, pickup_datetime, dropOff_datetime)) AS longest_trip_hours
FROM fhv
""").show()
```

### Question 5: 

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?

**Answer**

`4040`


### Question 6: 

**Least frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

**Answer**

`Jamaica Bay`

```python
df_zones = spark.read.parquet('zones/')

df_join = df.join(df_zones, df.PUlocationID == df_zones.LocationID)

df_least_freq_zone = df_join \
    .groupBy("Zone") \
    .count() \
    .orderBy("count") \
    .limit(10) \
    .show()
```
