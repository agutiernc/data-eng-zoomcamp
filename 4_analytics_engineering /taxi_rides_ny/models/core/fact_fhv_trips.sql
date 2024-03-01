{{ config(materialized='table') }}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

SELECT
    fhv_tripdata.tripid,
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.pickup_locationid,
    fhv_tripdata.dropoff_locationid,
    fhv_tripdata.affiliated_base_number,
    fhv_tripdata.sr_flag,

    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone as dropoff_zone
FROM fhv_tripdata
inner join dim_zones AS pickup_zone 
ON fhv_tripdata.pickup_locationid = pickup_zone.locationid 
inner join dim_zones AS dropoff_zone
ON fhv_tripdata.dropoff_locationid = dropoff_zone.locationid