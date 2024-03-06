{{ config(materialized='view') }}

with fhv_tripdata as (
    select *,
    from {{ source("staging", "fhv_tripdata") }}
    where dispatching_base_num is not null
    AND EXTRACT(YEAR FROM pickup_datetime) = 2019
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(["dispatching_base_num", "pickup_datetime"]) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- payment info
    Affiliated_base_number as affiliated_base_number

from fhv_tripdata
-- where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var("is_test_run", default=true) %}

    limit 100

{% endif %}