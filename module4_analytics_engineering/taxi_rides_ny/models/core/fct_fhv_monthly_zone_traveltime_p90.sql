{{
    config(
        materialized='table'
    )
}}


with table_duration as (
    select *,
        cast(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as numeric) as trip_duration
    from {{ ref('dim_fhv_trips') }}
), 
table_duration_p as (
    select *,
        percentile_cont(trip_duration, 0.9) over (partition by pickup_year, pickup_month, pickup_zone, dropoff_zone) as p90
    from table_duration
)

select pickup_year, 
    pickup_month, 
    pickup_zone, 
    dropoff_zone,
    max(p90) as p90
from table_duration_p
where pickup_year = 2019
    and pickup_month = 11
    and pickup_zone in ("Newark Airport", "SoHo", "Yorkville East")
group by pickup_year, pickup_month, pickup_zone, dropoff_zone
order by pickup_zone, p90