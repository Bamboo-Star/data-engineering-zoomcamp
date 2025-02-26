{{
    config(
        materialized='table'
    )
}}


select 
    fhv.*,
    EXTRACT(YEAR FROM fhv.pickup_datetime) as pickup_year, 
    EXTRACT(MONTH FROM fhv.pickup_datetime) as pickup_month, 
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
from {{ ref('stg_fhv_tripdata') }} fhv
left join {{ ref('dim_zones') }} pickup_zone
    on fhv.pickup_locationid = pickup_zone.locationid
left join {{ ref('dim_zones') }} dropoff_zone
    on fhv.dropoff_locationid = dropoff_zone.locationid