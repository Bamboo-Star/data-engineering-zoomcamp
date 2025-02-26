{{
    config(
        materialized='table'
    )
}}

with yoy as (
    select service_type, pickup_year, pickup_quarter, pickup_yrqtr, sum(total_amount) as sum_amount 
    from {{ ref('fact_trips') }}
    group by service_type, pickup_year, pickup_quarter, pickup_yrqtr
)


select y1.service_type, y1.pickup_yrqtr, (y1.sum_amount/y2.sum_amount-1) as sum_amount_yoy 
from yoy y1
join yoy y2
    on y1.service_type = y2.service_type
    and y1.pickup_year-1 = y2.pickup_year
    and y1.pickup_quarter = y2.pickup_quarter
where y1.pickup_year = 2020
order by service_type, sum_amount_yoy