{{
    config(
        materialized='table',
        schema='loadsmart_reports'
    )
}}

with raw as (
    select *,
           cast(delivery_date as timestamp) as delivery_ts
    from {{ source('loadsmart', '2025_data_challenge_ae') }}
),

last_month_count as (
    select count(*) as cnt
    from raw
    where delivery_ts >= current_date - interval '1 month'
),

last_month_min_date as (
    select min(delivery_ts) as min_dt
    from raw
    where delivery_ts >= current_date - interval '1 month'
),

fallback_date as (
    select max(delivery_ts) - interval '3 month' as fb_dt
    from raw
),

cutoff as (
    select
        case 
            when (select cnt from last_month_count) > 0
            then (select min_dt from last_month_min_date)
            else (select fb_dt from fallback_date)
        end as cutoff_date
)

select 
    r.loadsmart_id,
    r.shipper_name,
    r.delivery_ts as delivery_date,
    r.pickup_city,
    r.pickup_state,
    r.delivery_city,
    r.delivery_state,
    r.book_price,
    r.carrier_name

from raw r
cross join cutoff c
where r.delivery_ts >= c.cutoff_date
