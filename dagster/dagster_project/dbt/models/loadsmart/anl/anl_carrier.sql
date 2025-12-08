{{
    config(
        materialized='table',
        schema='loadsmart_anl'
    )
}}

select 
    f.carrier_id,
    dc.carrier_name,
    cast(f.delivery_date as date) as delivery_date,

    avg(f.carrier_rating)              as avg_carrier_rating,
    sum(f.book_price)                  as sum_book_price,
    sum(f.source_price)                as sum_source_price,
    sum(f.pnl)                         as sum_pnl,
    sum(f.carrier_dropped_us_count)    as sum_carrier_dropped_us_count

from {{ ref('fact_loadsmart_load') }} f
left join {{ ref('dim_carrier') }} dc
    on f.carrier_id = dc.carrier_id


where dc.carrier_name != 'Unknown'

group by
    f.carrier_id,
    dc.carrier_name,
    cast(f.delivery_date as date)
