{{
    config(
        materialized='table',
        schema='loadsmart_anl'
    )
}}

select 
    f.pickup_id,
    pl.pickup_city,
    pl.pickup_state,
    cast(f.delivery_date as date) as delivery_date,

    sum(f.book_price)      as sum_book_price,
    sum(f.source_price)    as sum_source_price,
    sum(f.pnl)             as sum_pnl

from {{ ref('fact_loadsmart_load') }} f
left join {{ ref('dim_pickup_location') }} pl
    on f.pickup_id = pl.pickup_id

group by
    f.pickup_id,
    pl.pickup_city,
    pl.pickup_state,
    cast(f.delivery_date as date)
