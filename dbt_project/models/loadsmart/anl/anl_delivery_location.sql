{{
    config(
        materialized='table',
        schema='loadsmart_anl'
    )
}}

select 
    f.delivery_id,
    dl.delivery_city,
    dl.delivery_state,
    cast(f.delivery_date as date) as delivery_date,

    sum(f.book_price)      as sum_book_price,
    sum(f.source_price)    as sum_source_price,
    sum(f.pnl)             as sum_pnl

from {{ ref('fact_loadsmart_load') }} f
left join {{ ref('dim_delivery_location') }} dl
    on f.delivery_id = dl.delivery_id

group by
    f.delivery_id,
    dl.delivery_city,
    dl.delivery_state,
    cast(f.delivery_date as date)
