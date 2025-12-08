{{ 
    config(
        materialized='incremental',
        incremental_strategy='append',
        schema='loadsmart_fact',
        unique_key='loadsmart_id',
        on_schema_change='ignore'
    )
}}

{% if is_incremental() %}
    {% set last_load_ts = " (select coalesce(max(line_created_at), '1900-01-01'::timestamp) from " ~ this ~ ") " %}
{% else %}
    {% set last_load_ts = "'1900-01-01'::timestamp" %}
{% endif %}

with dedup as (

    select
        raw.*,
        row_number() over (
            partition by raw.loadsmart_id
            order by raw.delivery_date::timestamp desc
        ) as rn
    from {{ source('loadsmart', '2025_data_challenge_ae') }} raw
    where raw.ingestion_date > {{ last_load_ts }}

),

enriched as (

    select
        d.loadsmart_id,

        coalesce(pl.pickup_id, -1) as pickup_id,
        coalesce(dl.delivery_id, -1) as delivery_id,

        coalesce(sh.shipper_id, -1) as shipper_id,
        coalesce(car.carrier_id, -1) as carrier_id,
        coalesce(sc.sourcing_channel_id, -1) as sourcing_channel_id,

        d.vip_carrier,

        coalesce(d.quote_date::timestamp,               '1900-01-01') as quote_date,
        coalesce(d.book_date::timestamp,                '1900-01-01') as book_date,
        coalesce(d.source_date::timestamp,              '1900-01-01') as source_date,
        coalesce(d.pickup_date::timestamp,              '1900-01-01') as pickup_date,
        coalesce(d.delivery_date::timestamp,            '1900-01-01') as delivery_date,
        coalesce(d.pickup_appointment_time::timestamp,  '1900-01-01') as pickup_appointment_time,
        coalesce(d.delivery_appointment_time::timestamp,'1900-01-01') as delivery_appointment_time,

        d.book_price,
        d.source_price,
        d.pnl,
        d.mileage,
        d.equipment_type,
        coalesce(d.carrier_rating, 0) as carrier_rating,
        d.carrier_dropped_us_count,

        lower(d.carrier_on_time_to_pickup)   = 'true' as carrier_on_time_to_pickup,
        lower(d.carrier_on_time_to_delivery) = 'true' as carrier_on_time_to_delivery,
        lower(d.carrier_on_time_overall)     = 'true' as carrier_on_time_overall,

        d.contracted_load,
        d.load_booked_autonomously,
        d.load_sourced_autonomously,
        d.load_was_cancelled,

        d.ingestion_date as line_created_at

    from dedup d

    left join {{ ref('dim_pickup_location') }} pl
        on d.pickup_city = pl.pickup_city
       and d.pickup_state = pl.pickup_state

    left join {{ ref('dim_delivery_location') }} dl
        on d.delivery_city = dl.delivery_city
       and d.delivery_state = dl.delivery_state

    left join {{ ref('dim_shipper') }} sh
        on d.shipper_name = sh.shipper_name

    left join {{ ref('dim_carrier') }} car
        on d.carrier_name = car.carrier_name

    left join {{ ref('dim_sourcing_channel') }} sc
        on d.sourcing_channel = sc.sourcing_channel
    
    where d.rn = 1
)

select *
from enriched
