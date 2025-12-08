{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['delivery_city','delivery_state'],
        schema='loadsmart_dim',
        on_schema_change='ignore'
    )
}}

with unknown_record as (
    select
        -1::bigint        as delivery_id,
        'Unknown'::text   as delivery_city,
        'Unknown'::text   as delivery_state,
        current_timestamp as line_created_at,
        current_timestamp as line_updated_at
),

distinct_values as (
    select distinct
        cast(delivery_city  as text) as delivery_city,
        cast(delivery_state as text) as delivery_state
    from {{ source('loadsmart', '2025_data_challenge_ae') }}
    where lane is not null
),

ordered_values as (
    select
        row_number() over (order by delivery_city, delivery_state)::bigint as delivery_id,
        delivery_city,
        delivery_state,
        current_timestamp as line_created_at,
        current_timestamp as line_updated_at
    from distinct_values
)

select *
from (

    {% if not is_incremental() %}        
        select * from unknown_record
        union all
    {% endif %}

    select *
    from ordered_values

) full_data

{% if is_incremental() %}
where (delivery_city, delivery_state) not in (
    select delivery_city, delivery_state
    from {{ this }}
)
{% endif %}
