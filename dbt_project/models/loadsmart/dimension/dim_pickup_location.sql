{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['pickup_city','pickup_state'],
        schema='loadsmart_dim',
        on_schema_change='ignore'
    )
}}

with unknown_record as (
    select
        -1::bigint        as pickup_id,
        'Unknown'::text   as pickup_city,
        'Unknown'::text   as pickup_state,
        current_timestamp as line_created_at,
        current_timestamp as line_updated_at
),

distinct_values as (
    select distinct
        cast(pickup_city  as text) as pickup_city,
        cast(pickup_state as text) as pickup_state
    from {{ source('loadsmart', '2025_data_challenge_ae') }}
    where lane is not null
),

ordered_values as (
    select
        row_number() over (order by pickup_city, pickup_state)::bigint as pickup_id,
        pickup_city,
        pickup_state,
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
where (pickup_city, pickup_state) not in (
    select pickup_city, pickup_state from {{ this }}
)
{% endif %}
