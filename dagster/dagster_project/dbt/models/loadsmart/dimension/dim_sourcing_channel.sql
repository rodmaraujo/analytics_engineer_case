{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['sourcing_channel'],
        schema='loadsmart_dim',
        on_schema_change='ignore'
    )
}}

with unknown_record as (
    select
        -1::bigint        as sourcing_channel_id,
        'Unknown'::text   as sourcing_channel,
        current_timestamp as line_created_at,
        current_timestamp as line_updated_at
),

distinct_values as (
    select distinct
        cast(sourcing_channel as text) as sourcing_channel
    from {{ source('loadsmart', '2025_data_challenge_ae') }}
    where sourcing_channel is not null
),

ordered_values as (
    select
        row_number() over (order by sourcing_channel)::bigint as sourcing_channel_id,
        sourcing_channel,
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
where sourcing_channel not in (
    select sourcing_channel
    from {{ this }}
)
{% endif %}
