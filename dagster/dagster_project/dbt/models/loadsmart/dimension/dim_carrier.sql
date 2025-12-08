{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['carrier_name','carrier_segment'],
        schema='loadsmart_dim',
        on_schema_change='ignore'
    )
}}

with unknown_record as (
    select
        -1::bigint        as carrier_id,
        'Unknown'::text   as carrier_name,
        'Unknown'::text   as carrier_segment,
        current_timestamp as line_created_at,
        current_timestamp as line_updated_at
),

distinct_values as (
    select distinct
        cast(carrier_name as text)      as carrier_name,
        'transportation'::text          as carrier_segment
    from {{ source('loadsmart', '2025_data_challenge_ae') }}
    where carrier_name is not null
),

ordered_values as (
    select
        row_number() over (
            order by carrier_name, carrier_segment
        )::bigint                       as carrier_id,
        carrier_name,
        carrier_segment,
        current_timestamp                as line_created_at,
        current_timestamp                as line_updated_at
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
where (carrier_name, carrier_segment) not in (
    select carrier_name, carrier_segment
    from {{ this }}
)
{% endif %}
