{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['shipper_name'],
        schema='loadsmart_dim',
        on_schema_change='ignore'
    )
}}

with unknown_record as (
    select
        -1::bigint        as shipper_id,
        'Unknown'::text   as shipper_name,
        current_timestamp as line_created_at,
        current_timestamp as line_updated_at
),

distinct_values as (
    select distinct
        cast(shipper_name as text) as shipper_name
    from {{ source('loadsmart', '2025_data_challenge_ae') }}
    where shipper_name is not null
),

ordered_values as (
    select
        row_number() over (order by shipper_name)::bigint as shipper_id,
        shipper_name,
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

) final_data

{% if is_incremental() %}
where shipper_name not in (
    select shipper_name
    from {{ this }}
)
{% endif %}
