{{ config(
    materialized = 'incremental',
    unique_key = 'email',
    on_schema_change = 'sync_all_columns'
) }}

with ranked as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        batch_id,
        ingestion_ts,
        row_number() over (
            partition by email
            order by ingestion_ts desc, customer_id desc
        ) as rn
    from {{ ref('stg_customers') }}

)

select
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    batch_id,
    ingestion_ts as resolved_ts
from ranked
where rn = 1
