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
        ingestion_ts,
        row_number() over (
            partition by email
            order by ingestion_ts desc
        ) as rn
    from {{ ref('stg_customers') }}

    {% if is_incremental() %}
      where ingestion_ts > (
        select coalesce(max(resolved_ts), '1900-01-01')
        from {{ this }}
      )
    {% endif %}

)

select
    email,
    first_name,
    last_name,
    city,
    phone_number,
    ingestion_ts as resolved_ts
from ranked
where rn = 1
