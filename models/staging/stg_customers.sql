-- stg_customers.sql
select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    -- schema-safe column: will be NULL if missing in raw data
    -- cast(null as varchar) as phone_number,

    source_system,
    ingestion_ts

from {{ ref('raw_customers') }}
