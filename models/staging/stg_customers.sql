-- stg_customers.sql
select
    --customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(phone_number as varchar) as phone_number,
    source_system,
    batch_id,
    ingestion_ts

from {{ ref('raw_customers') }}
