select 
    customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(null as varchar) as phone_number,
    source_system,
    'day1' as batch_id,
    current_timestamp as ingestion_ts
from {{ ref('customers_day1') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(null as varchar) as phone_number,
    source_system,
    'day2' as batch_id,
    current_timestamp as ingestion_ts
from {{ ref('customers_day2') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(null as varchar) as phone_number,
    source_system,
    'day3' as batch_id,
    current_timestamp as ingestion_ts
from {{ ref('customers_day3') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    'day4' as batch_id,
    current_timestamp as ingestion_ts
from {{ ref('customers_day4') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    cast(null as varchar) as city,
    cast(null as varchar) as phone_number,
    source_system,
    'day5' as batch_id,
    current_timestamp as ingestion_ts
from {{ ref('customers_day5') }}