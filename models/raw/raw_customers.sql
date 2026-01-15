/*-- testing the test_customers file
select 
    --customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    'test_customers' as batch_id,
    --'day1' as batch_id,
    --'2025-01-01'::timestamp as ingestion_ts
    timestamp::timestamp as ingestion_ts
from {{ ref('test_customers') }}*/

/*union all

-- testing the test_customers2 file
select 
    --customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(null as varchar) as phone_number,
    source_system,
    'test_customers2' as batch_id,
    --'day1' as batch_id,
    --'2025-01-01'::timestamp as ingestion_ts
    timestamp::timestamp as ingestion_ts
from {{ ref('test_customers2') }}*/

-- Running the test file with 9k rows
select 
    customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    'customers_seed_new' as batch_id,
    --'day1' as batch_id,
    --'2025-01-01'::timestamp as ingestion_ts
    timestamp::timestamp as ingestion_ts
from {{ ref('customers_seed_new') }}

/*select 
    customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(null as varchar) as phone_number,
    source_system,
    'day1' as batch_id,
    '2025-01-01'::timestamp as ingestion_ts
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
    '2025-01-02'::timestamp as ingestion_ts
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
    '2025-01-03'::timestamp as ingestion_ts
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
    '2025-01-04'::timestamp as ingestion_ts
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
    '2025-01-05'::timestamp as ingestion_ts
from {{ ref('customers_day5') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    'day6' as batch_id,
    '2025-01-06'::timestamp as ingestion_ts
from {{ ref('customers_day6') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    'day7' as batch_id,
    '2025-01-07'::timestamp as ingestion_ts
from {{ ref('customers_day7') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    'day8' as batch_id,
    '2025-01-08'::timestamp as ingestion_ts
from {{ ref('customers_day8') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    'day9' as batch_id,
    '2025-01-09'::timestamp as ingestion_ts
from {{ ref('customers_day9') }}

union all

select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    'day10' as batch_id,
    '2025-01-10'::timestamp as ingestion_ts
from {{ ref('customers_day10') }}*/