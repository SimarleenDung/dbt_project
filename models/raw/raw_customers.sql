select 
    customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(null as varchar) as phone_number,
    source_system,
    cast(ingestion_ts as timestamp) as ingestion_ts
from {{ ref('customers_scd_test') }}

/*select 
    customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(null as varchar) as phone_number,
    source_system,
    cast(ingestion_ts as timestamp) as ingestion_ts
from {{ ref('customers_5000') }}*/

/*select 
    customer_id,
    email,
    first_name,
    last_name,
    city,
    cast(null as varchar) as phone_number,
    source_system,
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
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
    cast(ingestion_ts as timestamp) as ingestion_ts
from {{ ref('customers_day10') }}*/