-- int_customer_resolved.sql
select
    email,

    max_by(first_name, ingestion_ts) as first_name,
    max_by(last_name, ingestion_ts)  as last_name,
    max_by(city, ingestion_ts)       as city,
    max_by(phone_number, ingestion_ts) as phone_number,

    max(ingestion_ts) as resolved_ts

from {{ ref('stg_customers') }}
group by email
