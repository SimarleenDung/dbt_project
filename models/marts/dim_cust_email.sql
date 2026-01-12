select
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    valid_from as start_ts,
    valid_to   as end_ts
from {{ ref('int_cust_email_scd2') }}
where is_current = 'Y'
