select
    customer_id,
    first_name,
    middle_name,
    last_name,
    city,
    updated_at
from {{ source('staging_source', 'dbt_uc_one_customer_data') }}