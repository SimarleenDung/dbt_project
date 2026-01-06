select
    email,
    max(first_name)   as first_name,
    max(middle_name)  as middle_name,
    max(last_name)    as last_name
from {{ ref('stg_uc_one_sample_data') }}
group by email
